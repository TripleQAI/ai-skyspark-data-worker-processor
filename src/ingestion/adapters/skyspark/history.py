"""Bounded five-minute history reads with provider-supplied coverage receipts."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import json
import re
from typing import Any, Protocol
from urllib.parse import quote, urljoin
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from ingestion.contracts.config import FeedKind
from ingestion.contracts.history import HistoryObservation
from ingestion.contracts.jobs import Job
from ingestion.contracts.resources import HistoryReadPolicy
from ingestion.core.failures import NonRetryableSourceError, RawResponseTooLarge


_SITE_PATH = re.compile(r"[A-Za-z0-9_:\-./]+")


class HistorySource(Protocol):
    def read(self, site_uri: str, point_ids: tuple[str, ...], start: datetime, end: datetime) -> dict[str, Any]: ...


@dataclass(frozen=True, slots=True)
class HistoryExtraction:
    observations: tuple[HistoryObservation, ...]
    raw_response: bytes
    query_id: str
    completed_ids: tuple[str, ...]


def _instant(value: object) -> datetime:
    if not isinstance(value, str):
        raise NonRetryableSourceError("history timestamp is missing")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise NonRetryableSourceError("history timestamp is malformed") from exc
    if parsed.tzinfo is None:
        raise NonRetryableSourceError("history timestamp has no time zone")
    return parsed


def _typed(point_id: str, timestamp: datetime, source_timestamp: str, value: object) -> HistoryObservation:
    fields: dict[str, object] = {}
    status: str | None = None
    if isinstance(value, dict) and set(value) == {"value", "status"}:
        status = value["status"]
        if not isinstance(status, str) or not status:
            raise NonRetryableSourceError("history value status is malformed")
        value = value["value"]
    if isinstance(value, bool):
        fields["val_bool"] = value
    elif isinstance(value, (int, float, Decimal)) and not isinstance(value, bool):
        try:
            fields["val_num"] = Decimal(str(value))
        except InvalidOperation as exc:
            raise NonRetryableSourceError("history number is invalid") from exc
    elif isinstance(value, str):
        fields["val_str"] = value
    elif isinstance(value, dict) and value == {"_kind": "na"}:
        fields["val_na"] = True
    else:
        raise NonRetryableSourceError("unsupported or untyped history value")
    try:
        return HistoryObservation(
            point_id=point_id, observed_at=timestamp,
            source_timestamp=source_timestamp, source_timezone=str(timestamp.tzinfo),
            source_status=status, **fields,
        )
    except ValueError as exc:
        raise NonRetryableSourceError("history value is invalid") from exc


def read_history(
    job: Job, *, site_uri: str, source: HistorySource, policy: HistoryReadPolicy,
) -> HistoryExtraction:
    if (job.feed != FeedKind.HISTORY or not site_uri or job.window_start is None
            or job.window_end is None or len(job.scope_ids) > policy.max_ids):
        raise ValueError("history reader requires a bounded approved history job")
    if len(set(job.scope_ids)) != len(job.scope_ids):
        raise ValueError("history job contains duplicate point IDs")
    if not job.scope_ids:
        # Empty eligible site still needs a source-independent zero-coverage result.
        receipt = {"schema_version": 1, "query_id": f"empty/{job.job_id}",
                   "completed_ids": [], "rows": []}
        raw = json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode()
        return HistoryExtraction((), raw, receipt["query_id"], ())
    page = source.read(site_uri, job.scope_ids, job.window_start, job.window_end)
    if not isinstance(page, dict) or not isinstance(page.get("meta"), dict):
        raise NonRetryableSourceError("history source returned no coverage receipt")
    try:
        raw = json.dumps(page, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    except (TypeError, ValueError) as exc:
        raise NonRetryableSourceError("history response is not serializable") from exc
    if len(raw) > policy.max_response_bytes:
        raise RawResponseTooLarge("history response exceeds configured byte cap")
    meta = page["meta"]
    rows = page.get("rows")
    columns = page.get("cols")
    query_id = meta.get("query_id")
    requested = meta.get("requested_ids")
    completed = meta.get("completed_ids")
    if (meta.get("err") is not None or meta.get("complete") is not True
            or meta.get("truncated") is not False or meta.get("project") != job.project_id
            or meta.get("site") != site_uri or not isinstance(query_id, str) or not query_id
            or meta.get("window_start") != job.window_start.isoformat()
            or meta.get("window_end") != job.window_end.isoformat()
            or not isinstance(requested, list) or not isinstance(completed, list)
            or requested != list(job.scope_ids) or completed != requested
            or type(meta.get("returned_rows")) is not int
            or not isinstance(rows, list) or meta["returned_rows"] != len(rows)
            or len(rows) > policy.max_rows or meta.get("total_rows") != len(rows)
            or not isinstance(columns, list)):
        raise NonRetryableSourceError("history response has incomplete or mismatched coverage")
    names: list[str] = []
    column_names: list[str] = []
    for column in columns:
        if not isinstance(column, dict) or not isinstance(column.get("name"), str):
            raise NonRetryableSourceError("history column is malformed")
        name = column["name"]
        column_names.append(name)
        if name != "ts":
            ref = column.get("meta", {}).get("id", name)
            if not isinstance(ref, str):
                raise NonRetryableSourceError("history column point ID is malformed")
            names.append(ref)
    if (column_names.count("ts") != 1 or len(column_names) != len(set(column_names))
            or len(names) != len(set(names)) or set(names) != set(job.scope_ids)):
        raise NonRetryableSourceError("history columns do not cover requested point IDs")
    mapping = {column["name"]: column.get("meta", {}).get("id", column["name"])
               for column in columns if column["name"] != "ts"}
    observations: list[HistoryObservation] = []
    seen: set[tuple[str, datetime]] = set()
    for row in rows:
        if not isinstance(row, dict) or set(row) - ({"ts"} | set(mapping)):
            raise NonRetryableSourceError("history row contains an unrequested column")
        stamp = _instant(row.get("ts"))
        if not job.window_start <= stamp < job.window_end:
            raise NonRetryableSourceError("history timestamp is outside requested window")
        for name, point_id in mapping.items():
            value = row.get(name)
            if value is None:
                continue
            key = (point_id, stamp.astimezone(timezone.utc))
            if key in seen:
                raise NonRetryableSourceError("duplicate history point/timestamp")
            seen.add(key)
            observations.append(_typed(point_id, stamp, row["ts"], value))
            if len(observations) > policy.max_observations:
                raise RawResponseTooLarge("history observation cap exceeded")
    return HistoryExtraction(tuple(observations), raw, query_id, job.scope_ids)


class FixtureHistorySource:
    """Local JSON source implementing an explicit complete-query contract."""

    def __init__(self, endpoint: str, *, max_response_bytes: int, timeout_seconds: float):
        if not endpoint.endswith("/"):
            raise ValueError("fixture API root must end in /")
        self._endpoint = endpoint
        self._maximum = max_response_bytes
        self._timeout = timeout_seconds

    def read(self, site_uri: str, point_ids: tuple[str, ...], start: datetime, end: datetime) -> dict[str, Any]:
        if not _SITE_PATH.fullmatch(site_uri) or ".." in site_uri:
            raise ValueError("invalid fixture site URI")
        body = json.dumps({"point_ids": list(point_ids), "window_start": start.isoformat(),
                           "window_end": end.isoformat()}, separators=(",", ":")).encode()
        request = Request(urljoin(self._endpoint, f"{quote(site_uri)}/fixtures/history"),
                          data=body, headers={"Content-Type": "application/json"})
        try:
            with urlopen(request, timeout=self._timeout) as response:
                payload = response.read(self._maximum + 1)
        except HTTPError as exc:
            if exc.code == 413:
                raise RawResponseTooLarge("history fixture response exceeds source cap") from exc
            raise
        if len(payload) > self._maximum:
            raise RawResponseTooLarge("history fixture response exceeds byte cap")
        result = json.loads(payload)
        if not isinstance(result, dict):
            raise NonRetryableSourceError("fixture returned no history grid")
        return result
