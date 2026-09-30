"""Equipment-scoped nightly rule reads with complete-query receipts."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
import hashlib
import json
import re
from typing import Any, Protocol
from urllib.error import HTTPError
from urllib.parse import quote, urljoin
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from ingestion.adapters.skyspark.metadata import _json_value, _reference
from ingestion.contracts.config import FeedKind
from ingestion.contracts.jobs import Job
from ingestion.contracts.resources import RulesReadPolicy
from ingestion.contracts.rules import RuleDetection
from ingestion.core.failures import NonRetryableSourceError, RawResponseTooLarge


_SAFE_PATH = re.compile(r"[A-Za-z0-9_:\-./]+")
_REF = re.compile(r"[A-Za-z0-9_:\-.]+")


class RulesSource(Protocol):
    def read(
        self, site_uri: str, equipment_ids: tuple[str, ...], day: date,
        start: datetime, end: datetime, source_timezone: str,
    ) -> dict[str, Any]: ...


@dataclass(frozen=True, slots=True)
class RulesExtraction:
    detections: tuple[RuleDetection, ...]
    raw_response: bytes
    query_id: str
    completed_ids: tuple[str, ...]


def _digest(value: dict[str, object]) -> str:
    return hashlib.sha256(json.dumps(
        value, sort_keys=True, separators=(",", ":"), allow_nan=False,
    ).encode("utf-8")).hexdigest()


def _source_day(job: Job, source_timezone: str) -> date:
    if (job.feed != FeedKind.RULES or job.window_start is None or job.window_end is None
            or not source_timezone):
        raise ValueError("rules reader requires a bounded equipment job")
    zone = ZoneInfo(source_timezone)
    day = job.window_start.astimezone(zone).date()
    start = datetime.combine(day, time.min, tzinfo=zone).astimezone(timezone.utc)
    end = datetime.combine(day + timedelta(days=1), time.min, tzinfo=zone).astimezone(timezone.utc)
    if (job.window_start.astimezone(timezone.utc), job.window_end.astimezone(timezone.utc)) != (start, end):
        raise ValueError("rules job window must match one local calendar day")
    return day


def _point_ids(value: object) -> tuple[str, ...]:
    if value is None or value == "":
        return ()
    if isinstance(value, (list, tuple)):
        refs = tuple(_reference(item) for item in value)
        if len(refs) != len(set(refs)):
            raise NonRetryableSourceError("rule points contain duplicate references")
        return refs
    if isinstance(value, str) and (_REF.fullmatch(value) or value.startswith("r:")):
        return (_reference(value),)
    # The original CSV renders Haystack lists as text. The live grid should
    # supply structured references; retain an opaque tag without inventing IDs.
    return ()


def read_rules(
    job: Job, *, site_uri: str, source: RulesSource, policy: RulesReadPolicy,
    source_timezone: str, allowed_tz_tags: tuple[str, ...],
) -> RulesExtraction:
    day = _source_day(job, source_timezone)
    if (not site_uri or len(job.scope_ids) > policy.max_ids
            or len(job.scope_ids) != len(set(job.scope_ids))
            or not allowed_tz_tags or len(allowed_tz_tags) != len(set(allowed_tz_tags))):
        raise ValueError("rules reader requires a unique bounded approved scope")
    if not job.scope_ids:
        receipt = {"schema_version": 1, "query_id": f"empty/{job.job_id}",
                   "completed_ids": [], "rows": []}
        raw = json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode()
        return RulesExtraction((), raw, receipt["query_id"], ())
    page = source.read(
        site_uri, job.scope_ids, day, job.window_start, job.window_end,
        source_timezone,
    )
    if not isinstance(page, dict) or not isinstance(page.get("meta"), dict):
        raise NonRetryableSourceError("rules source returned no coverage receipt")
    try:
        raw = json.dumps(page, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    except (TypeError, ValueError) as exc:
        raise NonRetryableSourceError("rules response is not serializable") from exc
    if len(raw) > policy.max_response_bytes:
        raise RawResponseTooLarge("rules response exceeds configured byte cap")
    meta = page["meta"]
    rows = page.get("rows")
    cols = page.get("cols")
    requested = meta.get("requested_ids")
    completed = meta.get("completed_ids")
    query_id = meta.get("query_id")
    if (meta.get("err") is not None or meta.get("complete") is not True
            or meta.get("truncated") is not False or meta.get("project") != job.project_id
            or meta.get("site") != site_uri or meta.get("day") != day.isoformat()
            or meta.get("timezone") != source_timezone
            or meta.get("window_start") != job.window_start.isoformat()
            or meta.get("window_end") != job.window_end.isoformat()
            or not isinstance(query_id, str) or not query_id
            or requested != list(job.scope_ids) or completed != requested
            or meta.get("requested_count") != len(job.scope_ids)
            or type(meta.get("returned_rows")) is not int
            or not isinstance(rows, list) or len(rows) > policy.max_rows
            or meta["returned_rows"] != len(rows) or meta.get("total_rows") != len(rows)
            or meta.get("page_index") != 0 or meta.get("next_page") is not None
            or not isinstance(cols, list)):
        raise NonRetryableSourceError("rules query has incomplete or mismatched coverage")
    names = [column.get("name") for column in cols if isinstance(column, dict)]
    if len(names) != len(cols) or any(not isinstance(name, str) for name in names) or len(names) != len(set(names)):
        raise NonRetryableSourceError("rules columns are malformed")
    detections: list[RuleDetection] = []
    seen: set[str] = set()
    for source_row in rows:
        if not isinstance(source_row, dict) or set(source_row) - set(names):
            raise NonRetryableSourceError("rules row contains an unrequested column")
        tags = _json_value(source_row)
        equipment_id = _reference(tags.get("targetRef"))
        rule_id = _reference(tags.get("ruleRef"))
        if equipment_id not in job.scope_ids:
            raise NonRetryableSourceError("rule detection targets an unrequested equipment")
        if tags.get("fsk") not in (None, job.project_id):
            raise NonRetryableSourceError("rule detection belongs to another project")
        if tags.get("date") != day.isoformat() or tags.get("tz") not in allowed_tz_tags:
            raise NonRetryableSourceError("rule detection date or time zone differs from the query")
        if tags.get("spark") is None:
            raise NonRetryableSourceError("rule detection has no spark value")
        identity = {
            "tenant_id": job.tenant_id, "project_id": job.project_id,
            "site_ref": job.site_ref, "equipment_id": equipment_id,
            "rule_id": rule_id, "source_date": day.isoformat(),
            "source_timezone": source_timezone,
        }
        detection_key = _digest(identity)
        if detection_key in seen:
            raise NonRetryableSourceError("rule day has ambiguous duplicate detection identity")
        seen.add(detection_key)
        detections.append(RuleDetection(
            detection_key=detection_key, revision_hash=_digest(tags),
            tenant_id=job.tenant_id, project_id=job.project_id,
            site_ref=job.site_ref, equipment_id=equipment_id, rule_id=rule_id,
            source_date=day, source_tz_tag=tags["tz"],
            source_timezone=source_timezone,
            window_start=job.window_start, window_end=job.window_end,
            spark=tags["spark"], duration=tags.get("dur"),
            periods=tags.get("periods"), point_ids=_point_ids(tags.get("points")),
            points=tags.get("points"), priority=tags.get("priority"),
            severity=tags.get("severity"),
            alarm_message_text=tags.get("alarmMessageText"),
            help_message_text=tags.get("helpMessageText"), tags=tags,
        ))
    return RulesExtraction(tuple(detections), raw, query_id, job.scope_ids)


class FixtureRulesSource:
    """Local HTTP source exposing explicit equipment query coverage."""

    def __init__(self, endpoint: str, *, max_response_bytes: int, timeout_seconds: float):
        if not endpoint.endswith("/"):
            raise ValueError("fixture API root must end in /")
        self._endpoint = endpoint
        self._maximum = max_response_bytes
        self._timeout = timeout_seconds

    def read(
        self, site_uri: str, equipment_ids: tuple[str, ...], day: date,
        start: datetime, end: datetime, source_timezone: str,
    ) -> dict[str, Any]:
        if not _SAFE_PATH.fullmatch(site_uri) or ".." in site_uri:
            raise ValueError("invalid fixture site URI")
        payload = json.dumps({
            "equipment_ids": list(equipment_ids), "day": day.isoformat(),
            "window_start": start.isoformat(), "window_end": end.isoformat(),
            "timezone": source_timezone,
        }, separators=(",", ":")).encode()
        request = Request(
            urljoin(self._endpoint, f"{quote(site_uri)}/fixtures/rules"),
            data=payload, headers={"Content-Type": "application/json"},
        )
        try:
            with urlopen(request, timeout=self._timeout) as response:
                raw = response.read(self._maximum + 1)
        except HTTPError as exc:
            if exc.code == 413:
                raise RawResponseTooLarge("rules fixture response exceeds source cap") from exc
            raise
        if len(raw) > self._maximum:
            raise RawResponseTooLarge("rules fixture response exceeds byte cap")
        result = json.loads(raw)
        if not isinstance(result, dict):
            raise NonRetryableSourceError("fixture returned no rules grid")
        return result


class PhableRulesSource:
    """Observed Haystack grids remain uncertifiable without coverage receipts."""

    def __init__(self, client: Any):
        self._client = client

    def read(
        self, site_uri: str, equipment_ids: tuple[str, ...], day: date,
        start: datetime, end: datetime, source_timezone: str,
    ) -> dict[str, Any]:
        if not _REF.fullmatch(site_uri) or any(not _REF.fullmatch(value) for value in equipment_ids):
            raise ValueError("invalid Haystack rules query scope")
        from ingestion.source_probe import PhableProbeClient

        selected = " or ".join(f"id==@{identifier}" for identifier in equipment_ids)
        expression = (
            f"readAll(equip and siteRef==@{site_uri} and ({selected}))"
            f".ruleSparks({day.isoformat()})"
        )
        grid = PhableProbeClient(self._client).eval(expression)
        return {
            "meta": _json_value(grid.meta),
            "cols": [{"name": column.name, "meta": _json_value(column.meta)} for column in grid.cols],
            "rows": _json_value(grid.rows),
        }
