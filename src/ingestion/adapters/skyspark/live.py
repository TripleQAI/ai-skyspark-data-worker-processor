"""Live SkySpark sources for the reviewed metadata, history, and rules readers.

Each source returns the page shape its ``read_*`` validator already enforces.
SkySpark does not return a completeness receipt, so these sources build one
from the response itself only when ``SourceClientPolicy.receipt_mode`` is
``observed``; the receipt is labelled ``receipt_kind: observed`` in raw
evidence. In ``provider`` mode they refuse to run, keeping production closed.

When the binding carries a ``source_replica`` shape, each site re-reads the
real source site and presents the configured virtual topology (see
``replica.py``). Replica requests are deduplicated to real IDs before any
source call, so a job never asks SkySpark for more IDs than it covers.
"""

from __future__ import annotations

import json
import math
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal
import re
from typing import Any, Protocol
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

from ingestion.adapters.skyspark.metadata import _historized
from ingestion.adapters.skyspark.replica import (
    ReplicaShape, expand_equipment, expand_points, real_ids,
)
from ingestion.contracts.jobs import Job
from ingestion.contracts.resources import SourceClientPolicy
from ingestion.core.failures import NonRetryableSourceError


_REF = re.compile(r"[A-Za-z0-9_:\-.]+")


class LiveClient(Protocol):
    def read_all(self, filter_expr: str) -> Any: ...
    def eval(self, expression: str) -> Any: ...
    def his_read(self, ids: tuple[str, ...], start: datetime, end: datetime) -> Any: ...


class PhableLiveClient:
    """Thin read-only wrapper; only read, eval, and hisRead are exposed."""

    def __init__(self, client: Any):
        self._client = client

    def read_all(self, filter_expr: str) -> Any:
        return self._client.read_all(filter_expr)

    def eval(self, expression: str) -> Any:
        from phable import Grid, GridCol

        return self._client.call("eval", Grid(
            meta={"ver": "3.0"}, cols=[GridCol(name="expr")], rows=[{"expr": expression}],
        ))

    def his_read(self, ids: tuple[str, ...], start: datetime, end: datetime) -> Any:
        from phable import DateTimeRange, Ref

        utc = ZoneInfo("UTC")
        return self._client.his_read_by_ids(
            [Ref(identifier) for identifier in ids],
            DateTimeRange(start.astimezone(utc), end.astimezone(utc)),
        )


def credentials_from_env(environ: Mapping[str, str], policy: SourceClientPolicy) -> tuple[str, str]:
    """Read a JSON ``{"username","password"}`` secret injected by ECS."""
    raw = environ.get(policy.credentials_env)
    if not raw:
        raise ValueError(f"source credential variable {policy.credentials_env} is not set")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("source credential secret is not JSON") from None
    username = payload.get("username") if isinstance(payload, dict) else None
    password = payload.get("password") if isinstance(payload, dict) else None
    if not isinstance(username, str) or not username or not isinstance(password, str) or not password:
        raise ValueError("source credential secret needs nonempty username and password")
    return username, password


def project_api_url(endpoint: str) -> str:
    """The binding endpoint is the project's Haystack API root."""
    parsed = urlsplit(endpoint)
    if (parsed.scheme not in {"http", "https"} or not parsed.hostname
            or parsed.username or parsed.password or parsed.query or parsed.fragment
            or parsed.path.rstrip("/") in ("", "/api")):
        raise ValueError("source endpoint must be a credential-free project API URL")
    return endpoint.rstrip("/")


@contextmanager
def open_live_client(endpoint: str, username: str, password: str) -> Iterator[PhableLiveClient]:
    try:
        from phable import open_haystack_client
    except ImportError as exc:
        raise RuntimeError('install the source dependency with pip install ".[source]"') from exc
    with open_haystack_client(project_api_url(endpoint), username, password) as client:
        yield PhableLiveClient(client)


def require_observed(policy: SourceClientPolicy | None) -> SourceClientPolicy:
    if policy is None:
        raise ValueError("resources.source_client is required for live readers")
    if policy.receipt_mode != "observed":
        # SkySpark returns no provider receipt; fail closed rather than guess.
        raise NonRetryableSourceError("live SkySpark has no provider completeness receipt")
    return policy


def haystack_json(value: Any) -> Any:
    """Encode phable kinds as Haystack-JSON-like values; never drop a value."""
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else {"_kind": "number", "val": str(value)}
    if isinstance(value, Decimal):
        return _number(value, None)
    if isinstance(value, Mapping):
        return {str(key): haystack_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [haystack_json(item) for item in value]
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    kind = type(value).__name__
    if kind == "Marker":
        return {"_kind": "marker"}
    if kind == "NA":
        return {"_kind": "na"}
    if kind == "Remove":
        return {"_kind": "remove"}
    if kind == "Number":
        return _number(value.val, value.unit)
    if kind == "Ref":
        encoded = {"_kind": "ref", "val": value.val}
        if getattr(value, "dis", None):
            encoded["dis"] = value.dis
        return encoded
    if kind == "Coord":
        return {"_kind": "coord", "lat": haystack_json(value.lat), "lng": haystack_json(value.lng)}
    if kind == "XStr":
        return {"_kind": "xstr", "type": value.type, "val": value.val}
    if kind in ("Uri", "Symbol"):
        return {"_kind": kind.casefold(), "val": value.val}
    if kind == "Grid":
        return grid_json(value)
    return {"_kind": kind.casefold(), "val": str(value)}


def _number(val: Any, unit: str | None) -> dict[str, Any]:
    number = float(val)
    encoded: dict[str, Any] = {"_kind": "number", "val": number if math.isfinite(number) else str(val)}
    if unit:
        encoded["unit"] = unit
    return encoded


def grid_json(grid: Any) -> dict[str, Any]:
    return {
        "meta": haystack_json(dict(grid.meta)),
        "cols": [{"name": column.name, "meta": haystack_json(dict(column.meta or {}))}
                 for column in grid.cols],
        "rows": [haystack_json(dict(row)) for row in grid.rows],
    }


def _checked(grid: Any, policy: SourceClientPolicy) -> dict[str, Any]:
    encoded = grid_json(grid)
    meta = encoded["meta"]
    if "err" in meta:
        raise NonRetryableSourceError("source returned a Haystack error grid")
    if any(marker in str(key).casefold() for key in meta for marker in policy.truncation_meta_markers):
        raise NonRetryableSourceError("source grid reports truncation or a limit")
    return encoded


def _ref_value(value: Any) -> str | None:
    if isinstance(value, Mapping):
        value = value.get("val")
    return value if isinstance(value, str) and _REF.fullmatch(value) else None


def _ref_list(ids: tuple[str, ...]) -> str:
    if any(not _REF.fullmatch(identifier) for identifier in ids):
        raise ValueError("source IDs must be plain Haystack Refs")
    return " or ".join(f"id==@{identifier}" for identifier in ids)


def _sorted_by_id(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(rows, key=lambda row: _ref_value(row.get("id")) or "")


def _source_meta(grid: dict[str, Any], **extra: Any) -> dict[str, Any]:
    return {"receipt_kind": "observed", "source_grid_meta": grid["meta"], **extra}


@dataclass(slots=True)
class LiveMetadataSource:
    """One site snapshot per job; the run ID is the shared snapshot token."""

    client: LiveClient
    job: Job
    policy: SourceClientPolicy
    replica: ReplicaShape | None = None
    _equipment: dict[str, Any] | None = None

    def _read(self, filter_tag: str, site_uri: str) -> dict[str, Any]:
        if not _REF.fullmatch(site_uri):
            raise ValueError("site URI must be the source siteRef")
        return _checked(self.client.read_all(f"({filter_tag}) and siteRef==@{site_uri}"), self.policy)

    def read(self, kind: str, site_uri: str, page_token: str | None) -> dict[str, Any]:
        if page_token is not None or kind not in {"equipment", "points"}:
            raise ValueError("live metadata is one page per kind")
        if self._equipment is None:
            self._equipment = self._read(self.policy.equipment_filter, site_uri)
        grid = self._equipment if kind == "equipment" else self._read(self.policy.point_filter, site_uri)
        rows = _sorted_by_id(grid["rows"])
        extra: dict[str, Any] = {"source_rows": len(rows)}
        if self.replica is not None:
            equipment = _sorted_by_id(self._equipment["rows"])
            if kind == "equipment":
                rows = expand_equipment(self.replica, self.job.site_ref, equipment)
            else:
                historized = [row for row in rows if _historized(row.get("his"))]
                rows = expand_points(self.replica, self.job.site_ref, equipment, historized)
                extra["source_historized_rows"] = len(historized)
            extra["replica"] = {
                "equipment_per_site": self.replica.equipment_per_site,
                "points_per_equipment": self.replica.points_per_equipment,
            }
        return {
            "meta": {
                **_source_meta(grid, **extra),
                "snapshot": self.job.run_id, "project": self.job.project_id,
                "site": site_uri, "page_index": 0, "next_page": None, "complete": True,
                "returned_rows": len(rows), "total_rows": len(rows),
            },
            "rows": rows,
        }


@dataclass(frozen=True, slots=True)
class LiveHistorySource:
    """hisRead for a job's IDs; drops samples outside the half-open window."""

    client: LiveClient
    job: Job
    policy: SourceClientPolicy
    replica: ReplicaShape | None = None

    def read(self, site_uri: str, point_ids: tuple[str, ...], start: datetime, end: datetime) -> dict[str, Any]:
        mapping = (real_ids(self.job.site_ref, point_ids, points=True) if self.replica
                   else {identifier: (identifier,) for identifier in point_ids})
        real = tuple(mapping)
        _ref_list(real)
        grid = _checked(self.client.his_read(real, start, end), self.policy)
        by_real: dict[str, str] = {}
        for column in grid["cols"]:
            if column["name"] == "ts":
                continue
            identifier = _ref_value(column.get("meta", {}).get("id")) or column["name"]
            if identifier in by_real:
                raise NonRetryableSourceError("history grid repeats a point column")
            by_real[identifier] = column["name"]
        if set(by_real) != set(real):
            raise NonRetryableSourceError("history grid does not answer every requested point")
        # One output column per requested ID; replicas copy their real column.
        virtual_columns: list[tuple[str, str, str]] = []
        for source, virtuals in mapping.items():
            for virtual in virtuals:
                virtual_columns.append((f"v{len(virtual_columns)}", virtual, by_real[source]))
        rows: list[dict[str, Any]] = []
        dropped = 0
        start_utc, end_utc = start.astimezone(timezone.utc), end.astimezone(timezone.utc)
        for source_row in grid["rows"]:
            stamp = source_row.get("ts")
            if not isinstance(stamp, str):
                raise NonRetryableSourceError("history row has no timestamp")
            instant = datetime.fromisoformat(stamp).astimezone(timezone.utc)
            if not start_utc <= instant < end_utc:
                # hisRead ranges are end-inclusive; the next window owns this sample.
                dropped += 1
                continue
            row: dict[str, Any] = {"ts": stamp}
            for name, _, source_name in virtual_columns:
                value = _history_value(source_row.get(source_name))
                if value is not None:
                    row[name] = value
            rows.append(row)
        return {
            "meta": {
                **_source_meta(grid, source_ids=len(real), boundary_rows_dropped=dropped),
                "query_id": f"observed/history/{self.job.job_id}",
                "project": self.job.project_id, "site": site_uri,
                "window_start": start.isoformat(), "window_end": end.isoformat(),
                "requested_ids": list(point_ids), "completed_ids": list(point_ids),
                "complete": True, "truncated": False,
                "returned_rows": len(rows), "total_rows": len(rows),
            },
            "cols": [{"name": "ts"}] + [{"name": name, "meta": {"id": virtual}}
                                        for name, virtual, _ in virtual_columns],
            "rows": rows,
        }


def _history_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, str)):
        return value
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, Mapping):
        kind = value.get("_kind")
        if kind == "na":
            return {"_kind": "na"}
        if kind == "number":
            number = value["val"]
            # Non-finite Haystack numbers keep their text form rather than vanish.
            return number if isinstance(number, (int, float)) else str(number)
        if "val" in value:
            return str(value["val"])
    return json.dumps(value, sort_keys=True)


@dataclass(frozen=True, slots=True)
class LiveRulesSource:
    """ruleSparks for one source-local day over a verified equipment scope."""

    client: LiveClient
    job: Job
    policy: SourceClientPolicy
    replica: ReplicaShape | None = None

    def read(
        self, site_uri: str, equipment_ids: tuple[str, ...], day: date,
        start: datetime, end: datetime, source_timezone: str,
    ) -> dict[str, Any]:
        if not _REF.fullmatch(site_uri):
            raise ValueError("site URI must be the source siteRef")
        mapping = (real_ids(self.job.site_ref, equipment_ids, points=False) if self.replica
                   else {identifier: (identifier,) for identifier in equipment_ids})
        real = tuple(mapping)
        scope = f"({self.policy.equipment_filter}) and siteRef==@{site_uri} and ({_ref_list(real)})"
        if self.policy.verify_rules_equipment_scope:
            found = _checked(self.client.read_all(scope), self.policy)
            returned = {_ref_value(row.get("id")) for row in found["rows"]}
            if returned != set(real):
                raise NonRetryableSourceError("rules equipment scope differs from the request")
        grid = _checked(self.client.eval(f"readAll({scope}).ruleSparks({day.isoformat()})"), self.policy)
        names = [column["name"] for column in grid["cols"]]
        if self.replica is not None:
            names.append("replicaSourceRef")
        rows: list[dict[str, Any]] = []
        for source_row in grid["rows"]:
            target = _ref_value(source_row.get("targetRef"))
            if target not in mapping:
                raise NonRetryableSourceError("rule detection targets an unrequested equipment")
            for virtual in mapping[target]:
                row = {key: value for key, value in source_row.items() if value is not None}
                if self.replica is not None:
                    row["targetRef"] = {"_kind": "ref", "val": virtual}
                    row["replicaSourceRef"] = {"_kind": "ref", "val": target}
                rows.append(row)
        return {
            "meta": {
                **_source_meta(grid, source_ids=len(real),
                               equipment_scope_verified=self.policy.verify_rules_equipment_scope),
                "query_id": f"observed/rules/{self.job.job_id}",
                "project": self.job.project_id, "site": site_uri,
                "day": day.isoformat(), "timezone": source_timezone,
                "window_start": start.isoformat(), "window_end": end.isoformat(),
                "requested_ids": list(equipment_ids), "completed_ids": list(equipment_ids),
                "requested_count": len(equipment_ids),
                "complete": True, "truncated": False, "page_index": 0, "next_page": None,
                "returned_rows": len(rows), "total_rows": len(rows),
            },
            "cols": [{"name": name} for name in names],
            "rows": rows,
        }
