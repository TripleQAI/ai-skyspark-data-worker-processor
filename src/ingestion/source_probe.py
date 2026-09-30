"""Bounded, read-only SkySpark source-contract probe.

This module is deliberately separate from the ingestion worker. It never stores
source rows or marks an inventory or job certified.
"""

from __future__ import annotations

import hashlib
import json
import re
import time
from datetime import date, datetime, timezone
from typing import Any, Literal, Protocol
from urllib.parse import quote, urljoin, urlsplit
from zoneinfo import ZoneInfo

from pydantic import Field, field_validator, model_validator

from ingestion.contracts.config import SourceBinding, StrictModel


_REF = re.compile(r"[A-Za-z0-9_.:-]{1,128}\Z")


class PilotScope(StrictModel):
    schema_version: Literal[1]
    site_ref: str = Field(min_length=1)
    source_site_ref: str
    equipment_ids: tuple[str, ...] = Field(min_length=1, max_length=500)
    history_point_ids: tuple[str, ...] = Field(min_length=1, max_length=500)
    history_batch_sizes: tuple[int, ...] = Field(min_length=1, max_length=5)
    rules_batch_sizes: tuple[int, ...] = Field(min_length=1, max_length=5)
    window_start_utc: datetime
    window_end_utc: datetime
    rule_day: date
    max_response_rows: int = Field(default=50_000, ge=1, le=100_000)
    max_estimated_bytes: int = Field(default=33_554_432, ge=1024, le=67_108_864)

    @field_validator("source_site_ref")
    @classmethod
    def validate_site_id(cls, value: str) -> str:
        if not _REF.fullmatch(value.lstrip("@")):
            raise ValueError("source_site_ref must be a plain Haystack Ref")
        return value.lstrip("@")

    @field_validator("equipment_ids", "history_point_ids")
    @classmethod
    def validate_ids(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        cleaned = tuple(value.lstrip("@") for value in values)
        if len(cleaned) != len(set(cleaned)) or any(not _REF.fullmatch(value) for value in cleaned):
            raise ValueError("probe IDs must be unique plain Haystack Refs")
        return cleaned

    @model_validator(mode="after")
    def validate_probe(self) -> "PilotScope":
        start, end = self.window_start_utc, self.window_end_utc
        if (
            start.tzinfo is None or end.tzinfo is None
            or start.utcoffset().total_seconds() != 0
            or end.utcoffset().total_seconds() != 0
            or not start < end
            or (end - start).total_seconds() > 3600
        ):
            raise ValueError("probe requires an exact UTC window of at most one hour")
        for sizes, available in (
            (self.history_batch_sizes, len(self.history_point_ids)),
            (self.rules_batch_sizes, len(self.equipment_ids)),
        ):
            if tuple(sorted(set(sizes))) != sizes or sizes[0] != 1 or sizes[-1] > available:
                raise ValueError("batch sizes must be sorted, unique, start at 1, and fit approved IDs")
        if 2 + len(self.history_batch_sizes) + 2 * len(self.rules_batch_sizes) > 20:
            raise ValueError("probe exceeds the 20-call ceiling")
        return self


class ProbeClient(Protocol):
    def eval(self, expression: str) -> Any: ...
    def history(self, ids: tuple[str, ...], start: datetime, end: datetime) -> Any: ...


class PhableProbeClient:
    def __init__(self, client: Any):
        self._client = client

    def eval(self, expression: str) -> Any:
        from phable import Grid, GridCol

        request = Grid(
            meta={"ver": "3.0"},
            cols=[GridCol(name="expr")],
            rows=[{"expr": expression}],
        )
        return self._client.call("eval", request)

    def history(self, ids: tuple[str, ...], start: datetime, end: datetime) -> Any:
        from phable import DateTimeRange, Ref

        return self._client.his_read_by_ids(
            [Ref(identifier) for identifier in ids],
            DateTimeRange(start.astimezone(ZoneInfo("UTC")), end.astimezone(ZoneInfo("UTC"))),
        )


def approved_api_url(binding: SourceBinding, scope: PilotScope) -> str:
    if scope.site_ref not in binding.approved_sites:
        raise ValueError("pilot site is not in the approved binding")
    root = str(binding.endpoint)
    parsed = urlsplit(root)
    if (
        parsed.scheme not in {"http", "https"} or not parsed.hostname
        or parsed.username or parsed.password or parsed.query or parsed.fragment
        or not parsed.path.endswith("/")
    ):
        raise ValueError("pilot endpoint must be a credential-free HTTP API root ending in /")
    site_uri = binding.approved_sites[scope.site_ref]
    if not site_uri or any(not _REF.fullmatch(part) for part in site_uri.split("/")):
        raise ValueError("pilot site URI has an unsafe path segment")
    return urljoin(root, "/".join(quote(part) for part in site_uri.split("/")))


def _ref(value: object) -> str:
    return str(getattr(value, "val", value)).lstrip("@")


def _grid_parts(grid: Any) -> tuple[dict[str, Any], tuple[str, ...], list[dict[str, Any]]]:
    meta = grid.get("meta", {}) if isinstance(grid, dict) else getattr(grid, "meta", {})
    cols = grid.get("cols", []) if isinstance(grid, dict) else getattr(grid, "cols", [])
    rows = grid.get("rows", []) if isinstance(grid, dict) else getattr(grid, "rows", [])
    if not isinstance(meta, dict) or not isinstance(rows, (list, tuple)):
        raise ValueError("source returned an unsupported grid shape")
    if "err" in meta:
        raise ValueError("source returned a Haystack error grid")
    if any(not isinstance(row, dict) for row in rows):
        raise ValueError("source grid contains a non-object row")
    names = tuple(
        col.get("name") if isinstance(col, dict) else getattr(col, "name", None)
        for col in cols
    )
    if any(not isinstance(name, str) for name in names):
        raise ValueError("source grid has unnamed columns")
    return meta, names, list(rows)


def _summarize(
    grid: Any, scope: PilotScope, *, history_ids: tuple[str, ...] | None = None,
) -> tuple[dict[str, object], list[dict[str, Any]]]:
    meta, columns, rows = _grid_parts(grid)
    if len(rows) > scope.max_response_rows:
        raise ValueError("source grid exceeds pilot row cap")
    estimated_bytes = len(json.dumps(meta, default=str).encode("utf-8"))
    for row in rows:
        estimated_bytes += len(json.dumps(row, default=str).encode("utf-8"))
        if estimated_bytes > scope.max_estimated_bytes:
            raise ValueError("source grid exceeds pilot estimated-byte cap")
    summary: dict[str, object] = {
        "rows": len(rows),
        "empty": not rows,
        "columns_count": len(columns),
        "meta_keys": sorted(str(key) for key in meta),
        "estimated_decoded_bytes": estimated_bytes,
        "truncation_marker_present": any(
            "trunc" in str(key).lower() or "limit" in str(key).lower()
            for key in meta
        ),
    }
    if history_ids is None:
        summary["columns"] = sorted(set(columns))
    else:
        # Haystack history columns carry point IDs. Some servers use v0/v1
        # names and store the point Ref in column metadata instead. Resolve
        # either shape in memory, then report counts without exposing IDs.
        raw_columns = grid.get("cols", []) if isinstance(grid, dict) else getattr(grid, "cols", [])
        resolved_columns = []
        metadata_refs = 0
        for column in raw_columns:
            name = column.get("name") if isinstance(column, dict) else column.name
            if name == "ts":
                continue
            column_meta = column.get("meta") if isinstance(column, dict) else column.meta
            point_id = column_meta.get("id") if isinstance(column_meta, dict) else None
            metadata_refs += point_id is not None
            resolved_columns.append(_ref(point_id if point_id is not None else name))
        point_columns = set(resolved_columns)
        requested = set(history_ids)
        summary.update({
            "history_timestamp_rows": sum("ts" in row for row in rows),
            "history_point_columns": len(resolved_columns),
            "history_columns_with_id_metadata": metadata_refs,
            "duplicate_resolved_point_columns": len(resolved_columns) - len(point_columns),
            "requested_point_columns_matched": len(point_columns & requested),
            "missing_requested_point_columns": len(requested - point_columns),
            "unexpected_point_columns": len(point_columns - requested),
            "history_value_cells": sum(
                value is not None and type(value).__name__ != "NA"
                for row in rows for key, value in row.items() if key != "ts"
            ),
        })
    return summary, rows


def _scope_hash(ids: tuple[str, ...]) -> str:
    return hashlib.sha256("\n".join(ids).encode("utf-8")).hexdigest()


def _equipment_filter(site_id: str, ids: tuple[str, ...]) -> str:
    selected = " or ".join(f"id==@{identifier}" for identifier in ids)
    return f"equip and siteRef==@{site_id} and ({selected})"


def run_probe(binding: SourceBinding, scope: PilotScope, client: ProbeClient) -> dict[str, object]:
    """Run fixed read-only calls and return a report without source values or IDs."""
    approved_api_url(binding, scope)
    calls: list[dict[str, object]] = []

    def call(
        name: str, requested: tuple[str, ...], operation, *, history: bool = False,
    ) -> list[dict[str, Any]] | None:
        started = time.monotonic()
        entry: dict[str, object] = {
            "operation": name,
            "requested_count": len(requested),
            "requested_scope_sha256": _scope_hash(requested),
        }
        try:
            summary, rows = _summarize(
                operation(), scope, history_ids=requested if history else None,
            )
            entry.update(summary)
            entry["status"] = "succeeded"
            return rows
        except Exception as exc:
            entry["status"] = "failed"
            entry["error_class"] = type(exc).__name__
            return None
        finally:
            entry["elapsed_ms"] = round((time.monotonic() - started) * 1000, 2)
            calls.append(entry)

    for kind in ("equip", "point"):
        call(
            f"metadata_{kind}", (scope.source_site_ref,),
            lambda kind=kind: client.eval(
                f"readAll({kind} and siteRef==@{scope.source_site_ref})"
            ),
        )

    for size in scope.history_batch_sizes:
        ids = scope.history_point_ids[:size]
        call(
            f"history_{size}", ids,
            lambda ids=ids: client.history(ids, scope.window_start_utc, scope.window_end_utc),
            history=True,
        )

    for size in scope.rules_batch_sizes:
        ids = scope.equipment_ids[:size]
        filter_expr = _equipment_filter(scope.source_site_ref, ids)
        equipment_rows = call(
            f"rules_scope_{size}", ids,
            lambda expr=filter_expr: client.eval(f"readAll({expr})"),
        )
        scope_entry = calls[-1]
        if equipment_rows is not None:
            returned = {_ref(row["id"]) for row in equipment_rows if row.get("id") is not None}
            scope_entry["returned_equipment_count"] = len(returned)
            scope_entry["equipment_scope_matches"] = returned == set(ids)
        rule_rows = call(
            f"rules_results_{size}", ids,
            lambda expr=filter_expr: client.eval(
                f"readAll({expr}).ruleSparks({scope.rule_day.isoformat()})"
            ),
        )
        if rule_rows is not None:
            target_refs = {
                _ref(row["targetRef"]) for row in rule_rows if row.get("targetRef") is not None
            }
            calls[-1]["returned_target_count"] = len(target_refs)
            calls[-1]["unexpected_target_count"] = len(target_refs - set(ids))
            calls[-1]["zero_rows_does_not_prove_equipment_coverage"] = not rule_rows

    return {
        "schema_version": 1,
        "probe_kind": "read_only_source_contract",
        "tenant_id": binding.tenant_id,
        "project_id": binding.project_id,
        "site_ref": scope.site_ref,
        "window_start_utc": scope.window_start_utc.isoformat(),
        "window_end_utc": scope.window_end_utc.isoformat(),
        "rule_day": scope.rule_day.isoformat(),
        "source_values_included": False,
        "source_coverage_certified": False,
        "calls": calls,
        "all_calls_succeeded": all(item["status"] == "succeeded" for item in calls),
    }
