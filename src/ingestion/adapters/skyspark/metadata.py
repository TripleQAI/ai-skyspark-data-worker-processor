"""Bounded site metadata reads with explicit, provider-supplied page receipts.

The observed SkySpark readAll grid has no completeness receipt. It can be
inspected here, but it cannot pass this reader's certification boundary until
the source supplies a stable snapshot/page contract.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time
import json
import re
from typing import Any, Protocol
from urllib.parse import quote, urljoin
from urllib.request import urlopen

from ingestion.contracts.jobs import Job
from ingestion.contracts.resources import MetadataReadPolicy
from ingestion.core.failures import NonRetryableSourceError, RawResponseTooLarge


_REF = re.compile(r"[A-Za-z0-9_:\-.]+")
_SITE_PATH = re.compile(r"[A-Za-z0-9_:\-./]+")


class MetadataPageSource(Protocol):
    def read(self, kind: str, site_uri: str, page_token: str | None) -> dict[str, Any]: ...


@dataclass(frozen=True, slots=True)
class MetadataExtraction:
    rows: tuple[dict[str, object], ...]
    raw_response: bytes
    query_id: str
    snapshot_token: str


def _json_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if isinstance(value, (date, datetime, time)):
        return value.isoformat()
    if hasattr(value, "val"):
        return {"_kind": type(value).__name__.casefold(), "val": _json_value(value.val)}
    if type(value).__name__.casefold() == "marker":
        return {"_kind": "marker"}
    raise NonRetryableSourceError("unsupported Haystack metadata value")


def _reference(value: object) -> str:
    if isinstance(value, Mapping):
        if value.get("_kind") != "ref":
            raise NonRetryableSourceError("metadata reference has wrong Haystack kind")
        value = value.get("val")
    if isinstance(value, str) and value.startswith("r:"):
        value = value[2:].split(" ", 1)[0]
    if not isinstance(value, str) or not _REF.fullmatch(value):
        raise NonRetryableSourceError("metadata reference is missing or malformed")
    return value


def _historized(value: object) -> bool:
    if value is None or value is False or value == "" or value == "false":
        return False
    if value is True or value == "m:" or value == "true":
        return True
    if isinstance(value, Mapping) and value.get("_kind") == "marker":
        return True
    raise NonRetryableSourceError("point has an unrecognized historized tag")


def _page_bytes(page: dict[str, Any], maximum: int) -> bytes:
    try:
        encoded = json.dumps(page, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    except (TypeError, ValueError) as exc:
        raise NonRetryableSourceError("metadata page is not JSON serializable") from exc
    if len(encoded) > maximum:
        raise RawResponseTooLarge("metadata page exceeds configured byte cap")
    return encoded


def read_site_metadata(
    job: Job, *, site_uri: str, source: MetadataPageSource,
    policy: MetadataReadPolicy,
) -> MetadataExtraction:
    """Read both kinds from one snapshot; reject missing pages or bad links."""
    if job.feed.value != "metadata" or job.scope_ids or not site_uri:
        raise ValueError("metadata reader requires one approved site job")
    snapshot: str | None = None
    pages: list[dict[str, Any]] = []
    by_kind: dict[str, list[dict[str, Any]]] = {}
    for kind in ("equipment", "points"):
        token: str | None = None
        seen_tokens: set[str] = set()
        rows: list[dict[str, Any]] = []
        for index in range(policy.max_pages_per_kind):
            page = source.read(kind, site_uri, token)
            if not isinstance(page, dict) or not isinstance(page.get("meta"), dict):
                raise NonRetryableSourceError("metadata source returned no page receipt")
            _page_bytes(page, policy.max_page_bytes)
            meta = page["meta"]
            if meta.get("err") is not None:
                raise NonRetryableSourceError("metadata source returned an error grid")
            current = meta.get("snapshot")
            if not isinstance(current, str) or not current:
                raise NonRetryableSourceError("metadata page has no stable snapshot token")
            if snapshot is None:
                snapshot = current
            if (current != snapshot or meta.get("project") != job.project_id
                    or meta.get("site") != site_uri or meta.get("page_index") != index):
                raise NonRetryableSourceError("metadata page scope or snapshot changed")
            part = page.get("rows")
            if (not isinstance(part, list) or any(not isinstance(row, dict) for row in part)
                    or len(part) > policy.max_rows_per_page
                    or meta.get("returned_rows") != len(part)):
                raise NonRetryableSourceError("metadata page rows differ from receipt")
            rows.extend(part)
            if len(rows) > policy.max_entities_per_site:
                raise RawResponseTooLarge("metadata site exceeds entity cap")
            pages.append({"kind": kind, "page": page})
            following = meta.get("next_page")
            if following is None:
                if meta.get("complete") is not True or meta.get("total_rows") != len(rows):
                    raise NonRetryableSourceError("metadata query has no complete row-count receipt")
                break
            if (meta.get("complete") is True or not isinstance(following, str)
                    or not following or following in seen_tokens):
                raise NonRetryableSourceError("metadata paging receipt is invalid")
            seen_tokens.add(following)
            token = following
        else:
            raise RawResponseTooLarge("metadata query exceeds page cap")
        by_kind[kind] = rows

    equipment: dict[str, dict[str, object]] = {}
    normalized: list[dict[str, object]] = []
    for raw in by_kind["equipment"]:
        tags = _json_value(raw)
        identifier = _reference(tags.get("id"))
        if identifier in equipment or _reference(tags.get("siteRef")) != site_uri:
            raise NonRetryableSourceError("equipment ID or site reference is invalid")
        item = {
            "kind": "equipment", "source_id": identifier,
            "site_ref": job.site_ref, "equipment_ref": None,
            "historized": False, "tags": tags,
        }
        equipment[identifier] = item
        normalized.append(item)
    seen_points: set[str] = set()
    for raw in by_kind["points"]:
        tags = _json_value(raw)
        identifier = _reference(tags.get("id"))
        if identifier in seen_points or _reference(tags.get("siteRef")) != site_uri:
            raise NonRetryableSourceError("point ID or site reference is invalid")
        seen_points.add(identifier)
        equipment_ref = tags.get("equipRef")
        parent = None if equipment_ref in (None, "") else _reference(equipment_ref)
        if parent is not None and parent not in equipment:
            raise NonRetryableSourceError("point references an unknown site equipment")
        normalized.append({
            "kind": "point", "source_id": identifier,
            "site_ref": job.site_ref, "equipment_ref": parent,
            "site_level": parent is None,
            "historized": _historized(tags.get("his")), "tags": tags,
        })
    raw_response = json.dumps(
        {"schema_version": 1, "snapshot": snapshot, "pages": pages},
        sort_keys=True, separators=(",", ":"), allow_nan=False,
    ).encode()
    return MetadataExtraction(
        rows=tuple(normalized), raw_response=raw_response,
        query_id=f"metadata/{snapshot}/{job.site_ref}", snapshot_token=snapshot or "",
    )


class FixtureMetadataSource:
    """Local contract fixture transport; the fixture asserts page completeness."""

    def __init__(self, endpoint: str, *, max_page_bytes: int, timeout_seconds: float):
        if not endpoint.endswith("/"):
            raise ValueError("fixture API root must end in /")
        self._endpoint = endpoint
        self._maximum = max_page_bytes
        self._timeout = timeout_seconds

    def read(self, kind: str, site_uri: str, page_token: str | None) -> dict[str, Any]:
        if page_token is not None:
            raise NonRetryableSourceError("fixture does not support metadata paging")
        if kind not in {"equipment", "points"} or not _SITE_PATH.fullmatch(site_uri) or ".." in site_uri:
            raise ValueError("invalid fixture query scope")
        url = urljoin(self._endpoint, f"{quote(site_uri)}/fixtures/{kind}")
        with urlopen(url, timeout=self._timeout) as response:
            payload = response.read(self._maximum + 1)
        if len(payload) > self._maximum:
            raise RawResponseTooLarge("metadata fixture response exceeds byte cap")
        result = json.loads(payload)
        if not isinstance(result, dict):
            raise NonRetryableSourceError("fixture returned no metadata grid")
        return result


class PhableMetadataSource:
    """Read-only Haystack transport; observed grids fail closed without receipts."""

    def __init__(self, client: Any):
        self._client = client

    def read(self, kind: str, site_uri: str, page_token: str | None) -> dict[str, Any]:
        if (page_token is not None or kind not in {"equipment", "points"}
                or not _REF.fullmatch(site_uri)):
            raise ValueError("unsupported Haystack metadata query")
        from ingestion.source_probe import PhableProbeClient

        grid = PhableProbeClient(self._client).eval(
            f"readAll({'equip' if kind == 'equipment' else 'point'} and siteRef==@{site_uri})"
        )
        return {
            "meta": _json_value(grid.meta),
            "cols": [{"name": column.name, "meta": _json_value(column.meta)} for column in grid.cols],
            "rows": _json_value(grid.rows),
        }
