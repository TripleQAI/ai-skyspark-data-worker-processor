"""Site-namespaced virtual IDs for scale-test replicas of one real source site.

A replica site presents ``equipment_per_site`` equipment, each with
``points_per_equipment`` historized points. Every virtual ID embeds the real
source ID it reads, so history and rules readers can map a job back to real
SkySpark IDs without any inventory lookup:

    equipment  <site_ref>.e<i>.<real equipment id>
    point      <site_ref>.e<i>.p<j>.<real historized point id>

Equipment cycle through the real site's equipment; points cycle through its
historized points. The topology is synthetic; the values are real.
"""

from __future__ import annotations

import copy
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from ingestion.contracts.config import SourceReplica
from ingestion.core.failures import NonRetryableSourceError


_REAL = re.compile(r"[A-Za-z0-9_:\-.]+")


@dataclass(frozen=True, slots=True)
class ReplicaShape:
    equipment_per_site: int
    points_per_equipment: int

    @classmethod
    def from_source(cls, value: Mapping[str, Any] | None) -> "ReplicaShape | None":
        if value is None:
            return None
        replica = SourceReplica.model_validate(value)
        return cls(replica.equipment_per_site, replica.points_per_equipment)

    def _width(self, count: int) -> int:
        return len(str(count - 1))

    def equipment_id(self, site_ref: str, index: int, real_id: str) -> str:
        return f"{site_ref}.e{index:0{self._width(self.equipment_per_site)}d}.{real_id}"

    def point_id(self, site_ref: str, equipment: int, point: int, real_id: str) -> str:
        return (f"{site_ref}.e{equipment:0{self._width(self.equipment_per_site)}d}"
                f".p{point:0{self._width(self.points_per_equipment)}d}.{real_id}")


def _decode(site_ref: str, virtual_id: str, pattern: re.Pattern[str]) -> str:
    prefix = f"{site_ref}."
    match = pattern.fullmatch(virtual_id[len(prefix):]) if virtual_id.startswith(prefix) else None
    if match is None or not _REAL.fullmatch(match["real"]):
        raise NonRetryableSourceError("replica ID does not belong to this site")
    return match["real"]


_EQUIPMENT = re.compile(r"e\d+\.(?P<real>.+)")
_POINT = re.compile(r"e\d+\.p\d+\.(?P<real>.+)")


def real_equipment_id(site_ref: str, virtual_id: str) -> str:
    return _decode(site_ref, virtual_id, _EQUIPMENT)


def real_point_id(site_ref: str, virtual_id: str) -> str:
    return _decode(site_ref, virtual_id, _POINT)


def real_ids(site_ref: str, virtual_ids: Iterable[str], *, points: bool) -> dict[str, tuple[str, ...]]:
    """Map each distinct real ID to the requested virtual IDs that read it."""
    decode = real_point_id if points else real_equipment_id
    mapping: dict[str, list[str]] = {}
    for virtual in virtual_ids:
        mapping.setdefault(decode(site_ref, virtual), []).append(virtual)
    return {real: tuple(virtuals) for real, virtuals in mapping.items()}


def _ref(value: str) -> dict[str, str]:
    return {"_kind": "ref", "val": value}


def _virtual_equipment_ids(
    shape: ReplicaShape, site_ref: str, equipment: list[dict[str, Any]],
) -> list[tuple[str, dict[str, Any]]]:
    if not equipment:
        raise NonRetryableSourceError("replica source site has no equipment")
    return [
        (shape.equipment_id(site_ref, index, _id(equipment[index % len(equipment)])),
         equipment[index % len(equipment)])
        for index in range(shape.equipment_per_site)
    ]


def expand_equipment(
    shape: ReplicaShape, site_ref: str, equipment: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Virtual equipment rows from JSON-encoded real rows sorted by ID.

    Each row keeps the real tags, including the real ``siteRef``, and records
    the real record it copies under ``replicaOf``.
    """
    rows: list[dict[str, Any]] = []
    for virtual_id, source in _virtual_equipment_ids(shape, site_ref, equipment):
        row = copy.deepcopy(source)
        row["replicaOf"] = _ref(_id(source))
        row["id"] = _ref(virtual_id)
        rows.append(row)
    return rows


def expand_points(
    shape: ReplicaShape, site_ref: str,
    equipment: list[dict[str, Any]], historized_points: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Virtual historized points, cycling through the real historized points."""
    if not historized_points:
        raise NonRetryableSourceError("replica source site has no historized points")
    rows: list[dict[str, Any]] = []
    for index, (equipment_id, _) in enumerate(_virtual_equipment_ids(shape, site_ref, equipment)):
        for point in range(shape.points_per_equipment):
            source = historized_points[
                (index * shape.points_per_equipment + point) % len(historized_points)
            ]
            real = _id(source)
            row = copy.deepcopy(source)
            row["replicaOf"] = _ref(real)
            row["id"] = _ref(shape.point_id(site_ref, index, point, real))
            row["equipRef"] = _ref(equipment_id)
            rows.append(row)
    return rows


def _id(row: Mapping[str, Any]) -> str:
    value = row.get("id")
    if isinstance(value, Mapping):
        value = value.get("val")
    if not isinstance(value, str) or not _REAL.fullmatch(value):
        raise NonRetryableSourceError("replica source row has no usable id")
    return value
