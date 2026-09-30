"""Read-only assessment of the original equipment and point CSV exports.

An export has no query-completeness receipt, so this module never produces a
CertifiedInventory. It preserves the source IDs and reports every excluded row.
"""

from __future__ import annotations

from collections import Counter
import csv
from dataclasses import asdict, dataclass
import hashlib
from pathlib import Path
from typing import Iterator

from ingestion.contracts.resources import MetadataInspectionPolicy


@dataclass(frozen=True, slots=True)
class MetadataIssue:
    code: str
    kind: str
    row_number: int
    source_id: str | None


@dataclass(frozen=True, slots=True)
class MetadataAssessment:
    project_key: str
    equipment_rows: int
    point_rows: int
    equipment_ids_by_site: dict[str, tuple[str, ...]]
    point_ids_by_site: dict[str, tuple[str, ...]]
    historized_point_ids_by_site: dict[str, tuple[str, ...]]
    site_level_point_ids_by_site: dict[str, tuple[str, ...]]
    issue_counts: dict[str, int]
    issue_examples: tuple[MetadataIssue, ...]
    equipment_sha256: str
    points_sha256: str

    def summary(self) -> dict[str, object]:
        sites = {
            site_ref: {
                "equipment": len(self.equipment_ids_by_site[site_ref]),
                "points": len(self.point_ids_by_site[site_ref]),
                "historized_points": len(self.historized_point_ids_by_site[site_ref]),
                "site_level_points": len(self.site_level_point_ids_by_site[site_ref]),
            }
            for site_ref in sorted(self.equipment_ids_by_site)
        }
        return {
            "project_key": self.project_key,
            "equipment_rows": self.equipment_rows,
            "point_rows": self.point_rows,
            "accepted_equipment": sum(item["equipment"] for item in sites.values()),
            "accepted_points": sum(item["points"] for item in sites.values()),
            "accepted_historized_points": sum(item["historized_points"] for item in sites.values()),
            "site_level_points": sum(item["site_level_points"] for item in sites.values()),
            "sites": sites,
            "issue_counts": self.issue_counts,
            "issue_examples": [asdict(issue) for issue in self.issue_examples],
            "equipment_sha256": self.equipment_sha256,
            "points_sha256": self.points_sha256,
            "certified": False,
            "certification_reason": "CSV exports contain no complete source-query receipts",
        }


def _fingerprint(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _rows(
    path: Path, required: set[str], *, max_bytes: int, max_rows: int,
) -> Iterator[tuple[int, dict[str, str | None]]]:
    if not path.is_file():
        raise ValueError(f"CSV file does not exist: {path}")
    if path.stat().st_size > max_bytes:
        raise ValueError(f"CSV file exceeds configured byte limit: {path}")
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        headers = reader.fieldnames
        if not headers or len(headers) != len(set(headers)) or not required.issubset(headers):
            raise ValueError(f"CSV headers are missing or duplicated: {path}")
        for row_number, row in enumerate(reader, start=2):
            if row_number - 1 > max_rows:
                raise ValueError(f"CSV exceeds configured row limit: {path}")
            if None in row:
                raise ValueError(f"CSV row has extra columns at line {row_number}: {path}")
            yield row_number, row


def assess_metadata_exports(
    equipment_path: Path,
    points_path: Path,
    *, project_key: str,
    expected_site_refs: set[str],
    policy: MetadataInspectionPolicy,
) -> MetadataAssessment:
    """Classify every source row without certifying export completeness."""
    if not project_key or not expected_site_refs or any(not item for item in expected_site_refs):
        raise ValueError("project key and expected site refs are required")
    equipment_before, points_before = _fingerprint(equipment_path), _fingerprint(points_path)
    true_values = {value.strip().casefold() for value in policy.historized_true_values}
    false_values = {value.strip().casefold() for value in policy.historized_false_values}
    equipment_ids: dict[str, list[str]] = {site: [] for site in expected_site_refs}
    point_ids: dict[str, list[str]] = {site: [] for site in expected_site_refs}
    historized_ids: dict[str, list[str]] = {site: [] for site in expected_site_refs}
    site_level_ids: dict[str, list[str]] = {site: [] for site in expected_site_refs}
    equipment_site_by_id: dict[str, str] = {}
    seen_equipment: set[str] = set()
    seen_points: set[str] = set()
    counts: Counter[str] = Counter()
    examples: list[MetadataIssue] = []
    equipment_rows = point_rows = 0

    def issue(code: str, kind: str, line: int, source_id: str | None) -> None:
        counts[code] += 1
        if len(examples) < policy.max_issue_examples:
            examples.append(MetadataIssue(code, kind, line, source_id))

    for line, row in _rows(
        equipment_path, {"fsk", "id", "siteRef"},
        max_bytes=policy.max_csv_bytes, max_rows=policy.max_equipment_rows,
    ):
        equipment_rows += 1
        source_id = (row["id"] or "").strip()
        site_ref = (row["siteRef"] or "").strip()
        if (row["fsk"] or "").strip() != project_key:
            issue("wrong_project", "equipment", line, source_id or None)
            continue
        if not source_id:
            issue("missing_id", "equipment", line, None)
            continue
        if source_id in seen_equipment:
            issue("duplicate_equipment_id", "equipment", line, source_id)
            continue
        seen_equipment.add(source_id)
        if not site_ref:
            issue("missing_site_ref", "equipment", line, source_id)
            continue
        if site_ref not in expected_site_refs:
            issue("unexpected_site_ref", "equipment", line, source_id)
            continue
        equipment_site_by_id[source_id] = site_ref
        equipment_ids[site_ref].append(source_id)

    for line, row in _rows(
        points_path, {"fsk", "id", "siteRef", "equipRef", "his"},
        max_bytes=policy.max_csv_bytes, max_rows=policy.max_point_rows,
    ):
        point_rows += 1
        source_id = (row["id"] or "").strip()
        site_ref = (row["siteRef"] or "").strip()
        equip_ref = (row["equipRef"] or "").strip()
        if (row["fsk"] or "").strip() != project_key:
            issue("wrong_project", "point", line, source_id or None)
            continue
        if not source_id:
            issue("missing_id", "point", line, None)
            continue
        if source_id in seen_points:
            issue("duplicate_point_id", "point", line, source_id)
            continue
        seen_points.add(source_id)
        if not site_ref:
            issue("missing_site_ref", "point", line, source_id)
            continue
        if site_ref not in expected_site_refs:
            issue("unexpected_site_ref", "point", line, source_id)
            continue
        if equip_ref:
            equip_site = equipment_site_by_id.get(equip_ref)
            if equip_site is None:
                issue("unknown_equipment_ref", "point", line, source_id)
                continue
            if equip_site != site_ref:
                issue("cross_site_equipment_ref", "point", line, source_id)
                continue
        marker = (row["his"] or "").strip().casefold()
        if marker not in true_values and marker not in false_values:
            issue("unknown_historized_marker", "point", line, source_id)
            continue
        point_ids[site_ref].append(source_id)
        if marker in true_values:
            historized_ids[site_ref].append(source_id)
        if not equip_ref:
            site_level_ids[site_ref].append(source_id)

    if _fingerprint(equipment_path) != equipment_before or _fingerprint(points_path) != points_before:
        raise ValueError("CSV file changed during inspection")

    def sorted_ids(values: dict[str, list[str]]) -> dict[str, tuple[str, ...]]:
        return {site: tuple(sorted(ids)) for site, ids in values.items()}

    return MetadataAssessment(
        project_key=project_key, equipment_rows=equipment_rows,
        point_rows=point_rows, equipment_ids_by_site=sorted_ids(equipment_ids),
        point_ids_by_site=sorted_ids(point_ids),
        historized_point_ids_by_site=sorted_ids(historized_ids),
        site_level_point_ids_by_site=sorted_ids(site_level_ids),
        issue_counts=dict(sorted(counts.items())), issue_examples=tuple(examples),
        equipment_sha256=equipment_before, points_sha256=points_before,
    )
