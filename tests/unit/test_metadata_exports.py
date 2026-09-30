"""Read-only metadata export checks with synthetic source records."""

import csv
import json
from pathlib import Path

import pytest

from ingestion.adapters.skyspark.metadata_exports import assess_metadata_exports
from ingestion.contracts.resources import MetadataInspectionPolicy, load_resources
from ingestion.control_cli import main as control_main


ROOT = Path(__file__).resolve().parents[2]


def _write(path: Path, columns: tuple[str, ...], rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def _exports(tmp_path: Path) -> tuple[Path, Path]:
    equipment = tmp_path / "equipment.csv"
    points = tmp_path / "points.csv"
    _write(equipment, ("fsk", "id", "siteRef"), [
        {"fsk": "1001", "id": "e-a", "siteRef": "site-a"},
        {"fsk": "1001", "id": "e-b", "siteRef": "site-b"},
        {"fsk": "1001", "id": "e-a", "siteRef": "site-a"},
    ])
    _write(points, ("fsk", "id", "siteRef", "equipRef", "his"), [
        {"fsk": "1001", "id": "p-a", "siteRef": "site-a", "equipRef": "e-a", "his": "True"},
        {"fsk": "1001", "id": "p-site", "siteRef": "site-a", "equipRef": "", "his": "m:"},
        {"fsk": "1001", "id": "p-cross", "siteRef": "site-a", "equipRef": "e-b", "his": "True"},
        {"fsk": "1001", "id": "p-unknown", "siteRef": "site-b", "equipRef": "missing", "his": "True"},
        {"fsk": "1001", "id": "p-marker", "siteRef": "site-a", "equipRef": "e-a", "his": "maybe"},
        {"fsk": "1001", "id": "p-orphan", "siteRef": "", "equipRef": "", "his": ""},
        {"fsk": "1001", "id": "p-a", "siteRef": "site-a", "equipRef": "e-a", "his": "True"},
        {"fsk": "wrong", "id": "p-wrong", "siteRef": "site-a", "equipRef": "e-a", "his": "True"},
        {"fsk": "1001", "id": "p-other", "siteRef": "unapproved", "equipRef": "", "his": "True"},
    ])
    return equipment, points


def test_metadata_assessment_classifies_all_rows_without_certifying(tmp_path):
    equipment, points = _exports(tmp_path)
    policy = load_resources(ROOT / "local/resources.yaml").metadata_inspection
    assessment = assess_metadata_exports(
        equipment, points, project_key="1001",
        expected_site_refs={"site-a", "site-b"}, policy=policy,
    )
    summary = assessment.summary()
    assert (summary["equipment_rows"], summary["point_rows"]) == (3, 9)
    assert (summary["accepted_equipment"], summary["accepted_points"]) == (2, 2)
    assert (summary["accepted_historized_points"], summary["site_level_points"]) == (2, 1)
    assert assessment.historized_point_ids_by_site["site-a"] == ("p-a", "p-site")
    assert assessment.issue_counts == {
        "cross_site_equipment_ref": 1,
        "duplicate_equipment_id": 1,
        "duplicate_point_id": 1,
        "missing_site_ref": 1,
        "unknown_equipment_ref": 1,
        "unknown_historized_marker": 1,
        "unexpected_site_ref": 1,
        "wrong_project": 1,
    }
    assert not summary["certified"]
    assert len(summary["equipment_sha256"]) == len(summary["points_sha256"]) == 64


def test_inspection_cli_reports_issues_without_database(tmp_path, capsys, monkeypatch):
    equipment, points = _exports(tmp_path)
    monkeypatch.delenv("CONTROL_DATABASE_URL", raising=False)
    assert control_main([
        "inspect-metadata-export", "--equipment", str(equipment),
        "--points", str(points), "--resources", str(ROOT / "local/resources.yaml"),
        "--project-key", "1001", "--site-ref", "site-a", "--site-ref", "site-b",
    ]) == 2
    result = json.loads(capsys.readouterr().out)
    assert result["issue_counts"]["missing_site_ref"] == 1
    assert result["certified"] is False


def test_inspection_rejects_truncated_or_over_limit_csv(tmp_path):
    equipment, points = _exports(tmp_path)
    policy = load_resources(ROOT / "local/resources.yaml").metadata_inspection
    with pytest.raises(ValueError, match="row limit"):
        assess_metadata_exports(
            equipment, points, project_key="1001",
            expected_site_refs={"site-a", "site-b"},
            policy=policy.model_copy(update={"max_point_rows": 2}),
        )
    points.write_text("fsk,id,id,siteRef,equipRef,his\n", encoding="utf-8")
    with pytest.raises(ValueError, match="headers"):
        assess_metadata_exports(
            equipment, points, project_key="1001",
            expected_site_refs={"site-a", "site-b"}, policy=policy,
        )


def test_historized_marker_policy_rejects_overlap():
    policy = load_resources(ROOT / "local/resources.yaml").metadata_inspection
    data = policy.model_dump()
    data["historized_false_values"] = ["FALSE", "TRUE"]
    with pytest.raises(ValueError, match="overlap"):
        MetadataInspectionPolicy.model_validate(data)
