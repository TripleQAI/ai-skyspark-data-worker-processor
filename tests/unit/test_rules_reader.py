"""Equipment rule mapping and complete-query coverage."""

from __future__ import annotations

import copy
from datetime import datetime, timedelta, timezone
import importlib.util
from pathlib import Path

import pytest

from ingestion.adapters.skyspark.rules import read_rules
from ingestion.config.loader import resolve_config
from ingestion.contracts.config import FeedKind
from ingestion.contracts.jobs import CertifiedInventory, SiteInventory
from ingestion.contracts.resources import load_resources
from ingestion.core.failures import NonRetryableSourceError
from ingestion.core.planner import plan_run


ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("rules_fixture", ROOT / "local/fixture/server.py")
assert SPEC and SPEC.loader
fixture = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(fixture)


class Source:
    def __init__(self, page):
        self.page = page
        self.calls = 0

    def read(self, site_uri, equipment_ids, day, start, end, source_timezone):
        self.calls += 1
        return self.page


def _config():
    return resolve_config(
        ROOT / "config/profiles/default.yaml", ROOT / "config/bindings/example-local.yaml",
        ROOT / "config/manifests/plugins.yaml", environment="local",
    )


def _jobs():
    config = _config()
    inventory = CertifiedInventory(
        version="rules-reader", tenant_id=config.binding.tenant_id,
        project_id=config.binding.project_id,
        sites={"site-a": SiteInventory(equipment_ids=("equip-a-1", "equip-a-2")),
               "site-b": SiteInventory(equipment_ids=("equip-b-1",))},
    )
    start = datetime(2026, 9, 27, tzinfo=timezone.utc)
    return plan_run(config, FeedKind.RULES, start + timedelta(days=1),
                    inventory=inventory, window_start=start,
                    window_end=start + timedelta(days=1))[1]


def _page(ids=("equip-a-1", "equip-a-2")):
    data = fixture.load_fixture(ROOT / "local/fixture/data.json")
    return fixture.grid(data, "demoSiteA", "rules", {
        "equipment_ids": list(ids), "day": "2026-09-27", "timezone": "UTC",
        "window_start": "2026-09-27T00:00:00+00:00",
        "window_end": "2026-09-28T00:00:00+00:00",
    })


def _read(job, page):
    return read_rules(job, site_uri="demoSiteA", source=Source(page),
                      policy=load_resources(ROOT / "local/resources.yaml").rules_read,
                      source_timezone="UTC", allowed_tz_tags=("UTC",))


def test_full_equipment_coverage_fields_and_zero_detection():
    job = next(job for job in _jobs() if job.site_ref == "site-a")
    result = _read(job, _page())
    assert result.completed_ids == ("equip-a-1", "equip-a-2")
    assert len(result.detections) == 1
    row = result.detections[0]
    assert (row.equipment_id, row.rule_id, row.source_date.isoformat()) == (
        "equip-a-1", "rule-demo-1", "2026-09-27",
    )
    assert (row.duration, row.periods, row.point_ids, row.priority,
            row.severity) == (300, 1, ("point-a-1",), 2, "warning")
    assert row.model_dump(mode="json")["source_timezone"] == "UTC"
    empty_page = _page()
    empty_page["rows"] = []
    empty_page["meta"]["returned_rows"] = empty_page["meta"]["total_rows"] = 0
    assert _read(job, empty_page).completed_ids == job.scope_ids
    assert _read(job, empty_page).detections == ()


@pytest.mark.parametrize("mutate", [
    lambda p: p["meta"].pop("complete"),
    lambda p: p["meta"].update(completed_ids=["equip-a-1"]),
    lambda p: p["meta"].update(truncated=True),
    lambda p: p["meta"].update(total_rows=2),
    lambda p: p["meta"].update(day="2026-09-26"),
    lambda p: p["meta"].update(timezone="America/Chicago"),
    lambda p: p["meta"].update(window_end="2026-09-27T23:00:00+00:00"),
    lambda p: p["rows"][0].update(targetRef="equip-b-1"),
    lambda p: p["rows"][0].update(date="2026-09-26"),
    lambda p: p["rows"][0].update(tz="other"),
    lambda p: p["rows"].append(copy.deepcopy(p["rows"][0])),
])
def test_partial_mismatched_or_ambiguous_query_is_rejected(mutate):
    job = next(job for job in _jobs() if job.site_ref == "site-a")
    page = _page()
    mutate(page)
    with pytest.raises(NonRetryableSourceError):
        _read(job, page)


def test_same_candidate_keeps_key_and_changes_revision_for_corrected_content():
    job = next(job for job in _jobs() if job.site_ref == "site-a")
    first = _read(job, _page()).detections[0]
    corrected = _page()
    corrected["rows"][0]["severity"] = "critical"
    second = _read(job, corrected).detections[0]
    assert first.detection_key == second.detection_key
    assert first.revision_hash != second.revision_hash


def test_250_equipment_ids_are_partitioned_once_at_200_cap():
    config = _config()
    inventory = CertifiedInventory(
        version="250-equipment", tenant_id=config.binding.tenant_id,
        project_id=config.binding.project_id,
        sites={"site-a": SiteInventory(
            equipment_ids=tuple(f"equip-{i:03}" for i in range(249)),
        ), "site-b": SiteInventory(equipment_ids=("equip-249",))},
    )
    start = datetime(2026, 9, 27, tzinfo=timezone.utc)
    _, jobs = plan_run(config, FeedKind.RULES, start + timedelta(days=1),
                       inventory=inventory, window_start=start,
                       window_end=start + timedelta(days=1))
    assert [len(job.scope_ids) for job in jobs] == [200, 49, 1]
    assert len({identifier for job in jobs for identifier in job.scope_ids}) == 250
