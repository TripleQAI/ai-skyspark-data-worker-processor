from datetime import datetime, timezone
from pathlib import Path

import pytest

from ingestion.config.loader import resolve_config
from ingestion.contracts.config import FeedKind
from ingestion.contracts.jobs import CertifiedInventory
from ingestion.core.planner import plan_run


ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def config():
    return resolve_config(
        ROOT / "config/profiles/default.yaml",
        ROOT / "config/bindings/example-local.yaml",
        ROOT / "config/manifests/plugins.yaml",
        environment="local",
    )


@pytest.fixture
def inventory():
    return CertifiedInventory.model_validate_json(
        (ROOT / "local/fixtures/inventory.json").read_text(encoding="utf-8")
    )


SCHEDULED = datetime(2026, 9, 26, 10, 5, tzinfo=timezone.utc)
START = datetime(2026, 9, 26, 10, 0, tzinfo=timezone.utc)


def test_metadata_one_job_per_approved_site_without_inventory(config):
    run, jobs = plan_run(config, FeedKind.METADATA, SCHEDULED)
    assert len(jobs) == 2
    assert {job.site_ref for job in jobs} == {"site-a", "site-b"}
    assert all(job.run_id == run.run_id and job.scope_ids == () for job in jobs)


def test_history_jobs_are_deterministic_and_cover_all_eligible_ids(config, inventory):
    first_run, first_jobs = plan_run(
        config, FeedKind.HISTORY, SCHEDULED, inventory=inventory,
        window_start=START, window_end=SCHEDULED,
    )
    second_run, second_jobs = plan_run(
        config, FeedKind.HISTORY, SCHEDULED, inventory=inventory,
        window_start=START, window_end=SCHEDULED,
    )
    assert first_run.run_id == second_run.run_id
    assert [job.job_id for job in first_jobs] == [job.job_id for job in second_jobs]
    assert len(first_jobs) == 2
    assert {point for job in first_jobs for point in job.scope_ids} == {
        "point-a-1", "point-a-2", "point-a-3", "point-a-4", "point-a-5",
        "point-b-1", "point-b-2",
    }


def test_rules_plan_equipment_not_detection_rows(config, inventory):
    _, jobs = plan_run(
        config, FeedKind.RULES, SCHEDULED, inventory=inventory,
        window_start=START, window_end=SCHEDULED,
    )
    assert len(jobs) == 2
    assert sum(len(job.scope_ids) for job in jobs) == 4


def test_unapproved_site_is_rejected(config, inventory):
    data = inventory.model_dump()
    data["sites"]["unapproved-site"] = {"equipment_ids": [], "historized_point_ids": []}
    changed = CertifiedInventory.model_validate(data)
    with pytest.raises(ValueError, match="unapproved sites"):
        plan_run(config, FeedKind.RULES, SCHEDULED, inventory=changed,
                 window_start=START, window_end=SCHEDULED)


def test_history_window_must_match_config(config, inventory):
    with pytest.raises(ValueError, match="configured duration"):
        plan_run(config, FeedKind.HISTORY, SCHEDULED, inventory=inventory,
                 window_start=START, window_end=datetime(2026, 9, 26, 10, 4, tzinfo=timezone.utc))


def test_partition_policy_splits_within_each_site(config, inventory):
    data = inventory.model_dump()
    data["sites"]["site-a"]["equipment_ids"] = [f"equip-a-{n:03d}" for n in range(201)]
    data["sites"]["site-a"]["historized_point_ids"] = [f"point-a-{n:03d}" for n in range(501)]
    changed = CertifiedInventory.model_validate(data)
    _, rule_jobs = plan_run(config, FeedKind.RULES, SCHEDULED, inventory=changed,
                            window_start=START, window_end=SCHEDULED)
    _, history_jobs = plan_run(config, FeedKind.HISTORY, SCHEDULED, inventory=changed,
                               window_start=START, window_end=SCHEDULED)
    assert [len(job.scope_ids) for job in rule_jobs if job.site_ref == "site-a"] == [200, 1]
    assert [len(job.scope_ids) for job in history_jobs if job.site_ref == "site-a"] == [500, 1]
    assert all(job.site_ref == "site-b" for job in rule_jobs[-1:] + history_jobs[-1:])


def test_empty_eligible_scope_still_produces_site_window_job(config, inventory):
    data = inventory.model_dump()
    data["sites"]["site-b"] = {"equipment_ids": [], "historized_point_ids": []}
    changed = CertifiedInventory.model_validate(data)
    for feed in (FeedKind.HISTORY, FeedKind.RULES):
        _, jobs = plan_run(config, feed, SCHEDULED, inventory=changed,
                           window_start=START, window_end=SCHEDULED)
        empty_site_jobs = [job for job in jobs if job.site_ref == "site-b"]
        assert len(empty_site_jobs) == 1
        assert empty_site_jobs[0].scope_ids == ()
