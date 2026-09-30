"""Five-minute wide-grid parsing and fail-closed query coverage."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import copy
import importlib.util

import pytest

from ingestion.adapters.skyspark.history import read_history
from ingestion.config.loader import resolve_config
from ingestion.contracts.config import FeedKind
from ingestion.contracts.jobs import CertifiedInventory, SiteInventory
from ingestion.contracts.resources import load_resources
from ingestion.core.failures import NonRetryableSourceError, RawResponseTooLarge
from ingestion.core.planner import plan_run


ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("history_fixture", ROOT / "local/fixture/server.py")
assert SPEC and SPEC.loader
fixture = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(fixture)


class Source:
    def __init__(self, page):
        self.page = page
        self.calls = 0

    def read(self, site_uri, point_ids, start, end):
        self.calls += 1
        return self.page


def _job():
    config = resolve_config(
        ROOT / "config/profiles/default.yaml", ROOT / "config/bindings/example-local.yaml",
        ROOT / "config/manifests/plugins.yaml", environment="local",
    )
    inventory = CertifiedInventory(
        version="reader-test", tenant_id=config.binding.tenant_id,
        project_id=config.binding.project_id,
        sites={
            "site-a": SiteInventory(point_ids=("point-a-1", "point-a-2"),
                                    historized_point_ids=("point-a-1", "point-a-2")),
            "site-b": SiteInventory(),
        },
    )
    end = datetime(2026, 9, 28, 10, 5, tzinfo=timezone.utc)
    _, jobs = plan_run(config, FeedKind.HISTORY, end, inventory=inventory,
                       window_start=end - timedelta(minutes=5), window_end=end)
    return next(job for job in jobs if job.site_ref == "site-a")


def _page(job):
    data = fixture.load_fixture(ROOT / "local/fixture/data.json")
    return fixture.grid(data, "demoSiteA", "history", {
        "point_ids": list(job.scope_ids),
        "window_start": job.window_start.isoformat(),
        "window_end": job.window_end.isoformat(),
    })


def _policy():
    return load_resources(ROOT / "local/resources.yaml").history_read


def test_full_window_and_complete_empty_are_distinct():
    job = _job()
    page = _page(job)
    result = read_history(job, site_uri="demoSiteA", source=Source(page), policy=_policy())
    assert result.completed_ids == job.scope_ids
    assert len(result.observations) == 2
    assert {row.value_kind for row in result.observations} == {"num", "bool"}
    assert result.observations[0].source_timestamp == "2026-09-28T10:00:00Z"
    assert result.observations[0].source_timezone == "UTC"
    assert result.query_id == page["meta"]["query_id"]
    empty = copy.deepcopy(page)
    empty["rows"] = []
    empty["meta"]["returned_rows"] = empty["meta"]["total_rows"] = 0
    result = read_history(job, site_uri="demoSiteA", source=Source(empty), policy=_policy())
    assert result.observations == () and result.completed_ids == job.scope_ids


@pytest.mark.parametrize("change", [
    lambda p: p["meta"].pop("complete"),
    lambda p: p["meta"].update(completed_ids=["point-a-1"]),
    lambda p: p["meta"].update(total_rows=3),
    lambda p: p["meta"].update(truncated=True),
    lambda p: p["cols"].pop(),
    lambda p: p["cols"].append({"name": "point-a-1"}),
    lambda p: p["rows"].append({"ts": "2026-09-28T10:05:00Z", "point-a-1": 1}),
])
def test_partial_cap_or_out_of_window_response_is_rejected(change):
    job = _job()
    page = _page(job)
    change(page)
    with pytest.raises(NonRetryableSourceError):
        read_history(job, site_uri="demoSiteA", source=Source(page), policy=_policy())


def test_response_byte_cap_blocks_before_sink():
    job = _job()
    policy = _policy().model_copy(update={"max_response_bytes": 10})
    with pytest.raises(RawResponseTooLarge):
        read_history(job, site_uri="demoSiteA", source=Source(_page(job)), policy=policy)


def test_explicit_source_status_is_preserved():
    job = _job()
    page = _page(job)
    page["rows"][0]["point-a-1"] = {"value": 21.5, "status": "uncertain"}
    result = read_history(job, site_uri="demoSiteA", source=Source(page), policy=_policy())
    assert result.observations[0].source_status == "uncertain"
