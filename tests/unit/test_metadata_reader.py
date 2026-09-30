"""Metadata source contracts must fail closed before an inventory is built."""

from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from ingestion.adapters.skyspark.metadata import PhableMetadataSource, read_site_metadata
from ingestion.config.loader import resolve_config
from ingestion.contracts.config import FeedKind
from ingestion.contracts.resources import load_resources
from ingestion.core.failures import NonRetryableSourceError, RawResponseTooLarge
from ingestion.core.planner import plan_run


ROOT = Path(__file__).resolve().parents[2]


def _job_and_policy():
    config = resolve_config(
        ROOT / "config/profiles/default.yaml",
        ROOT / "config/bindings/example-local.yaml",
        ROOT / "config/manifests/plugins.yaml", environment="local",
    )
    _, jobs = plan_run(config, FeedKind.METADATA, datetime(2026, 9, 27, tzinfo=timezone.utc))
    return jobs[0], load_resources(ROOT / "local/resources.yaml").metadata_read


class Pages:
    def __init__(self, equipment, points, meta_changes=None):
        self._rows = {"equipment": equipment, "points": points}
        self._changes = meta_changes or {}

    def read(self, kind, site_uri, page_token):
        assert page_token is None
        rows = self._rows[kind]
        meta = {
            "project": "demo-project", "site": site_uri, "snapshot": "snapshot-1",
            "page_index": 0, "next_page": None, "complete": True,
            "returned_rows": len(rows), "total_rows": len(rows),
        }
        meta.update(self._changes.get(kind, {}))
        return {"meta": meta, "cols": [], "rows": rows}


def test_reader_preserves_tags_links_and_site_level_historized_point():
    job, policy = _job_and_policy()
    source = Pages(
        [{"id": {"_kind": "ref", "val": "equip-1"}, "siteRef": "r:demoSiteA Label", "navName": "AHU"}],
        [
            {"id": "r:point-1", "siteRef": "demoSiteA", "equipRef": "r:equip-1", "his": "m:"},
            {"id": "point-site", "siteRef": "demoSiteA", "his": True},
            {"id": "point-nohis", "siteRef": "demoSiteA", "equipRef": "equip-1", "his": False},
        ],
    )
    result = read_site_metadata(job, site_uri="demoSiteA", source=source, policy=policy)
    assert result.snapshot_token == "snapshot-1"
    assert len(result.rows) == 4
    assert result.rows[0]["tags"]["navName"] == "AHU"
    assert result.rows[1]["equipment_ref"] == "equip-1"
    assert result.rows[1]["historized"] is True
    assert result.rows[2]["site_level"] is True
    assert result.rows[3]["historized"] is False
    assert b'"snapshot":"snapshot-1"' in result.raw_response


@pytest.mark.parametrize("change", [
    {"complete": False}, {"total_rows": 2}, {"snapshot": None},
    {"page_index": 1}, {"site": "wrong"},
])
def test_reader_rejects_missing_or_mismatched_completion_receipt(change):
    job, policy = _job_and_policy()
    source = Pages([], [], meta_changes={"equipment": change})
    with pytest.raises(NonRetryableSourceError):
        read_site_metadata(job, site_uri="demoSiteA", source=source, policy=policy)


def test_reader_rejects_unknown_equipment_and_duplicate_point():
    job, policy = _job_and_policy()
    equipment = [{"id": "equip-1", "siteRef": "demoSiteA"}]
    with pytest.raises(NonRetryableSourceError, match="unknown"):
        read_site_metadata(job, site_uri="demoSiteA", policy=policy, source=Pages(
            equipment, [{"id": "point-1", "siteRef": "demoSiteA", "equipRef": "missing"}],
        ))
    point = {"id": "point-1", "siteRef": "demoSiteA", "equipRef": "equip-1"}
    with pytest.raises(NonRetryableSourceError, match="point ID"):
        read_site_metadata(job, site_uri="demoSiteA", policy=policy, source=Pages(
            equipment, [point, point],
        ))


def test_reader_caps_rows_and_page_bytes():
    job, policy = _job_and_policy()
    source = Pages([{"id": "equip-1", "siteRef": "demoSiteA"}], [])
    with pytest.raises(NonRetryableSourceError, match="page rows"):
        read_site_metadata(job, site_uri="demoSiteA", source=source, policy=policy.model_copy(
            update={"max_rows_per_page": 0},
        ))
    with pytest.raises(RawResponseTooLarge, match="byte cap"):
        read_site_metadata(job, site_uri="demoSiteA", source=source, policy=policy.model_copy(
            update={"max_page_bytes": 1},
        ))


def test_reader_requires_every_page_in_one_snapshot():
    job, policy = _job_and_policy()

    class Paged:
        def read(self, kind, site_uri, page_token):
            if kind == "equipment" and page_token is None:
                rows, index, following, complete = (
                    [{"id": "e1", "siteRef": site_uri}], 0, "second", False,
                )
            elif kind == "equipment" and page_token == "second":
                rows, index, following, complete = (
                    [{"id": "e2", "siteRef": site_uri}], 1, None, True,
                )
            else:
                rows, index, following, complete = [], 0, None, True
            return {"meta": {
                "project": job.project_id, "site": site_uri, "snapshot": "same",
                "page_index": index, "next_page": following, "complete": complete,
                "returned_rows": len(rows), "total_rows": 2 if kind == "equipment" else 0,
            }, "cols": [], "rows": rows}

    assert len(read_site_metadata(
        job, site_uri="demoSiteA", source=Paged(), policy=policy,
    ).rows) == 2


def test_observed_phable_grid_without_completeness_receipt_fails_closed():
    job, policy = _job_and_policy()

    class Client:
        def call(self, operation, request):
            assert operation == "eval"
            return SimpleNamespace(meta={"ver": "3.0"}, cols=[], rows=[])

    with pytest.raises(NonRetryableSourceError, match="snapshot token"):
        read_site_metadata(
            job, site_uri="demoSiteA", source=PhableMetadataSource(Client()),
            policy=policy,
        )
