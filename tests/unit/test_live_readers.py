from __future__ import annotations

import hashlib
import importlib.util
import json
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest
import yaml

phable = pytest.importorskip("phable")
from phable import NA, Grid, GridCol, Marker, Number, Ref  # noqa: E402

from ingestion.adapters.control.metadata_inventory import PostgresMetadataInventoryPublisher  # noqa: E402
from ingestion.adapters.skyspark import reader_runtime  # noqa: E402
from ingestion.adapters.skyspark.history import read_history  # noqa: E402
from ingestion.adapters.skyspark.live import (  # noqa: E402
    LiveHistorySource, LiveMetadataSource, LiveRulesSource, credentials_from_env,
    project_api_url, require_observed,
)
from ingestion.adapters.skyspark.metadata import read_site_metadata  # noqa: E402
from ingestion.adapters.skyspark.replica import (  # noqa: E402
    ReplicaShape, real_equipment_id, real_ids, real_point_id,
)
from ingestion.adapters.skyspark.rules import read_rules  # noqa: E402
from ingestion.contracts.config import FeedKind  # noqa: E402
from ingestion.contracts.jobs import Job, JobCompletion, RawArtifact, SinkReceipt  # noqa: E402
from ingestion.contracts.resources import (  # noqa: E402
    HistoryReadPolicy, MetadataReadPolicy, RulesReadPolicy, SourceClientPolicy,
)
from ingestion.core.failures import NonRetryableSourceError  # noqa: E402


ROOT = Path(__file__).resolve().parents[2]
READERS = ROOT / "readers" / "skyspark"
DEV = ROOT / "config" / "environments" / "aws-dev"
SITE = "p:demo:r:site-1"
START = datetime(2026, 9, 28, 10, 0, tzinfo=timezone.utc)
END = START + timedelta(minutes=5)
POLICY = SourceClientPolicy(receipt_mode="observed", credentials_env="SKYSPARK_SOURCE_CREDENTIALS")
SHAPE = ReplicaShape(equipment_per_site=4, points_per_equipment=3)
HEX = "a" * 64


def _job(feed: FeedKind, scope_ids: tuple[str, ...] = (), **window) -> Job:
    return Job(
        job_id="b" * 64, run_id="c" * 64, tenant_id="dev-tenant", project_id="dev-scale",
        site_ref="dev-site-0001", feed=feed, scope_ids=scope_ids, config_hash=HEX, **window,
    )


def _grid(rows: list[dict], meta: dict | None = None, cols: list[GridCol] | None = None) -> Grid:
    names = cols or [GridCol(name) for name in sorted({key for row in rows for key in row})]
    return Grid(meta={"ver": "3.0", **(meta or {})}, cols=names, rows=rows)


class FakeSkySpark:
    """Real site: three equipment, five points (four historized)."""

    def __init__(self, *, meta: dict | None = None, rule_rows: list[dict] | None = None):
        self.meta = meta or {}
        self.calls: list[tuple[str, object]] = []
        self.equipment = [
            {"id": Ref(f"p:demo:r:eq-{n}"), "equip": Marker(), "siteRef": Ref(SITE), "dis": f"AHU {n}"}
            for n in (3, 1, 2)
        ]
        self.points = [
            {"id": Ref(f"p:demo:r:pt-{n}"), "point": Marker(), "siteRef": Ref(SITE),
             "equipRef": Ref("p:demo:r:eq-1"), **({"his": Marker()} if n != 5 else {}),
             "unit": "°F"}
            for n in range(1, 6)
        ]
        self.rule_rows = rule_rows

    def read_all(self, filter_expr: str) -> Grid:
        self.calls.append(("read_all", filter_expr))
        if "id==@" in filter_expr:
            ids = set(re.findall(r"\bid==@([^\s)]+)", filter_expr))
            return _grid([row for row in self.equipment if row["id"].val in ids], self.meta)
        rows = self.equipment if filter_expr.startswith("(equip)") else self.points
        return _grid(rows, self.meta)

    def his_read(self, ids, start, end) -> Grid:
        self.calls.append(("his_read", ids))
        cols = [GridCol("ts")] + [GridCol(f"v{i}", {"id": Ref(point)}) for i, point in enumerate(ids)]
        rows = [
            {"ts": START, "v0": Number(71.5, "°F"), **({"v1": True} if len(ids) > 1 else {})},
            {"ts": START + timedelta(minutes=1), "v0": NA()},
            {"ts": END, "v0": Number(99.0)},  # end-inclusive sample belongs to the next window
        ]
        return Grid(meta={"ver": "3.0", "hisStart": start, "hisEnd": end, **self.meta}, cols=cols, rows=rows)

    def eval(self, expression: str) -> Grid:
        self.calls.append(("eval", expression))
        rows = self.rule_rows if self.rule_rows is not None else [{
            "targetRef": Ref("p:demo:r:eq-1"), "ruleRef": Ref("p:demo:r:rule-1"),
            "date": date(2026, 9, 27), "tz": "New_York", "spark": "trigger",
            "dur": Number(5, "min"), "points": [Ref("p:demo:r:pt-1")],
        }]
        return _grid(rows, {"span": "2026-09-27"})


METADATA_READ = MetadataReadPolicy(
    max_pages_per_kind=1, max_rows_per_page=1000, max_entities_per_site=1000,
    max_page_bytes=10_000_000, request_timeout_seconds=30,
)


def test_replica_ids_round_trip_and_reject_other_sites():
    equipment = SHAPE.equipment_id("dev-site-0001", 2, "p:demo:r:eq-2")
    point = SHAPE.point_id("dev-site-0001", 2, 1, "p:demo:r:pt-4")
    assert equipment == "dev-site-0001.e2.p:demo:r:eq-2"
    assert point == "dev-site-0001.e2.p1.p:demo:r:pt-4"
    assert real_equipment_id("dev-site-0001", equipment) == "p:demo:r:eq-2"
    assert real_point_id("dev-site-0001", point) == "p:demo:r:pt-4"
    assert real_ids("dev-site-0001", (point, point.replace(".p1.", ".p2.")), points=True) == {
        "p:demo:r:pt-4": (point, point.replace(".p1.", ".p2.")),
    }
    with pytest.raises(NonRetryableSourceError):
        real_point_id("dev-site-0002", point)
    with pytest.raises(NonRetryableSourceError):
        real_point_id("dev-site-0001", "p:demo:r:pt-4")


def test_replica_metadata_builds_configured_shape_that_inventory_accepts():
    job = _job(FeedKind.METADATA)
    source = FakeSkySpark()
    result = read_site_metadata(
        job, site_uri=SITE, source=LiveMetadataSource(source, job, POLICY, SHAPE),
        policy=METADATA_READ,
    )
    equipment = [row for row in result.rows if row["kind"] == "equipment"]
    points = [row for row in result.rows if row["kind"] == "point"]
    assert len(equipment) == 4 and len(points) == 12
    assert len({row["source_id"] for row in result.rows}) == 16
    assert all(point["historized"] and point["equipment_ref"] in {e["source_id"] for e in equipment}
               for point in points)
    # Four virtual equipment cycle through three real ones; the fourth repeats eq-1.
    assert [e["tags"]["replicaOf"]["val"] for e in equipment] == [
        "p:demo:r:eq-1", "p:demo:r:eq-2", "p:demo:r:eq-3", "p:demo:r:eq-1"]
    # Non-historized pt-5 is never replicated.
    assert {p["tags"]["replicaOf"]["val"] for p in points} == {f"p:demo:r:pt-{n}" for n in range(1, 5)}
    for item in result.rows:
        PostgresMetadataInventoryPublisher._validate_entity(item, job, SITE)
    raw = json.loads(result.raw_response)
    meta = raw["pages"][0]["page"]["meta"]
    assert meta["receipt_kind"] == "observed" and meta["snapshot"] == job.run_id
    assert result.query_id == f"metadata/{job.run_id}/{job.site_ref}"
    assert [call[1] for call in source.calls] == [f"(equip) and siteRef==@{SITE}", f"(point) and siteRef==@{SITE}"]


def test_direct_metadata_keeps_real_ids():
    job = _job(FeedKind.METADATA)
    result = read_site_metadata(
        job, site_uri=SITE, source=LiveMetadataSource(FakeSkySpark(), job, POLICY), policy=METADATA_READ,
    )
    assert sorted(row["source_id"] for row in result.rows if row["kind"] == "equipment") == [
        "p:demo:r:eq-1", "p:demo:r:eq-2", "p:demo:r:eq-3"]
    assert sum(row["kind"] == "point" and row["historized"] for row in result.rows) == 4


def test_replica_history_reads_real_ids_once_and_fans_out_values():
    first = SHAPE.point_id("dev-site-0001", 0, 0, "p:demo:r:pt-1")
    repeat = SHAPE.point_id("dev-site-0001", 1, 1, "p:demo:r:pt-1")
    second = SHAPE.point_id("dev-site-0001", 0, 1, "p:demo:r:pt-2")
    job = _job(FeedKind.HISTORY, (first, repeat, second), window_start=START, window_end=END)
    source = FakeSkySpark()
    result = read_history(
        job, site_uri=SITE, source=LiveHistorySource(source, job, POLICY, SHAPE),
        policy=HistoryReadPolicy(max_ids=500, max_rows=100, max_observations=1000,
                                 max_response_bytes=1_000_000, request_timeout_seconds=30),
    )
    assert source.calls == [("his_read", ("p:demo:r:pt-1", "p:demo:r:pt-2"))]
    values = {(o.point_id, o.observed_at): o for o in result.observations}
    assert values[(first, START)].val_num == values[(repeat, START)].val_num
    assert float(values[(first, START)].val_num) == 71.5
    assert values[(second, START)].val_bool is True
    assert values[(first, START + timedelta(minutes=1))].val_na
    assert all(o.observed_at < END for o in result.observations)
    assert result.completed_ids == job.scope_ids
    assert json.loads(result.raw_response)["meta"]["boundary_rows_dropped"] == 1


def test_history_missing_point_column_fails_closed():
    class Missing(FakeSkySpark):
        def his_read(self, ids, start, end):
            return super().his_read(ids[:1], start, end)

    job = _job(FeedKind.HISTORY, ("p:demo:r:pt-1", "p:demo:r:pt-2"), window_start=START, window_end=END)
    with pytest.raises(NonRetryableSourceError, match="every requested point"):
        LiveHistorySource(Missing(), job, POLICY).read(SITE, job.scope_ids, START, END)


def _rules_job(ids: tuple[str, ...]) -> Job:
    return _job(FeedKind.RULES, ids,
                window_start=datetime(2026, 9, 27, tzinfo=timezone.utc),
                window_end=datetime(2026, 9, 28, tzinfo=timezone.utc))


RULES_READ = RulesReadPolicy(max_ids=200, max_rows=1000, max_response_bytes=1_000_000,
                             request_timeout_seconds=30)


def test_replica_rules_verify_scope_and_map_detections_to_virtual_equipment():
    ids = tuple(SHAPE.equipment_id("dev-site-0001", n, f"p:demo:r:eq-{n % 3 + 1}") for n in range(4))
    job = _rules_job(ids)
    source = FakeSkySpark()
    result = read_rules(
        job, site_uri=SITE, source=LiveRulesSource(source, job, POLICY, SHAPE),
        policy=RULES_READ, source_timezone="UTC", allowed_tz_tags=("New_York",),
    )
    # eq-1 backs virtual e0 and e3, so its one real detection appears on both.
    assert sorted(d.equipment_id for d in result.detections) == [ids[0], ids[3]]
    assert all(d.tags["replicaSourceRef"]["val"] == "p:demo:r:eq-1" for d in result.detections)
    assert len({d.detection_key for d in result.detections}) == 2
    kinds = [call[0] for call in source.calls]
    assert kinds == ["read_all", "eval"]
    assert source.calls[1][1].endswith(".ruleSparks(2026-09-27)")


def test_rules_scope_mismatch_fails_before_rule_query():
    job = _rules_job(("p:demo:r:eq-1", "p:demo:r:eq-9"))
    source = FakeSkySpark()
    with pytest.raises(NonRetryableSourceError, match="scope differs"):
        LiveRulesSource(source, job, POLICY).read(
            SITE, job.scope_ids, date(2026, 9, 27), job.window_start, job.window_end, "UTC")
    assert [call[0] for call in source.calls] == ["read_all"]


def test_truncation_marker_and_provider_mode_fail_closed():
    job = _job(FeedKind.METADATA)
    with pytest.raises(NonRetryableSourceError, match="truncation"):
        LiveMetadataSource(FakeSkySpark(meta={"limitExceeded": Marker()}), job, POLICY).read(
            "equipment", SITE, None)
    with pytest.raises(NonRetryableSourceError, match="provider completeness"):
        require_observed(SourceClientPolicy(credentials_env="X"))


def test_credentials_and_endpoint_validation():
    policy = POLICY
    assert credentials_from_env(
        {"SKYSPARK_SOURCE_CREDENTIALS": '{"username":"u","password":"p"}'}, policy) == ("u", "p")
    with pytest.raises(ValueError):
        credentials_from_env({}, policy)
    with pytest.raises(ValueError):
        credentials_from_env({"SKYSPARK_SOURCE_CREDENTIALS": '{"username":"u"}'}, policy)
    assert project_api_url("http://host:8888/api/demo/") == "http://host:8888/api/demo"
    for bad in ("http://host:8888/api/", "http://host/api/demo?page=2", "ftp://host/api/demo"):
        with pytest.raises(ValueError):
            project_api_url(bad)
    with pytest.raises(ValueError, match="tag names"):
        SourceClientPolicy(credentials_env="X", point_filter="point and readAll(site)")


def _load_script(name: str):
    spec = importlib.util.spec_from_file_location(name, READERS / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_reader_scripts_write_s3_evidence_for_their_feed(monkeypatch):
    written = []

    def fake_evidence(request, *, raw_response, query_id, completed_scope, rows, row_count, environ):
        rows = list(rows)
        assert len(rows) == row_count and raw_response and query_id
        written.append((request.job.feed, completed_scope, row_count))
        job_id = request.job.job_id
        return JobCompletion(
            raw=RawArtifact(job_id=job_id, object_key="raw/k", checksum=HEX, byte_count=1),
            sink=SinkReceipt(job_id=job_id, sink_kind="s3", batch_key="c/k", row_count=row_count, checksum=HEX),
            completed_scope=completed_scope,
        )

    resources = reader_runtime.load_resources(DEV / "resources.yaml")
    replica = {"equipment_per_site": 4, "points_per_equipment": 3}
    cases = {
        "skyspark_metadata": _job(FeedKind.METADATA),
        "skyspark_history": _job(FeedKind.HISTORY, (SHAPE.point_id("dev-site-0001", 0, 0, "p:demo:r:pt-1"),),
                                 window_start=START, window_end=END),
        "skyspark_rules": _rules_job((SHAPE.equipment_id("dev-site-0001", 0, "p:demo:r:eq-1"),)),
    }
    for name, job in cases.items():
        module = _load_script(name)
        monkeypatch.setattr(module, "write_evidence", fake_evidence)
        request = reader_runtime.ReaderRequest(
            job=job, resources=resources, policy=resources.source_client,
            replica=ReplicaShape.from_source(replica),
            source={"site_uri": SITE, "endpoint": "http://host/api/demo",
                    "rules_timezone": "UTC", "rules_tz_tags": ["New_York"], "replica": replica},
        )
        module.read(request, FakeSkySpark())
    assert written == [
        (FeedKind.METADATA, ("dev-site-0001",), 16),
        (FeedKind.HISTORY, cases["skyspark_history"].scope_ids, 2),
        (FeedKind.RULES, cases["skyspark_rules"].scope_ids, 1),
    ]


def test_load_request_rejects_wrong_feed_and_timescale_target(monkeypatch):
    environ = {"RESOURCE_CONFIG_PATH": str(DEV / "resources.yaml")}
    payload = {"job": _job(FeedKind.METADATA).model_dump(mode="json"), "target": "s3",
               "source": {"site_uri": SITE, "endpoint": "http://host/api/demo", "replica": None}}
    assert reader_runtime.load_request(payload, FeedKind.METADATA, environ).replica is None
    with pytest.raises(ValueError, match="only history"):
        reader_runtime.load_request(payload, FeedKind.HISTORY, environ)
    with pytest.raises(ValueError, match="S3 only"):
        reader_runtime.load_request({**payload, "target": "timescale"}, FeedKind.METADATA, environ)


def test_dev_manifest_pins_current_reader_scripts():
    manifest = yaml.safe_load((DEV / "plugins.yaml").read_text(encoding="utf-8"))
    for reader in manifest["readers"]:
        execution = reader["execution"]
        assert hashlib.sha256((READERS / execution["path"]).read_bytes()).hexdigest() == execution["sha256"]
