"""Each feed writes certified S3 or Timescale evidence under its pinned target."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
import os
from pathlib import Path
from threading import Thread
from uuid import uuid4

import boto3
import psycopg
import pytest
import yaml

from ingestion.adapters.aws.s3_evidence import S3EvidenceVerifier, S3ObjectStore
from ingestion.adapters.control.migrations import apply_migrations
from ingestion.adapters.db.timescale import TimescaleEvidenceVerifier
from ingestion.adapters.scripts import RegisteredScriptHandler
from ingestion.config.loader import resolve_config_documents
from ingestion.contracts.config import FeedKind
from ingestion.contracts.jobs import CertifiedInventory, SiteInventory
from ingestion.contracts.resources import load_resources
from ingestion.core.planner import plan_run


ROOT = Path(__file__).resolve().parents[2]
ENDPOINT = os.environ.get("TEST_LOCALSTACK_ENDPOINT_URL")
DSN = os.environ.get("TEST_TARGET_DSN")
pytestmark = pytest.mark.skipif(not ENDPOINT or not DSN,
                                reason="TEST_LOCALSTACK_ENDPOINT_URL and TEST_TARGET_DSN are required")
SPEC = importlib.util.spec_from_file_location("phase7_fixture", ROOT / "local/fixture/server.py")
assert SPEC and SPEC.loader
fixture = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(fixture)


def test_six_feed_target_paths_have_physical_receipts(monkeypatch, tmp_path):
    assert ENDPOINT.startswith(("http://127.0.0.1:", "http://localhost:"))
    apply_migrations(DSN, ROOT / "migrations/target", registry="target_schema_migrations")
    server = fixture.ThreadingHTTPServer(
        ("127.0.0.1", 0), fixture.handler_for(
            fixture.load_fixture(ROOT / "local/fixture/data.json")),
    )
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    session = boto3.Session(aws_access_key_id="test", aws_secret_access_key="test",
                            region_name="us-east-1")
    s3 = session.client("s3", endpoint_url=ENDPOINT)
    bucket = f"phase7-targets-{uuid4().hex}"
    s3.create_bucket(Bucket=bucket)
    try:
        resources_doc = yaml.safe_load((ROOT / "local/resources.yaml").read_text())
        resources_doc["storage"]["raw_bucket"] = bucket
        resources_doc["storage"]["certified_bucket"] = bucket
        resources_doc["buckets"] = [bucket]
        resource_path = tmp_path / "resources.yaml"
        resource_path.write_text(yaml.safe_dump(resources_doc), encoding="utf-8")
        resources = load_resources(resource_path)
        binding = yaml.safe_load((ROOT / "config/bindings/example-local.yaml").read_text())
        binding["endpoint"] = f"http://127.0.0.1:{server.server_port}/api/"
        manifest = yaml.safe_load((ROOT / "local/manifests/contract-phase7.yaml").read_text())
        inventory = CertifiedInventory(
            version="phase7-fixture", tenant_id=binding["tenant_id"],
            project_id=binding["project_id"],
            sites={"site-a": SiteInventory(
                equipment_ids=("equip-a-1", "equip-a-2"),
                point_ids=("point-a-1", "point-a-2"),
                historized_point_ids=("point-a-1", "point-a-2"),
            ), "site-b": SiteInventory(equipment_ids=("equip-b-1",))},
        )
        monkeypatch.setenv("RESOURCE_CONFIG_PATH", str(resource_path))
        monkeypatch.setenv("AWS_ENDPOINT_URL", ENDPOINT)
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
        monkeypatch.setenv("TARGET_DATABASE_URL", DSN)
        raw_verifier = S3EvidenceVerifier(S3ObjectStore(
            region_name="us-east-1", endpoint_url=ENDPOINT, client=s3,
        ), resources.storage)
        actual = {}
        for feed in FeedKind:
            for target in ("s3", "timescale"):
                profile = yaml.safe_load((ROOT / "config/profiles/default.yaml").read_text())
                profile["feeds"][feed.value]["target"] = target
                config = resolve_config_documents(profile, binding, manifest,
                                                  environment="local")
                if feed == FeedKind.METADATA:
                    _, jobs = plan_run(config, feed, datetime(2026, 9, 28, tzinfo=timezone.utc))
                elif feed == FeedKind.RULES:
                    start = datetime(2026, 9, 27, tzinfo=timezone.utc)
                    _, jobs = plan_run(config, feed, start + timedelta(days=1),
                                       inventory=inventory, window_start=start,
                                       window_end=start + timedelta(days=1))
                else:
                    start = datetime(2026, 9, 28, 10, tzinfo=timezone.utc)
                    _, jobs = plan_run(config, feed, start + timedelta(minutes=5),
                                       inventory=inventory, window_start=start,
                                       window_end=start + timedelta(minutes=5))
                job = next(item for item in jobs if item.site_ref == "site-a")
                runner = RegisteredScriptHandler(
                    config, ROOT / "local/scripts", selected_feeds={feed},
                    python_executable=os.environ.get("TEST_SCRIPT_PYTHON"),
                )
                completion = runner.run(job)
                verifier = (raw_verifier if target == "s3" else
                            TimescaleEvidenceVerifier(DSN, raw_verifier, resources.storage))
                verifier.verify(job, completion)
                assert completion.completed_scope == (
                    (job.site_ref,) if feed == FeedKind.METADATA else job.scope_ids
                )
                actual[(feed.value, target)] = completion.sink.row_count
                if target == "timescale":
                    with psycopg.connect(DSN) as conn:
                        assert conn.execute(
                            "SELECT feed, row_count FROM ingestion_target.batch_receipts "
                            "WHERE batch_key = %s", (completion.sink.batch_key,),
                        ).fetchone() == (feed.value, completion.sink.row_count)
        assert actual == {
            ("metadata", "s3"): 4, ("metadata", "timescale"): 4,
            ("rules", "s3"): 1, ("rules", "timescale"): 1,
            ("history", "s3"): 2, ("history", "timescale"): 2,
        }
    finally:
        listing = s3.list_objects_v2(Bucket=bucket)
        if listing.get("Contents"):
            s3.delete_objects(Bucket=bucket, Delete={
                "Objects": [{"Key": item["Key"]} for item in listing["Contents"]],
            })
        s3.delete_bucket(Bucket=bucket)
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
