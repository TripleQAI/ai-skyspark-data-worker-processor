"""Reviewed history subprocess to LocalStack raw evidence and TimescaleDB."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
import json
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
TARGET_DSN = os.environ.get("TEST_TARGET_DSN")
ENDPOINT = os.environ.get("TEST_LOCALSTACK_ENDPOINT_URL")
pytestmark = pytest.mark.skipif(
    not TARGET_DSN or not ENDPOINT,
    reason="TEST_TARGET_DSN and TEST_LOCALSTACK_ENDPOINT_URL are required",
)
SPEC = importlib.util.spec_from_file_location("phase5_fixture", ROOT / "local/fixture/server.py")
assert SPEC and SPEC.loader
fixture = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(fixture)


def test_reviewed_history_script_writes_complete_and_empty_receipts(monkeypatch, tmp_path):
    assert ENDPOINT.startswith(("http://127.0.0.1:", "http://localhost:"))
    apply_migrations(TARGET_DSN, ROOT / "migrations/target", registry="target_schema_migrations")
    data = fixture.load_fixture(ROOT / "local/fixture/data.json")
    server = fixture.ThreadingHTTPServer(("127.0.0.1", 0), fixture.handler_for(data))
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    region = "us-east-1"
    session = boto3.Session(aws_access_key_id="test", aws_secret_access_key="test",
                            region_name=region)
    s3 = session.client("s3", endpoint_url=ENDPOINT)
    bucket = f"phase5-history-{uuid4().hex}"
    s3.create_bucket(Bucket=bucket)
    try:
        resources_doc = yaml.safe_load((ROOT / "local/resources.yaml").read_text())
        resources_doc["storage"]["raw_bucket"] = bucket
        resources_doc["storage"]["certified_bucket"] = bucket
        resources_doc["buckets"] = [bucket]
        resource_path = tmp_path / "resources.yaml"
        resource_path.write_text(yaml.safe_dump(resources_doc), encoding="utf-8")
        resources = load_resources(resource_path)
        profile = yaml.safe_load((ROOT / "config/profiles/default.yaml").read_text())
        binding = yaml.safe_load((ROOT / "config/bindings/example-local.yaml").read_text())
        binding["endpoint"] = f"http://127.0.0.1:{server.server_port}/api/"
        binding["approved_sites"] = {"site-a": "demoSiteA"}
        manifest = yaml.safe_load((ROOT / "local/manifests/contract-history.yaml").read_text())
        config = resolve_config_documents(profile, binding, manifest, environment="local")
        inventory = CertifiedInventory(
            version="phase5-fixture", tenant_id=binding["tenant_id"],
            project_id=binding["project_id"],
            sites={"site-a": SiteInventory(
                point_ids=("point-a-1", "point-a-2"),
                historized_point_ids=("point-a-1", "point-a-2"),
            )},
        )
        monkeypatch.setenv("RESOURCE_CONFIG_PATH", str(resource_path))
        monkeypatch.setenv("AWS_ENDPOINT_URL", ENDPOINT)
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
        monkeypatch.setenv("TARGET_DATABASE_URL", TARGET_DSN)
        runner = RegisteredScriptHandler(
            config, ROOT / "local/scripts", selected_feeds={FeedKind.HISTORY},
            python_executable=os.environ.get("TEST_SCRIPT_PYTHON"),
        )
        verifier = TimescaleEvidenceVerifier(
            TARGET_DSN, S3EvidenceVerifier(
                S3ObjectStore(region_name=region, endpoint_url=ENDPOINT,
                              client=s3), resources.storage,
            ),
        )
        for start, expected in ((datetime(2026, 9, 28, 10, 0, tzinfo=timezone.utc), 2),
                                (datetime(2026, 9, 28, 10, 5, tzinfo=timezone.utc), 0)):
            end = start + timedelta(minutes=5)
            _, jobs = plan_run(config, FeedKind.HISTORY, end, inventory=inventory,
                               window_start=start, window_end=end)
            job = jobs[0]
            completion = runner.run(job)
            verifier.verify(job, completion)
            assert completion.completed_scope == job.scope_ids
            assert completion.sink.row_count == expected
            with psycopg.connect(TARGET_DSN) as conn:
                assert conn.execute(
                    "SELECT row_count FROM ingestion_target.batch_receipts WHERE batch_key = %s",
                    (completion.sink.batch_key,),
                ).fetchone()[0] == expected
        with psycopg.connect(TARGET_DSN) as conn:
            assert conn.execute(
                "SELECT count(*) FROM ingestion_target.history_observations "
                "WHERE point_id IN ('point-a-1', 'point-a-2') AND observed_at >= %s "
                "AND observed_at < %s",
                (datetime(2026, 9, 28, 10, 0, tzinfo=timezone.utc),
                 datetime(2026, 9, 28, 10, 10, tzinfo=timezone.utc)),
            ).fetchone()[0] == 2
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
