"""Reviewed rules subprocess writes complete and empty LocalStack evidence."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
import json
import os
from pathlib import Path
from threading import Thread
from uuid import uuid4

import boto3
import pytest
import yaml

from ingestion.adapters.aws.s3_evidence import S3EvidenceVerifier, S3ObjectStore
from ingestion.adapters.scripts import RegisteredScriptHandler
from ingestion.config.loader import resolve_config_documents
from ingestion.contracts.config import FeedKind
from ingestion.contracts.jobs import CertifiedInventory, SiteInventory
from ingestion.contracts.resources import load_resources
from ingestion.core.planner import plan_run


ROOT = Path(__file__).resolve().parents[2]
ENDPOINT = os.environ.get("TEST_LOCALSTACK_ENDPOINT_URL")
pytestmark = pytest.mark.skipif(not ENDPOINT, reason="TEST_LOCALSTACK_ENDPOINT_URL is required")
SPEC = importlib.util.spec_from_file_location("phase6_fixture", ROOT / "local/fixture/server.py")
assert SPEC and SPEC.loader
fixture = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(fixture)


def test_reviewed_rules_script_writes_populated_and_zero_detection_batches(monkeypatch, tmp_path):
    assert ENDPOINT.startswith(("http://127.0.0.1:", "http://localhost:"))
    data = fixture.load_fixture(ROOT / "local/fixture/data.json")
    server = fixture.ThreadingHTTPServer(("127.0.0.1", 0), fixture.handler_for(data))
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    session = boto3.Session(aws_access_key_id="test", aws_secret_access_key="test",
                            region_name="us-east-1")
    s3 = session.client("s3", endpoint_url=ENDPOINT)
    bucket = f"phase6-rules-{uuid4().hex}"
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
        manifest = yaml.safe_load((ROOT / "local/manifests/contract-rules.yaml").read_text())
        config = resolve_config_documents(profile, binding, manifest, environment="local")
        inventory = CertifiedInventory(
            version="phase6-fixture", tenant_id=binding["tenant_id"],
            project_id=binding["project_id"],
            sites={"site-a": SiteInventory(equipment_ids=("equip-a-1", "equip-a-2")),
                   "site-b": SiteInventory(equipment_ids=("equip-b-1",))},
        )
        monkeypatch.setenv("RESOURCE_CONFIG_PATH", str(resource_path))
        monkeypatch.setenv("AWS_ENDPOINT_URL", ENDPOINT)
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
        runner = RegisteredScriptHandler(
            config, ROOT / "local/scripts", selected_feeds={FeedKind.RULES},
            python_executable=os.environ.get("TEST_SCRIPT_PYTHON"),
        )
        verifier = S3EvidenceVerifier(S3ObjectStore(
            region_name="us-east-1", endpoint_url=ENDPOINT, client=s3,
        ), resources.storage)
        start = datetime(2026, 9, 27, tzinfo=timezone.utc)
        _, jobs = plan_run(config, FeedKind.RULES, start + timedelta(days=1),
                           inventory=inventory, window_start=start,
                           window_end=start + timedelta(days=1))
        counts = {}
        keys = []
        for job in jobs:
            completion = runner.run(job)
            verifier.verify(job, completion)
            assert completion.completed_scope == job.scope_ids
            counts[job.site_ref] = completion.sink.row_count
            keys.extend((completion.raw.object_key, completion.sink.batch_key))
            if job.site_ref == "site-a":
                body = s3.get_object(Bucket=bucket, Key=completion.sink.batch_key)["Body"].read()
                detection = json.loads(body)
                assert detection["equipment_id"] == "equip-a-1"
                assert detection["rule_id"] == "rule-demo-1"
                assert detection["point_ids"] == ["point-a-1"]
        assert counts == {"site-a": 1, "site-b": 0}
        assert len(set(keys)) == 4
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
