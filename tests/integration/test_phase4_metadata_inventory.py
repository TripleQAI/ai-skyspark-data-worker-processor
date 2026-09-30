"""Opt-in LocalStack/PostgreSQL gate for complete metadata publication."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import sys
import threading
from http.server import ThreadingHTTPServer
from uuid import uuid4

import boto3
import psycopg
import pytest
import yaml

from ingestion.adapters.aws.inventory import S3VersionedInventoryStore
from ingestion.adapters.aws.s3_evidence import S3JsonlSink, S3ObjectStore, S3RawStore
from ingestion.adapters.aws.s3_evidence import S3EvidenceVerifier
from ingestion.adapters.control.inventory import PostgresInventoryRegistry
from ingestion.adapters.control.metadata_inventory import (
    IncompleteMetadataRun, PostgresMetadataInventoryPublisher,
)
from ingestion.adapters.control.migrations import apply_migrations
from ingestion.adapters.control.postgres import PostgresControlRepository
from ingestion.adapters.skyspark.metadata import read_site_metadata
from ingestion.config.loader import resolve_config_documents
from ingestion.contracts.config import FeedKind
from ingestion.contracts.jobs import JobCompletion
from ingestion.contracts.resources import load_resources
from ingestion.core.planner import plan_run


ROOT = Path(__file__).resolve().parents[2]
DSN = os.environ.get("TEST_CONTROL_DSN")
ENDPOINT = os.environ.get("TEST_LOCALSTACK_ENDPOINT_URL")
pytestmark = pytest.mark.skipif(
    not DSN or not ENDPOINT,
    reason="TEST_CONTROL_DSN and TEST_LOCALSTACK_ENDPOINT_URL are required",
)


class FixturePages:
    def __init__(self, data, snapshot):
        self._data = data
        self._snapshot = snapshot

    def read(self, kind, site_uri, page_token):
        assert page_token is None
        rows = self._data["sites"][site_uri][kind]
        return {
            "meta": {
                "project": self._data["project_id"], "site": site_uri,
                "snapshot": self._snapshot, "page_index": 0,
                "next_page": None, "complete": True,
                "total_rows": len(rows), "returned_rows": len(rows),
            },
            "cols": [], "rows": rows,
        }


def _config(tenant_id):
    import yaml

    profile = yaml.safe_load((ROOT / "config/profiles/default.yaml").read_text())
    binding = yaml.safe_load((ROOT / "config/bindings/example-local.yaml").read_text())
    manifest = yaml.safe_load((ROOT / "local/manifests/contract-metadata.yaml").read_text())
    binding["tenant_id"] = tenant_id
    binding["excluded_history_point_ids_by_site"] = {"site-a": ["point-a-2"]}
    return resolve_config_documents(profile, binding, manifest, environment="local")


def _certify_run(config, resources, data, scheduled_at, snapshot, repository, objects):
    run, jobs = plan_run(config, FeedKind.METADATA, scheduled_at)
    repository.save_plan(config, run, jobs, queue_class="metadata_sweep")
    for job in jobs:
        result = read_site_metadata(
            job, site_uri=config.binding.approved_sites[job.site_ref],
            source=FixturePages(data, snapshot), policy=resources.metadata_read,
        )
        completion = JobCompletion(
            raw=S3RawStore(objects, resources.storage).put(
                job, (result.raw_response,), query_id=result.query_id,
                completed_scope=(job.site_ref,), row_count=len(result.rows),
            ),
            sink=S3JsonlSink(objects, resources.storage).put(job, result.rows),
            completed_scope=(job.site_ref,),
        )
        fence = repository.acquire_job_lease(
            job_id=job.job_id, worker_id="phase4-test", lease_seconds=60,
        )
        assert fence is not None
        assert repository.certify_job(
            job_id=job.job_id, worker_id="phase4-test",
            fence_token=fence, completion=completion,
        )
    return run


def test_complete_snapshot_pins_scope_and_partial_or_shrunken_run_cannot_publish(monkeypatch, tmp_path):
    if not ENDPOINT.startswith(("http://localhost:", "http://127.0.0.1:")):
        raise ValueError("Phase 4 integration requires a host-local LocalStack endpoint")
    apply_migrations(DSN, ROOT / "migrations/control")
    resources = load_resources(ROOT / "local/resources.yaml")
    config = _config(f"phase4-{uuid4().hex}")
    fixture = json.loads((ROOT / "local/fixture/data.json").read_text())
    session = boto3.Session(
        aws_access_key_id="test", aws_secret_access_key="test", region_name=resources.region,
    )
    s3 = session.client("s3", endpoint_url=ENDPOINT)
    raw_bucket = f"phase4-raw-{uuid4().hex}"
    certified_bucket = f"phase4-certified-{uuid4().hex}"
    s3.create_bucket(Bucket=raw_bucket)
    s3.create_bucket(Bucket=certified_bucket)
    s3.put_bucket_versioning(
        Bucket=certified_bucket, VersioningConfiguration={"Status": "Enabled"},
    )
    storage = resources.storage.model_copy(update={
        "raw_bucket": raw_bucket, "certified_bucket": certified_bucket,
    })
    resources = resources.model_copy(update={
        "storage": storage, "buckets": (raw_bucket, certified_bucket),
    })
    objects = S3ObjectStore(region_name=resources.region, client=s3)
    inventory_store = S3VersionedInventoryStore(
        region_name=resources.region, policy=resources.inventory_artifacts, client=s3,
    )
    publisher = PostgresMetadataInventoryPublisher(
        DSN, objects=objects, inventory_store=inventory_store, resources=resources,
    )
    repository = PostgresControlRepository(DSN)
    start = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(hours=1)
    try:
        first = _certify_run(
            config, resources, fixture, start, "snapshot-one", repository, objects,
        )
        _, first_jobs = plan_run(config, FeedKind.METADATA, start)
        record = publisher.publish(run_id=first.run_id, config=config)
        assert publisher.publish(run_id=first.run_id, config=config) == record
        inventory = inventory_store.load(record, config=config)
        assert inventory_store.put(
            inventory, source_run_id=first.run_id, bucket=certified_bucket,
        ).object_ref == record.object_ref
        assert sum(len(site.equipment_ids) for site in inventory.sites.values()) == 3
        assert sum(len(site.point_ids) for site in inventory.sites.values()) == 3
        assert sum(len(site.historized_point_ids) for site in inventory.sites.values()) == 2
        assert inventory.sites["site-a"].historized_point_ids == ("point-a-1",)
        pin = PostgresInventoryRegistry(DSN).pin_for_due(
            tenant_id=config.binding.tenant_id, project_id=config.binding.project_id,
            feed=FeedKind.HISTORY, scheduled_at=datetime.now(timezone.utc) + timedelta(minutes=10),
            config_hash=config.config_hash, max_age_hours=336,
        )
        assert pin.version == record.version
        _, history_jobs = plan_run(
            config, FeedKind.HISTORY, datetime.now(timezone.utc), inventory=inventory,
            window_start=datetime(2026, 9, 28, 10, tzinfo=timezone.utc),
            window_end=datetime(2026, 9, 28, 10, 5, tzinfo=timezone.utc),
        )
        assert {job.site_ref: job.scope_ids for job in history_jobs} == {
            "site-a": ("point-a-1",), "site-b": ("point-b-1",),
        }

        # Exercise the reviewed local script through the HTTP contract fixture.
        fixture_spec = importlib.util.spec_from_file_location(
            "phase4_fixture_server", ROOT / "local/fixture/server.py",
        )
        assert fixture_spec and fixture_spec.loader
        fixture_module = importlib.util.module_from_spec(fixture_spec)
        fixture_spec.loader.exec_module(fixture_module)
        server = ThreadingHTTPServer(
            ("127.0.0.1", 0), fixture_module.handler_for(fixture),
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            script_path = ROOT / "local/scripts/contract_metadata.py"
            execution = next(
                reader.execution for reader in config.manifest.readers
                if reader.feed == FeedKind.METADATA
            )
            assert execution.sha256 == hashlib.sha256(script_path.read_bytes()).hexdigest()
            script_spec = importlib.util.spec_from_file_location("phase4_contract_script", script_path)
            assert script_spec and script_spec.loader
            script_module = importlib.util.module_from_spec(script_spec)
            script_spec.loader.exec_module(script_module)
            monkeypatch.setenv("AWS_ENDPOINT_URL", ENDPOINT)
            monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
            monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
            resources_path = tmp_path / "resources.yaml"
            resources_path.write_text(yaml.safe_dump(resources.model_dump(mode="json")))
            monkeypatch.setenv("RESOURCE_CONFIG_PATH", str(resources_path))
            output = io.StringIO()
            monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps({
                "job": first_jobs[0].model_dump(mode="json"), "target": "s3",
                "source": {
                    "endpoint": f"http://127.0.0.1:{server.server_port}/api/",
                    "site_uri": config.binding.approved_sites[first_jobs[0].site_ref],
                },
            })))
            monkeypatch.setattr(sys, "stdout", output)
            script_module.main()
            script_completion = JobCompletion.model_validate_json(output.getvalue())
            S3EvidenceVerifier(
                S3ObjectStore(region_name=resources.region, client=s3),
                resources.storage,
            ).verify(first_jobs[0], script_completion)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

        partial, jobs = plan_run(config, FeedKind.METADATA, start + timedelta(minutes=1))
        repository.save_plan(config, partial, jobs, queue_class="metadata_sweep")
        with pytest.raises(IncompleteMetadataRun):
            publisher.publish(run_id=partial.run_id, config=config)

        reduced = json.loads(json.dumps(fixture))
        reduced["sites"]["demoSiteA"]["points"] = reduced["sites"]["demoSiteA"]["points"][:1]
        second = _certify_run(
            config, resources, reduced, start + timedelta(minutes=2),
            "snapshot-two", repository, objects,
        )
        with pytest.raises(IncompleteMetadataRun, match="shrink"):
            publisher.publish(run_id=second.run_id, config=config)
        with psycopg.connect(DSN) as conn:
            assert conn.execute(
                "SELECT count(*) FROM ingestion.inventory_versions WHERE tenant_id = %s",
                (config.binding.tenant_id,),
            ).fetchone()[0] == 1
            assert conn.execute(
                "SELECT count(*) FROM ingestion.inventory_entity_changes WHERE source_run_id = %s",
                (first.run_id,),
            ).fetchone()[0] == 6

        expanded = json.loads(json.dumps(fixture))
        expanded["sites"]["demoSiteA"]["equipment"][0]["navName"] = "Renamed synthetic AHU"
        expanded["sites"]["demoSiteA"]["points"].append({
            "id": "point-a-3", "siteRef": "demoSiteA", "equipRef": "equip-a-1",
            "his": False, "dis": "Synthetic new nonhistorized point",
        })
        third = _certify_run(
            config, resources, expanded, start + timedelta(minutes=3),
            "snapshot-three", repository, objects,
        )
        next_record = publisher.publish(run_id=third.run_id, config=config)
        assert next_record.version != record.version
        next_inventory = inventory_store.load(next_record, config=config)
        assert len(next_inventory.sites["site-a"].point_ids) == 3
        assert next_inventory.sites["site-a"].historized_point_ids == ("point-a-1",)
        with psycopg.connect(DSN) as conn:
            assert conn.execute(
                "SELECT change_kind, count(*) FROM ingestion.inventory_entity_changes "
                "WHERE source_run_id = %s GROUP BY change_kind ORDER BY change_kind",
                (third.run_id,),
            ).fetchall() == [("added", 1), ("changed", 1)]
    finally:
        for bucket in (raw_bucket, certified_bucket):
            for page in s3.get_paginator("list_object_versions").paginate(Bucket=bucket):
                for item in [*page.get("Versions", []), *page.get("DeleteMarkers", [])]:
                    s3.delete_object(Bucket=bucket, Key=item["Key"], VersionId=item["VersionId"])
            s3.delete_bucket(Bucket=bucket)
