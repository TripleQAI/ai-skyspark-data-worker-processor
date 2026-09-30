"""Timescale metadata publication and recovery after a committed rules sink."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
from uuid import uuid4

import boto3
import psycopg
import pytest
import yaml

from ingestion.adapters.aws.inventory import S3VersionedInventoryStore
from ingestion.adapters.aws.s3_evidence import S3EvidenceVerifier, S3ObjectStore, S3RawStore
from ingestion.adapters.control.checkpoints import PostgresCheckpointReconciler
from ingestion.adapters.control.inventory import PostgresInventoryRegistry
from ingestion.adapters.control.metadata_inventory import PostgresMetadataInventoryPublisher
from ingestion.adapters.control.migrations import apply_migrations
from ingestion.adapters.control.postgres import PostgresControlRepository
from ingestion.adapters.db.timescale import TimescaleEvidenceVerifier
from ingestion.adapters.db.timescale_entities import TimescaleMetadataSink, TimescaleRulesSink
from ingestion.adapters.skyspark.metadata import read_site_metadata
from ingestion.adapters.skyspark.rules import read_rules
from ingestion.config.loader import resolve_config_documents
from ingestion.contracts.config import FeedKind
from ingestion.contracts.jobs import CertifiedInventory, JobCompletion, SiteInventory
from ingestion.contracts.resources import load_resources
from ingestion.contracts.replay import ReplayRequest
from ingestion.control_cli import main as control_main
from ingestion.core.planner import plan_run
from ingestion.core.storage_operations import measure_storage


ROOT = Path(__file__).resolve().parents[2]
CONTROL = os.environ.get("TEST_CONTROL_DSN")
TARGET = os.environ.get("TEST_TARGET_DSN")
ENDPOINT = os.environ.get("TEST_LOCALSTACK_ENDPOINT_URL")
pytestmark = pytest.mark.skipif(not CONTROL or not TARGET or not ENDPOINT,
                                reason="disposable control/target DSNs and LocalStack are required")
SPEC = importlib.util.spec_from_file_location("phase7_fixture", ROOT / "local/fixture/server.py")
assert SPEC and SPEC.loader
fixture = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(fixture)


class Pages:
    def __init__(self, data):
        self.data = data

    def read(self, kind, site_uri, page_token):
        assert page_token is None
        return fixture.grid(self.data, site_uri, kind)


class Rules:
    def __init__(self, page):
        self.page = page

    def read(self, *_args):
        return self.page


def _config(feed):
    profile = yaml.safe_load((ROOT / "config/profiles/default.yaml").read_text())
    profile["feeds"][feed.value]["target"] = "timescale"
    binding = yaml.safe_load((ROOT / "config/bindings/example-local.yaml").read_text())
    binding["tenant_id"] = f"phase7-{uuid4().hex}"
    binding["project_id"] = f"phase7-{uuid4().hex}"
    manifest = yaml.safe_load((ROOT / "local/manifests/contract-phase7.yaml").read_text())
    return resolve_config_documents(profile, binding, manifest, environment="local")


def _certify(repo, verifier, job, completion):
    verifier.verify(job, completion)
    token = repo.acquire_job_lease(job_id=job.job_id, worker_id="phase7",
                                   lease_seconds=120)
    assert token is not None
    assert repo.certify_job(job_id=job.job_id, worker_id="phase7",
                            fence_token=token, completion=completion)


def test_timescale_metadata_publishes_inventory_and_rules_recover_after_crash(tmp_path, monkeypatch, capsys):
    assert ENDPOINT.startswith(("http://127.0.0.1:", "http://localhost:"))
    apply_migrations(CONTROL, ROOT / "migrations/control")
    apply_migrations(TARGET, ROOT / "migrations/target", registry="target_schema_migrations")
    data = fixture.load_fixture(ROOT / "local/fixture/data.json")
    session = boto3.Session(aws_access_key_id="test", aws_secret_access_key="test",
                            region_name="us-east-1")
    s3 = session.client("s3", endpoint_url=ENDPOINT)
    raw_bucket = f"phase7-raw-{uuid4().hex}"
    certified_bucket = f"phase7-certified-{uuid4().hex}"
    s3.create_bucket(Bucket=raw_bucket)
    s3.create_bucket(Bucket=certified_bucket)
    s3.put_bucket_versioning(Bucket=certified_bucket,
                             VersioningConfiguration={"Status": "Enabled"})
    try:
        base = load_resources(ROOT / "local/resources.yaml")
        resources = base.model_copy(update={
            "storage": base.storage.model_copy(update={
                "raw_bucket": raw_bucket, "certified_bucket": certified_bucket,
            }), "buckets": (raw_bucket, certified_bucket),
        })
        objects = S3ObjectStore(region_name="us-east-1", client=s3)
        verifier = TimescaleEvidenceVerifier(
            TARGET, S3EvidenceVerifier(objects, resources.storage), resources.storage,
        )
        repo = PostgresControlRepository(CONTROL)

        metadata_config = _config(FeedKind.METADATA)
        metadata_data = json.loads(json.dumps(data))
        metadata_data["project_id"] = metadata_config.binding.project_id
        metadata_run, metadata_jobs = plan_run(
            metadata_config, FeedKind.METADATA,
            datetime(2026, 9, 28, 3, tzinfo=timezone.utc),
        )
        repo.save_plan(metadata_config, metadata_run, metadata_jobs,
                       queue_class="metadata_sweep")
        for job in metadata_jobs:
            result = read_site_metadata(
                job, site_uri=metadata_config.binding.approved_sites[job.site_ref],
                source=Pages(metadata_data), policy=resources.metadata_read,
            )
            completion = JobCompletion(
                raw=S3RawStore(objects, resources.storage).put(
                    job, (result.raw_response,), query_id=result.query_id,
                    completed_scope=(job.site_ref,), row_count=len(result.rows),
                ),
                sink=TimescaleMetadataSink(TARGET, resources.storage).put(job, result.rows),
                completed_scope=(job.site_ref,),
            )
            _certify(repo, verifier, job, completion)
        publisher = PostgresMetadataInventoryPublisher(
            CONTROL, objects=objects,
            inventory_store=S3VersionedInventoryStore(
                region_name="us-east-1", policy=resources.inventory_artifacts,
                client=s3,
            ), resources=resources, target_dsn=TARGET,
        )
        record = publisher.publish(run_id=metadata_run.run_id, config=metadata_config)
        inventory = publisher._inventory.load(record, config=metadata_config)
        assert sum(len(site.equipment_ids) for site in inventory.sites.values()) == 3
        assert sum(len(site.point_ids) for site in inventory.sites.values()) == 3
        assert PostgresInventoryRegistry(CONTROL).load_exact(
            tenant_id=metadata_config.binding.tenant_id,
            project_id=metadata_config.binding.project_id,
            version=record.version,
        ) == record
        now = datetime.now(timezone.utc).replace(microsecond=0)
        replay_request = ReplayRequest(
            request_id=uuid4(), tenant_id=metadata_config.binding.tenant_id,
            project_id=metadata_config.binding.project_id,
            config_hash=metadata_config.config_hash, feed="history",
            inventory_version=record.version,
            window_start=now - timedelta(minutes=15),
            window_end=now - timedelta(minutes=10), requested_at=now,
            requested_by="local-operator", approval_ref="reviewed-change-456",
            reason="recover late observations",
        )
        with psycopg.connect(CONTROL) as conn:
            conn.execute(
                """INSERT INTO ingestion.replay_approval_grants
                   (approval_ref, tenant_id, project_id, config_hash, feed,
                    inventory_version, window_start, window_end, max_jobs,
                    approved_by, expires_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (replay_request.approval_ref, replay_request.tenant_id,
                 replay_request.project_id, replay_request.config_hash,
                 replay_request.feed.value, replay_request.inventory_version,
                 replay_request.window_start, replay_request.window_end, 2,
                 "reviewer", now + timedelta(hours=1)),
            )
        request_path = tmp_path / "replay-request.json"
        request_path.write_text(replay_request.model_dump_json(), encoding="utf-8")
        resources_path = tmp_path / "replay-resources.yaml"
        resources_path.write_text(yaml.safe_dump(resources.model_dump(mode="json")),
                                  encoding="utf-8")
        monkeypatch.setenv("CONTROL_DATABASE_URL", CONTROL)
        monkeypatch.setenv("AWS_ENDPOINT_URL", ENDPOINT)
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
        assert control_main([
            "request-replay", "--environment", "local", "--resources",
            str(resources_path), "--request-file", str(request_path),
        ]) == 0
        replay_output = json.loads(capsys.readouterr().out)
        assert replay_output["queue_class"] == resources.backfill_queue
        assert replay_output["expected_jobs"] == 2
        with psycopg.connect(TARGET) as conn:
            assert conn.execute(
                "SELECT count(*) FROM ingestion_target.metadata_entity_revisions "
                "WHERE tenant_id = %s", (metadata_config.binding.tenant_id,),
            ).fetchone()[0] == 6

        rules_config = _config(FeedKind.RULES)
        rules_data = json.loads(json.dumps(data))
        rules_data["project_id"] = rules_config.binding.project_id
        for site in rules_data["sites"].values():
            for row in site["rules"]:
                row["fsk"] = rules_config.binding.project_id
        rules_inventory = CertifiedInventory(
            version="phase7-rules", tenant_id=rules_config.binding.tenant_id,
            project_id=rules_config.binding.project_id,
            sites={"site-a": SiteInventory(equipment_ids=("equip-a-1", "equip-a-2")),
                   "site-b": SiteInventory(equipment_ids=("equip-b-1",))},
        )
        start = datetime(2026, 9, 27, tzinfo=timezone.utc)
        rules_run, rules_jobs = plan_run(
            rules_config, FeedKind.RULES, start + timedelta(days=1),
            inventory=rules_inventory, window_start=start,
            window_end=start + timedelta(days=1),
        )
        repo.save_plan(rules_config, rules_run, rules_jobs,
                       queue_class="rules_nightly")
        cursor = PostgresCheckpointReconciler(CONTROL)
        for site in ("site-a", "site-b"):
            cursor.seed_checkpoint(rules_config, FeedKind.RULES, site, start)
        for job in rules_jobs:
            site_uri = rules_config.binding.approved_sites[job.site_ref]
            page = fixture.grid(rules_data, site_uri, "rules", {
                "equipment_ids": list(job.scope_ids), "day": "2026-09-27",
                "timezone": "UTC", "window_start": start.isoformat(),
                "window_end": (start + timedelta(days=1)).isoformat(),
            })
            result = read_rules(
                job, site_uri=site_uri, source=Rules(page),
                policy=resources.rules_read, source_timezone="UTC",
                allowed_tz_tags=("UTC",),
            )
            raw = S3RawStore(objects, resources.storage).put(
                job, (result.raw_response,), query_id=result.query_id,
                completed_scope=result.completed_ids,
                row_count=len(result.detections),
            )
            if job.site_ref == "site-a":
                site_a_job, site_a_page = job, page
                # A separate process commits the target and exits without
                # touching control state, as a worker killed at this boundary.
                input_path = tmp_path / "crash-input.json"
                marker = tmp_path / "sink-committed.json"
                input_path.write_text(json.dumps({
                    "job": job.model_dump(mode="json"),
                    "rows": [item.model_dump(mode="json") for item in result.detections],
                    "resources": resources.storage.model_dump(mode="json"),
                    "marker": str(marker),
                }), encoding="utf-8")
                crash_code = (
                    "import json,os,sys; from pathlib import Path; "
                    "from ingestion.contracts.jobs import Job; "
                    "from ingestion.contracts.rules import RuleDetection; "
                    "from ingestion.contracts.resources import StoragePolicy; "
                    "from ingestion.adapters.db.timescale_entities import TimescaleRulesSink; "
                    "p=json.loads(Path(sys.argv[1]).read_text()); "
                    "r=TimescaleRulesSink(os.environ['TARGET_DATABASE_URL'],"
                    "StoragePolicy.model_validate(p['resources'])).put("
                    "Job.model_validate(p['job']),[RuleDetection.model_validate(x) for x in p['rows']]); "
                    "Path(p['marker']).write_text(r.model_dump_json()); os._exit(77)"
                )
                env=os.environ.copy()
                env["PYTHONPATH"] = str(ROOT / "src")
                env["TARGET_DATABASE_URL"] = TARGET
                process = subprocess.run(
                    [sys.executable, "-c", crash_code, str(input_path)],
                    env=env, capture_output=True, check=False,
                )
                assert process.returncode == 77 and marker.exists()
                first = json.loads(marker.read_text())
                with psycopg.connect(CONTROL) as conn:
                    assert conn.execute(
                        "SELECT status FROM ingestion.jobs WHERE job_id = %s",
                        (job.job_id,),
                    ).fetchone() == ("planned",)
                sink = TimescaleRulesSink(TARGET, resources.storage).put(
                    job, result.detections,
                )
                assert sink.model_dump(mode="json") == first
                site_a_sink = sink
            else:
                sink = TimescaleRulesSink(TARGET, resources.storage).put(
                    job, result.detections,
                )
            _certify(repo, verifier, job, JobCompletion(
                raw=raw, sink=sink, completed_scope=result.completed_ids,
            ))
        corrected_page = json.loads(json.dumps(site_a_page))
        corrected_page["rows"][0]["severity"] = "critical"
        corrected = read_rules(
            site_a_job, site_uri="demoSiteA", source=Rules(corrected_page),
            policy=resources.rules_read, source_timezone="UTC",
            allowed_tz_tags=("UTC",),
        )
        corrected_sink = TimescaleRulesSink(TARGET, resources.storage).put(
            site_a_job, corrected.detections,
        )
        assert corrected_sink.batch_key != site_a_sink.batch_key
        assert TimescaleRulesSink(TARGET, resources.storage).put(
            site_a_job, corrected.detections,
        ) == corrected_sink
        assert cursor.reconcile_site(rules_run.run_id, "site-a").state == "already_advanced"
        assert cursor.reconcile_site(rules_run.run_id, "site-b").state == "already_advanced"
        with psycopg.connect(CONTROL) as conn:
            assert conn.execute(
                "SELECT count(*) FROM ingestion.publication_outbox WHERE job_id = ANY(%s)",
                ([job.job_id for job in rules_jobs],),
            ).fetchone()[0] == 2
        with psycopg.connect(TARGET) as conn:
            assert conn.execute(
                "SELECT count(*) FROM ingestion_target.rule_detection_revisions "
                "WHERE tenant_id = %s", (rules_config.binding.tenant_id,),
            ).fetchone()[0] == 2
        measured = measure_storage(resources, s3_client=s3,
                                   control_dsn=CONTROL, target_dsn=TARGET)
        assert measured["s3"]["raw"]["objects"] >= 4
        assert measured["s3"]["inventory"]["objects"] == 1
        assert measured["history_hypertable_bytes"] > 0
    finally:
        for bucket in (raw_bucket, certified_bucket):
            versions = s3.list_object_versions(Bucket=bucket)
            keys = [{"Key": item["Key"], "VersionId": item["VersionId"]}
                    for kind in ("Versions", "DeleteMarkers")
                    for item in versions.get(kind, [])]
            if keys:
                s3.delete_objects(Bucket=bucket, Delete={"Objects": keys})
            s3.delete_bucket(Bucket=bucket)
