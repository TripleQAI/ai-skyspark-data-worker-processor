"""Synthetic registry rows exercise due-time pinning, not metadata certification."""

import hashlib
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from psycopg.types.json import Jsonb

from ingestion.adapters.control.inventory import PostgresInventoryRegistry
from ingestion.adapters.control.migrations import apply_migrations
from ingestion.adapters.control.postgres import PostgresControlRepository
from ingestion.config.loader import resolve_config, resolve_config_documents
from ingestion.contracts.config import FeedKind
from ingestion.contracts.jobs import CertifiedInventory
from ingestion.control_cli import main as control_main
from ingestion.core.planner import plan_run


ROOT = Path(__file__).resolve().parents[2]
DSN = os.environ.get("TEST_CONTROL_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="TEST_CONTROL_DSN is unset")


def _config():
    base = resolve_config(
        ROOT / "config/profiles/default.yaml",
        ROOT / "config/bindings/example-local.yaml",
        ROOT / "config/manifests/plugins.yaml",
        environment="local",
    )
    binding = base.binding.model_dump(mode="json")
    binding["project_id"] = f"demo-{uuid4().hex}"
    return resolve_config_documents(
        base.profile.model_dump(mode="json"), binding,
        base.manifest.model_dump(mode="json"), environment="local",
    )


def _metadata_source(config, scheduled_at, completed_at):
    run, jobs = plan_run(config, FeedKind.METADATA, scheduled_at)
    PostgresControlRepository(DSN).save_plan(
        config, run, jobs, queue_class="metadata_sweep"
    )
    with psycopg.connect(DSN) as conn:
        # The production metadata certifier is not implemented. This fixture
        # supplies synthetic certification rows only to test registry selection.
        for job in jobs:
            artifact_id = hashlib.sha256(f"raw:{job.job_id}".encode()).hexdigest()
            batch_key = hashlib.sha256(f"sink:{job.job_id}".encode()).hexdigest()
            conn.execute(
                """
                INSERT INTO ingestion.raw_artifacts
                    (artifact_id, job_id, object_key, checksum, byte_count)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (artifact_id, job.job_id, f"synthetic/{artifact_id}", "a" * 64, 1),
            )
            conn.execute(
                """
                INSERT INTO ingestion.sink_receipts
                    (batch_key, job_id, sink_kind, row_count, checksum)
                VALUES (%s, %s, 's3', 1, %s)
                """,
                (batch_key, job.job_id, "b" * 64),
            )
            conn.execute(
                """
                INSERT INTO ingestion.certifications
                    (job_id, artifact_id, batch_key, completed_scope, certified_at)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (job.job_id, artifact_id, batch_key, Jsonb([job.site_ref]), completed_at),
            )
            conn.execute(
                "UPDATE ingestion.jobs SET status = 'certified' WHERE job_id = %s",
                (job.job_id,),
            )
            conn.execute(
                """
                INSERT INTO ingestion.site_run_completions
                    (run_id, site_ref, job_count, completed_at)
                VALUES (%s, %s, 1, %s)
                """,
                (run.run_id, job.site_ref, completed_at),
            )
        conn.execute(
            "UPDATE ingestion.runs SET status = 'certified' WHERE run_id = %s",
            (run.run_id,),
        )
    return run.run_id


def _insert_snapshot(config, source_run_id, certified_at, version):
    raw = (ROOT / "local/fixtures/inventory.json").read_bytes()
    with psycopg.connect(DSN) as conn:
        conn.execute(
            """
            INSERT INTO ingestion.inventory_versions
                (tenant_id, project_id, inventory_version, source_run_id,
                 object_ref, object_sha256, byte_count, certified_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                config.binding.tenant_id, config.binding.project_id, version,
                source_run_id, f"s3://inventory-bucket/demo/{version}.json?versionId=v1",
                hashlib.sha256(raw).hexdigest(), len(raw), certified_at,
            ),
        )


def test_registry_fails_closed_and_pins_one_inventory_per_due_time():
    apply_migrations(DSN, ROOT / "migrations/control")
    config = _config()
    due = datetime.now(timezone.utc).replace(second=0, microsecond=0) + timedelta(minutes=10)
    registry = PostgresInventoryRegistry(DSN)
    identity = {
        "tenant_id": config.binding.tenant_id,
        "project_id": config.binding.project_id,
        "feed": FeedKind.HISTORY,
        "scheduled_at": due,
        "config_hash": config.config_hash,
        "max_age_hours": 336,
    }
    with pytest.raises(ValueError, match="no eligible"):
        registry.pin_for_due(
            **{**identity, "tenant_id": f"missing-{uuid4().hex}"}
        )
    incomplete, incomplete_jobs = plan_run(
        config, FeedKind.METADATA, due - timedelta(hours=3)
    )
    PostgresControlRepository(DSN).save_plan(
        config, incomplete, incomplete_jobs, queue_class="metadata_sweep"
    )
    _insert_snapshot(
        config, incomplete.run_id, due - timedelta(hours=2),
        f"inventory-{uuid4().hex}",
    )
    with psycopg.connect(DSN) as conn:
        conn.execute(
            "UPDATE ingestion.runs SET status = 'certified' WHERE run_id = %s",
            (incomplete.run_id,),
        )
    with pytest.raises(ValueError, match="no eligible"):
        registry.pin_for_due(**identity)
    older_run = _metadata_source(
        config, due - timedelta(hours=2), due - timedelta(minutes=90)
    )
    older = f"inventory-{uuid4().hex}"
    _insert_snapshot(config, older_run, due - timedelta(hours=1), older)
    assert registry.pin_for_due(**identity).version == older

    newer_run = _metadata_source(
        config, due - timedelta(minutes=40), due - timedelta(minutes=30)
    )
    newer = f"inventory-{uuid4().hex}"
    _insert_snapshot(config, newer_run, due - timedelta(minutes=20), newer)
    assert registry.pin_for_due(**identity).version == older
    assert registry.pin_for_due(
        **{**identity, "scheduled_at": due + timedelta(minutes=5)}
    ).version == newer
    with pytest.raises(ValueError, match="no eligible"):
        registry.pin_for_due(
            **{**identity, "scheduled_at": due + timedelta(days=15)}
        )


def test_persist_scheduled_metadata_uses_pinned_config_and_no_inventory(monkeypatch, capsys):
    apply_migrations(DSN, ROOT / "migrations/control")
    config = _config()
    ref = "s3://reviewed-configs/demo/config.json?versionId=v1"
    trigger = {
        "schema_version": 1,
        "tenant_id": config.binding.tenant_id,
        "project_id": config.binding.project_id,
        "feed": "metadata",
        "profile_id": config.profile.profile_id,
        "config_hash": config.config_hash,
        "config_ref": ref,
        "scheduled_at": "2026-09-27T03:00:00Z",
    }

    class FakeConfigStore:
        def __init__(self, **kwargs):
            pass

        def load(self, actual_ref, *, environment):
            assert (actual_ref, environment) == (ref, "local")
            return config

    monkeypatch.setattr("ingestion.core.scheduled_service.S3VersionedConfigStore", FakeConfigStore)
    monkeypatch.setenv("CONTROL_DATABASE_URL", DSN)
    monkeypatch.setenv("SCHEDULED_TRIGGER_JSON", json.dumps(trigger))
    args = [
        "persist-scheduled", "--environment", "local",
        "--resources", str(ROOT / "local/resources.yaml"),
    ]
    assert control_main(args) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["expected_jobs"] == len(config.binding.approved_sites)
    assert output["queue_class"] == "metadata_sweep"
    assert output["inventory_version"] is None
    assert control_main(args) == 0
    again = json.loads(capsys.readouterr().out)
    assert again["run_id"] == output["run_id"]
    assert again["new_jobs"] == 0


def test_persist_scheduled_history_uses_registry_snapshot(monkeypatch, capsys):
    apply_migrations(DSN, ROOT / "migrations/control")
    config = _config()
    due = datetime.now(timezone.utc).replace(second=0, microsecond=0) + timedelta(minutes=15)
    due += timedelta(minutes=(-due.minute) % 5)
    source_run = _metadata_source(
        config, due - timedelta(hours=2), due - timedelta(minutes=40)
    )
    version = f"inventory-{uuid4().hex}"
    _insert_snapshot(config, source_run, due - timedelta(minutes=30), version)
    ref = "s3://reviewed-configs/demo/config.json?versionId=v1"
    trigger = {
        "schema_version": 1,
        "tenant_id": config.binding.tenant_id,
        "project_id": config.binding.project_id,
        "feed": "history",
        "profile_id": config.profile.profile_id,
        "config_hash": config.config_hash,
        "config_ref": ref,
        "scheduled_at": due.isoformat(),
    }

    class FakeConfigStore:
        def __init__(self, **kwargs):
            pass

        def load(self, actual_ref, *, environment):
            assert (actual_ref, environment) == (ref, "local")
            return config

    class FakeInventoryStore:
        def __init__(self, **kwargs):
            pass

        def load(self, record, *, config):
            assert record.version == version
            fixture = CertifiedInventory.model_validate_json(
                (ROOT / "local/fixtures/inventory.json").read_text(encoding="utf-8")
            )
            return fixture.model_copy(update={
                "version": record.version,
                "project_id": config.binding.project_id,
            })

    monkeypatch.setattr("ingestion.core.scheduled_service.S3VersionedConfigStore", FakeConfigStore)
    monkeypatch.setattr("ingestion.core.scheduled_service.S3VersionedInventoryStore", FakeInventoryStore)
    monkeypatch.setenv("CONTROL_DATABASE_URL", DSN)
    monkeypatch.setenv("SCHEDULED_TRIGGER_JSON", json.dumps(trigger))
    args = [
        "persist-scheduled", "--environment", "local",
        "--resources", str(ROOT / "local/resources.yaml"),
    ]
    assert control_main(args) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["queue_class"] == "history_live"
    assert output["inventory_version"] == version
    assert output["expected_jobs"] >= 2
    assert control_main(args) == 0
    assert json.loads(capsys.readouterr().out)["new_jobs"] == 0
