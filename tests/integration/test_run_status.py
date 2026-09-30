"""Report certified coverage from durable job evidence and site completions."""

import hashlib
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from psycopg.types.json import Jsonb

from ingestion.adapters.control.checkpoints import PostgresCheckpointReconciler
from ingestion.adapters.control.migrations import apply_migrations
from ingestion.adapters.control.postgres import PostgresControlRepository
from ingestion.adapters.control.run_status import PostgresRunStatusReader
from ingestion.config.loader import resolve_config, resolve_config_documents
from ingestion.contracts.config import FeedKind
from ingestion.control_cli import main as control_main
from ingestion.core.planner import plan_run


ROOT = Path(__file__).resolve().parents[2]
DSN = os.environ.get("TEST_CONTROL_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="TEST_CONTROL_DSN is unset")


def _plan():
    base = resolve_config(
        ROOT / "config/profiles/default.yaml",
        ROOT / "config/bindings/example-local.yaml",
        ROOT / "config/manifests/plugins.yaml",
        environment="local",
    )
    binding = base.binding.model_dump(mode="json")
    binding["project_id"] = f"demo-{uuid4().hex}"
    config = resolve_config_documents(
        base.profile.model_dump(mode="json"), binding,
        base.manifest.model_dump(mode="json"), environment="local",
    )
    due = datetime.now(timezone.utc).replace(microsecond=0)
    run, jobs = plan_run(config, FeedKind.METADATA, due)
    PostgresControlRepository(DSN).save_plan(
        config, run, jobs, queue_class="metadata_sweep"
    )
    return config, run, jobs


def _certify_site(job):
    artifact_id = hashlib.sha256(f"raw:{job.job_id}".encode()).hexdigest()
    batch_key = hashlib.sha256(f"sink:{job.job_id}".encode()).hexdigest()
    with psycopg.connect(DSN) as conn:
        conn.execute(
            """
            INSERT INTO ingestion.raw_artifacts
                (artifact_id, job_id, object_key, checksum, byte_count)
            VALUES (%s, %s, %s, %s, 1)
            """,
            (artifact_id, job.job_id, f"synthetic/{artifact_id}", "a" * 64),
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
                (job_id, artifact_id, batch_key, completed_scope)
            VALUES (%s, %s, %s, %s)
            """,
            (job.job_id, artifact_id, batch_key, Jsonb([job.site_ref])),
        )
        conn.execute(
            "UPDATE ingestion.jobs SET status = 'certified' WHERE job_id = %s",
            (job.job_id,),
        )
    PostgresCheckpointReconciler(DSN).reconcile_site(job.run_id, job.site_ref)


def test_run_status_transitions_from_pending_to_partial_to_certified(monkeypatch, capsys):
    apply_migrations(DSN, ROOT / "migrations/control")
    config, run, jobs = _plan()
    reader = PostgresRunStatusReader(DSN)
    args = {
        "run_id": run.run_id, "tenant_id": run.tenant_id,
        "project_id": run.project_id, "config_hash": run.config_hash,
        "feed": FeedKind.METADATA, "max_run_seconds": 600,
    }
    pending = reader.read(**args, now=run.scheduled_at + timedelta(minutes=1))
    assert (pending.state, pending.reason, pending.completed_sites) == (
        "pending", "awaiting_certification", 0
    )
    overdue = reader.read(**args, now=run.scheduled_at + timedelta(minutes=11))
    assert (overdue.state, overdue.reason) == ("blocked", "deadline_elapsed")
    _certify_site(jobs[0])
    partial = reader.read(**args, now=run.scheduled_at + timedelta(minutes=11))
    assert (partial.state, partial.reason, partial.completed_sites) == (
        "partial", "deadline_elapsed", 1
    )
    with psycopg.connect(DSN) as conn:
        conn.execute(
            "UPDATE ingestion.jobs SET status = 'quarantined' WHERE job_id = %s",
            (jobs[1].job_id,),
        )
    terminal = reader.read(**args, now=run.scheduled_at + timedelta(minutes=2))
    assert (terminal.state, terminal.reason, terminal.terminal_jobs) == (
        "partial", "terminal_job", 1
    )
    _certify_site(jobs[1])
    complete = reader.read(**args, now=run.scheduled_at + timedelta(minutes=11))
    assert (complete.state, complete.reason, complete.completed_sites) == (
        "certified", "all_sites_certified", 2
    )
    assert complete.evidence_certified_jobs == complete.total_jobs == 2

    monkeypatch.setenv("CONTROL_DATABASE_URL", DSN)
    assert control_main([
        "run-status", "--run-id", run.run_id,
        "--tenant-id", run.tenant_id, "--project-id", run.project_id,
        "--config-hash", config.config_hash, "--feed", "metadata",
        "--resources", str(ROOT / "local/resources.yaml"),
    ]) == 0
    assert json.loads(capsys.readouterr().out)["state"] == "certified"


def test_run_status_rejects_wrong_scope_and_inconsistent_certification():
    apply_migrations(DSN, ROOT / "migrations/control")
    _, run, jobs = _plan()
    reader = PostgresRunStatusReader(DSN)
    args = {
        "run_id": run.run_id, "tenant_id": run.tenant_id,
        "project_id": run.project_id, "config_hash": run.config_hash,
        "feed": FeedKind.METADATA, "max_run_seconds": 600,
    }
    with pytest.raises(ValueError, match="not found in approved"):
        reader.read(**{**args, "project_id": "another-project"})
    with pytest.raises(ValueError, match="not found in approved"):
        reader.read(**{**args, "feed": FeedKind.RULES})
    with psycopg.connect(DSN) as conn:
        conn.execute(
            "UPDATE ingestion.runs SET status = 'certified' WHERE run_id = %s",
            (run.run_id,),
        )
    inconsistent = reader.read(**args, now=run.scheduled_at + timedelta(minutes=1))
    assert (inconsistent.state, inconsistent.reason) == (
        "blocked", "inconsistent_control_state"
    )
