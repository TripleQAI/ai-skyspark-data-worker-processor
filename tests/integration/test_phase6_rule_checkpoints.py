"""Nightly rule coverage is equipment based and site checkpoints stay isolated."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest

from ingestion.adapters.control.checkpoints import PostgresCheckpointReconciler
from ingestion.adapters.control.migrations import apply_migrations
from ingestion.adapters.control.postgres import PostgresControlRepository
from ingestion.config.loader import resolve_config
from ingestion.contracts.config import FeedKind
from ingestion.contracts.jobs import (
    CertifiedInventory, JobCompletion, RawArtifact, SinkReceipt, SiteInventory,
)
from ingestion.core.planner import plan_run


ROOT = Path(__file__).resolve().parents[2]
DSN = os.environ.get("TEST_CONTROL_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="TEST_CONTROL_DSN is unset")


def _certify(repo, job):
    token = repo.acquire_job_lease(job_id=job.job_id, worker_id="phase6", lease_seconds=60)
    assert token is not None
    assert repo.certify_job(
        job_id=job.job_id, worker_id="phase6", fence_token=token,
        completion=JobCompletion(
            raw=RawArtifact(job_id=job.job_id, object_key=f"raw/{job.job_id}",
                            checksum="raw", byte_count=1),
            sink=SinkReceipt(job_id=job.job_id, sink_kind="s3",
                             batch_key=f"batch/{job.job_id}", row_count=0,
                             checksum="sink"), completed_scope=job.scope_ids,
        ),
    )


def test_250_equipment_partition_and_other_site_advances_while_one_is_incomplete():
    apply_migrations(DSN, ROOT / "migrations/control")
    base = resolve_config(
        ROOT / "config/profiles/default.yaml",
        ROOT / "config/bindings/example-local.yaml",
        ROOT / "config/manifests/plugins.yaml", environment="local",
    )
    unique = uuid4().hex
    binding = base.binding.model_copy(update={
        "tenant_id": f"phase6-{unique}", "project_id": f"phase6-{unique}",
    })
    config = replace(base, binding=binding, config_hash=unique + unique)
    inventory = CertifiedInventory(
        version="phase6-250", tenant_id=binding.tenant_id,
        project_id=binding.project_id,
        sites={
            "site-a": SiteInventory(equipment_ids=tuple(f"equip-{n:03}" for n in range(249))),
            "site-b": SiteInventory(equipment_ids=("equip-249",)),
        },
    )
    start = datetime(2026, 9, 27, tzinfo=timezone.utc)
    run, jobs = plan_run(config, FeedKind.RULES, start + timedelta(days=1),
                         inventory=inventory, window_start=start,
                         window_end=start + timedelta(days=1))
    assert [len(job.scope_ids) for job in jobs] == [200, 49, 1]
    repo = PostgresControlRepository(DSN)
    repo.save_plan(config, run, jobs, queue_class="rules_nightly")
    cursor = PostgresCheckpointReconciler(DSN)
    for site in ("site-a", "site-b"):
        cursor.seed_checkpoint(config, FeedKind.RULES, site, start)

    _certify(repo, jobs[2])
    assert cursor.reconcile_site(run.run_id, "site-b").state == "already_advanced"
    assert cursor.reconcile_site(run.run_id, "site-a").state == "waiting_for_certification"
    _certify(repo, jobs[0])
    assert cursor.reconcile_site(run.run_id, "site-a").state == "waiting_for_certification"
    with psycopg.connect(DSN) as conn:
        rows = conn.execute(
            "SELECT site_ref, certified_through FROM ingestion.checkpoints "
            "WHERE tenant_id = %s AND project_id = %s AND feed = 'rules' ORDER BY site_ref",
            (binding.tenant_id, binding.project_id),
        ).fetchall()
        assert rows == [("site-a", start), ("site-b", run.window_end)]
    _certify(repo, jobs[1])
    assert cursor.reconcile_site(run.run_id, "site-a").state == "already_advanced"
