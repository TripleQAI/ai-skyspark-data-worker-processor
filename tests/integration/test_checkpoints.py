"""Exercise contiguous, partition-complete checkpoint recovery in PostgreSQL."""

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
from ingestion.contracts.jobs import CertifiedInventory, JobCompletion, RawArtifact, SinkReceipt
from ingestion.core.planner import plan_run


ROOT = Path(__file__).resolve().parents[2]
DSN = os.environ.get("TEST_CONTROL_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="TEST_CONTROL_DSN is unset")


def _certify(repository, job):
    token = repository.acquire_job_lease(
        job_id=job.job_id, worker_id="checkpoint-test", lease_seconds=60
    )
    assert token is not None
    assert repository.certify_job(
        job_id=job.job_id, worker_id="checkpoint-test", fence_token=token,
        completion=JobCompletion(
            raw=RawArtifact(job_id=job.job_id, object_key=f"raw/{job.job_id}",
                            checksum="raw", byte_count=1),
            sink=SinkReceipt(job_id=job.job_id, sink_kind="timescale",
                             batch_key=f"batch/{job.job_id}", row_count=0,
                             checksum="sink"),
            completed_scope=job.scope_ids,
        ),
    )


def test_partition_complete_contiguous_windows_and_publication_are_idempotent():
    apply_migrations(DSN, ROOT / "migrations/control")
    base = resolve_config(
        ROOT / "config/profiles/default.yaml",
        ROOT / "config/bindings/example-local.yaml",
        ROOT / "config/manifests/plugins.yaml", environment="local",
    )
    unique = uuid4().hex
    config = replace(
        base,
        binding=base.binding.model_copy(update={"project_id": f"checkpoint-{unique}"}),
        config_hash=unique + unique,
    )
    inventory_data = CertifiedInventory.model_validate_json(
        (ROOT / "local/fixtures/inventory.json").read_text(encoding="utf-8")
    ).model_dump()
    inventory_data["project_id"] = config.binding.project_id
    inventory_data["sites"]["site-a"]["historized_point_ids"] = [
        f"point-a-{number:03d}" for number in range(501)
    ]
    inventory_data["sites"]["site-b"]["historized_point_ids"] = []
    inventory = CertifiedInventory.model_validate(inventory_data)

    start = datetime.now(timezone.utc) - timedelta(minutes=10)
    middle = start + timedelta(minutes=5)
    end = middle + timedelta(minutes=5)
    first, first_jobs = plan_run(
        config, FeedKind.HISTORY, middle, inventory=inventory,
        window_start=start, window_end=middle,
    )
    second, second_jobs = plan_run(
        config, FeedKind.HISTORY, end, inventory=inventory,
        window_start=middle, window_end=end,
    )
    assert len(first_jobs) == len(second_jobs) == 3
    assert [len(job.scope_ids) for job in first_jobs] == [500, 1, 0]
    repository = PostgresControlRepository(DSN)
    repository.save_plan(config, first, first_jobs, queue_class="history_live")
    repository.save_plan(config, second, second_jobs, queue_class="history_live")
    reconciler = PostgresCheckpointReconciler(DSN)
    assert reconciler.reconcile_site(first.run_id, "site-a").state == "waiting_for_certification"
    for job in second_jobs:
        _certify(repository, job)
    assert reconciler.reconcile_site(second.run_id, "site-a").state == "unseeded"
    for site in ("site-a", "site-b"):
        reconciler.seed_checkpoint(config, FeedKind.HISTORY, site, start)
        reconciler.seed_checkpoint(config, FeedKind.HISTORY, site, start)
    with pytest.raises(ValueError, match="different baseline"):
        reconciler.seed_checkpoint(config, FeedKind.HISTORY, "site-a", middle)

    assert reconciler.reconcile_site(second.run_id, "site-a").state == "waiting_for_prior_window"
    assert reconciler.reconcile_site(second.run_id, "site-b").state == "waiting_for_prior_window"
    _certify(repository, first_jobs[0])
    assert reconciler.reconcile_site(first.run_id, "site-a").state == "waiting_for_certification"
    _certify(repository, first_jobs[1])
    assert reconciler.reconcile_site(first.run_id, "site-a").state == "already_advanced"
    _certify(repository, first_jobs[2])
    assert reconciler.reconcile_site(first.run_id, "site-b").state == "already_advanced"
    assert [item.state for item in reconciler.reconcile_run(second.run_id)] == ["advanced", "advanced"]
    assert [item.state for item in reconciler.reconcile_run(second.run_id)] == [
        "already_advanced", "already_advanced"
    ]
    with psycopg.connect(DSN) as conn:
        rows = conn.execute(
            "SELECT site_ref, certified_through, baseline_at FROM ingestion.checkpoints "
            "WHERE tenant_id = %s AND project_id = %s AND feed = 'history' ORDER BY site_ref",
            (config.binding.tenant_id, config.binding.project_id),
        ).fetchall()
        assert rows == [("site-a", end, start), ("site-b", end, start)]
        assert conn.execute(
            "SELECT count(*) FROM ingestion.publication_outbox WHERE job_id = ANY(%s)",
            ([job.job_id for job in first_jobs + second_jobs],),
        ).fetchone()[0] == 6
        assert conn.execute(
            "SELECT status FROM ingestion.runs WHERE run_id = ANY(%s) ORDER BY run_id",
            ([first.run_id, second.run_id],),
        ).fetchall() == [("certified",), ("certified",)]


def test_checkpoint_error_rolls_back_final_certification(monkeypatch):
    apply_migrations(DSN, ROOT / "migrations/control")
    config = resolve_config(
        ROOT / "config/profiles/default.yaml",
        ROOT / "config/bindings/example-local.yaml",
        ROOT / "config/manifests/plugins.yaml", environment="local",
    )
    inventory = CertifiedInventory.model_validate_json(
        (ROOT / "local/fixtures/inventory.json").read_text(encoding="utf-8")
    )
    end = datetime.now(timezone.utc)
    run, jobs = plan_run(
        config, FeedKind.HISTORY, end, inventory=inventory,
        window_start=end - timedelta(minutes=5), window_end=end,
    )
    repository = PostgresControlRepository(DSN)
    repository.save_plan(config, run, jobs, queue_class="history_live")
    job = jobs[0]
    token = repository.acquire_job_lease(
        job_id=job.job_id, worker_id="rollback-test", lease_seconds=60
    )
    assert token is not None

    def fail_reconciliation(*_args, **_kwargs):
        raise RuntimeError("injected checkpoint failure")

    completion = JobCompletion(
        raw=RawArtifact(job_id=job.job_id, object_key=f"raw/{job.job_id}",
                        checksum="raw", byte_count=1),
        sink=SinkReceipt(job_id=job.job_id, sink_kind="timescale",
                         batch_key=f"batch/{job.job_id}", row_count=0,
                         checksum="sink"),
        completed_scope=job.scope_ids,
    )
    with monkeypatch.context() as patch:
        patch.setattr(PostgresCheckpointReconciler, "reconcile_site", fail_reconciliation)
        with pytest.raises(RuntimeError, match="injected checkpoint failure"):
            repository.certify_job(
                job_id=job.job_id, worker_id="rollback-test",
                fence_token=token, completion=completion,
            )
    assert repository.job_status(job.job_id) == "running"
    with psycopg.connect(DSN) as conn:
        assert conn.execute(
            "SELECT count(*) FROM ingestion.certifications WHERE job_id = %s",
            (job.job_id,),
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT count(*) FROM ingestion.publication_outbox WHERE job_id = %s",
            (job.job_id,),
        ).fetchone()[0] == 0
