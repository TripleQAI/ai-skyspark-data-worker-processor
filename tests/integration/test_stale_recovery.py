"""Bounded requeue of jobs stranded after a sent SQS reference."""

from dataclasses import replace
from datetime import datetime, timezone
import os
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest

from ingestion.adapters.control.migrations import apply_migrations
from ingestion.adapters.control.postgres import PostgresControlRepository
from ingestion.config.loader import resolve_config
from ingestion.contracts.config import FeedKind
from ingestion.core.planner import plan_run


ROOT = Path(__file__).resolve().parents[2]
DSN = os.environ.get("TEST_CONTROL_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="TEST_CONTROL_DSN is unset")


def test_stale_recovery_respects_lease_age_attempts_and_redrive_cap():
    apply_migrations(DSN, ROOT / "migrations/control")
    base = resolve_config(
        ROOT / "config/profiles/default.yaml",
        ROOT / "config/bindings/example-local.yaml",
        ROOT / "config/manifests/plugins.yaml", environment="local",
    )
    unique = uuid4().hex
    config = replace(
        base,
        binding=base.binding.model_copy(update={"project_id": f"recovery-{unique}"}),
        config_hash=unique + unique,
    )
    run, jobs = plan_run(config, FeedKind.METADATA, datetime.now(timezone.utc))
    repository = PostgresControlRepository(DSN)
    repository.save_plan(config, run, jobs, queue_class="metadata_sweep")
    expired_job, unstarted_job = jobs
    first_token = repository.acquire_job_lease(
        job_id=expired_job.job_id, worker_id="crashed-worker", lease_seconds=60
    )
    assert first_token == 1
    with psycopg.connect(DSN) as conn:
        conn.execute(
            "UPDATE ingestion.dispatch_outbox SET state = 'sent', "
            "sent_at = now() - interval '1 hour', sqs_message_id = 'old-message' "
            "WHERE job_id = ANY(%s)", ([job.job_id for job in jobs],),
        )
        conn.execute(
            "UPDATE ingestion.jobs SET updated_at = now() - interval '1 hour' "
            "WHERE job_id = ANY(%s)", ([job.job_id for job in jobs],),
        )
        conn.execute(
            "UPDATE ingestion.jobs SET lease_expires_at = now() - interval '1 hour' "
            "WHERE job_id = %s", (expired_job.job_id,),
        )

    recovered = repository.requeue_stale(
        limit=10, expired_running_after_seconds=60,
        never_started_after_seconds=7200, max_redrives=1, run_id=run.run_id,
    )
    assert [(item.job_id, item.reason, item.redrive_count) for item in recovered] == [
        (expired_job.job_id, "expired_running", 1)
    ]
    assert repository.requeue_stale(
        limit=10, expired_running_after_seconds=60,
        never_started_after_seconds=7200, max_redrives=1, run_id=run.run_id,
    ) == ()
    with psycopg.connect(DSN) as conn:
        assert conn.execute(
            "SELECT state, sqs_message_id, redrive_count, last_error "
            "FROM ingestion.dispatch_outbox WHERE job_id = %s",
            (expired_job.job_id,),
        ).fetchone() == ("pending", None, 1, "StaleRecovery:expired_running")
        assert conn.execute(
            "SELECT state FROM ingestion.dispatch_outbox WHERE job_id = %s",
            (unstarted_job.job_id,),
        ).fetchone()[0] == "sent"
        conn.execute(
            "UPDATE ingestion.dispatch_outbox "
            "SET next_attempt_at = now() + interval '1 hour' "
            "WHERE state = 'pending' AND job_id <> %s", (expired_job.job_id,),
        )

    claimed = repository.claim_dispatch(owner="recovery-dispatcher", limit=1, lease_seconds=60)
    assert len(claimed) == 1 and claimed[0].job_id == expired_job.job_id
    assert repository.mark_dispatched(
        job_id=expired_job.job_id, owner="recovery-dispatcher",
        delivery_attempts=claimed[0].delivery_attempts, message_id="new-message",
    )
    second_token = repository.acquire_job_lease(
        job_id=expired_job.job_id, worker_id="new-worker", lease_seconds=60
    )
    assert second_token == 2
    with psycopg.connect(DSN) as conn:
        conn.execute(
            "UPDATE ingestion.dispatch_outbox SET sent_at = now() - interval '3 hours' "
            "WHERE job_id = ANY(%s)", ([job.job_id for job in jobs],),
        )
        conn.execute(
            "UPDATE ingestion.jobs SET updated_at = now() - interval '3 hours' "
            "WHERE job_id = ANY(%s)", ([job.job_id for job in jobs],),
        )
    recovered = repository.requeue_stale(
        limit=10, expired_running_after_seconds=60,
        never_started_after_seconds=7200, max_redrives=1, run_id=run.run_id,
    )
    assert [(item.job_id, item.reason) for item in recovered] == [
        (unstarted_job.job_id, "never_started")
    ]
    unstarted_token = repository.acquire_job_lease(
        job_id=unstarted_job.job_id, worker_id="handled-worker", lease_seconds=60
    )
    assert unstarted_token == 1
    assert repository.release_job_lease(
        job_id=unstarted_job.job_id, worker_id="handled-worker",
        fence_token=unstarted_token, error_class="SyntheticRetry",
    )
    assert repository.release_job_lease(
        job_id=expired_job.job_id, worker_id="new-worker",
        fence_token=second_token, error_class="SyntheticRetry",
    )
    with psycopg.connect(DSN) as conn:
        conn.execute(
            "UPDATE ingestion.dispatch_outbox SET state = 'sent', "
            "sent_at = now() - interval '3 hours' WHERE job_id = %s",
            (unstarted_job.job_id,),
        )
        conn.execute(
            "UPDATE ingestion.jobs SET updated_at = now() - interval '3 hours' "
            "WHERE job_id = %s", (expired_job.job_id,),
        )
    assert repository.requeue_stale(
        limit=10, expired_running_after_seconds=60,
        never_started_after_seconds=7200, max_redrives=2, run_id=run.run_id,
    ) == ()


def test_stale_recovery_validates_thresholds():
    repository = PostgresControlRepository(DSN or "postgresql://unused")
    with pytest.raises(ValueError, match="too short"):
        repository.requeue_stale(
            limit=10, expired_running_after_seconds=60,
            never_started_after_seconds=30, max_redrives=1,
        )
