"""Exercise the control store against a disposable local PostgreSQL database."""

from __future__ import annotations

import os
import json
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg
import pytest

from ingestion.adapters.control.migrations import apply_migrations
from ingestion.adapters.control.postgres import PostgresControlRepository
from ingestion.control_cli import main as control_main
from ingestion.config.loader import resolve_config
from ingestion.contracts.config import FeedKind
from ingestion.contracts.jobs import CertifiedInventory, JobCompletion, RawArtifact, SinkReceipt
from ingestion.contracts.jobs import QueueEnvelope
from ingestion.core.dispatch import DispatchReceipt, dispatch_once
from ingestion.core.planner import plan_run
from ingestion.core.worker import Delivery, QueueWorker


ROOT = Path(__file__).resolve().parents[2]
DSN = os.environ.get("TEST_CONTROL_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="TEST_CONTROL_DSN is unset")


class SyntheticEvidenceVerifier:
    """The control tests do not write physical objects."""

    def verify(self, job, completion):
        assert completion.raw.job_id == job.job_id
        assert completion.sink.job_id == job.job_id


def _plan():
    config = resolve_config(
        ROOT / "config/profiles/default.yaml",
        ROOT / "config/bindings/example-local.yaml",
        ROOT / "config/manifests/plugins.yaml",
        environment="local",
    )
    inventory = CertifiedInventory.model_validate_json(
        (ROOT / "local/fixtures/inventory.json").read_text(encoding="utf-8")
    )
    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=5)
    run, jobs = plan_run(
        config, FeedKind.HISTORY, end, inventory=inventory,
        window_start=start, window_end=end,
    )
    return config, run, jobs


def test_migration_and_plan_are_idempotent():
    apply_migrations(DSN, ROOT / "migrations/control")
    assert apply_migrations(DSN, ROOT / "migrations/control") == ()
    config, run, jobs = _plan()
    repository = PostgresControlRepository(DSN)
    first = repository.save_plan(config, run, jobs, queue_class="history_live")
    second = repository.save_plan(config, run, jobs, queue_class="history_live")
    assert first.new_jobs == len(jobs)
    assert second.new_jobs == 0
    assert second.expected_jobs == len(jobs)
    with psycopg.connect(DSN) as conn:
        assert conn.execute(
            "SELECT count(*) FROM ingestion.dispatch_outbox WHERE job_id = ANY(%s)",
            ([job.job_id for job in jobs],),
        ).fetchone()[0] == len(jobs)
        assert conn.execute(
            "SELECT max_concurrent_calls FROM ingestion.source_budgets "
            "WHERE tenant_id = %s AND project_id = %s",
            (run.tenant_id, run.project_id),
        ).fetchone()[0] == config.profile.source_policy.max_concurrent_calls
    assert repository.get_job(jobs[0].job_id) == jobs[0]


def test_dispatch_claim_recovery_and_fenced_worker_lease():
    config, run, jobs = _plan()
    repository = PostgresControlRepository(DSN)
    repository.save_plan(config, run, jobs, queue_class="history_live")

    class Sender:
        messages = []

        def send_batch(self, queue_class, envelopes):
            receipts = []
            for envelope in envelopes:
                self.messages.append((queue_class, envelope))
                receipts.append(DispatchReceipt(
                    message_id=f"local-message-{len(self.messages)}", error_class=None,
                ))
            return tuple(receipts)

    # Keep unrelated test rows out of this claim batch by moving their retry
    # times forward. The two new rows are left ready to claim.
    with psycopg.connect(DSN) as conn:
        conn.execute(
            "UPDATE ingestion.dispatch_outbox SET next_attempt_at = now() + interval '1 hour' "
            "WHERE state = 'pending' AND job_id <> ALL(%s)",
            ([job.job_id for job in jobs],),
        )
    first_claim = repository.claim_dispatch(owner="crashed-dispatcher", limit=2, lease_seconds=60)
    assert {intent.job_id for intent in first_claim} == {job.job_id for job in jobs}
    assert repository.claim_dispatch(owner="other-dispatcher", limit=2, lease_seconds=60) == ()
    with psycopg.connect(DSN) as conn:
        conn.execute(
            "UPDATE ingestion.dispatch_outbox SET claim_until = now() - interval '1 second' "
            "WHERE job_id = ANY(%s)",
            ([job.job_id for job in jobs],),
        )
    sender = Sender()
    result = dispatch_once(
        repository, sender, owner="recovery-dispatcher", limit=2,
        lease_seconds=60, base_backoff_seconds=1, max_backoff_seconds=60,
    )
    assert (result.claimed, result.sent, result.failed) == (2, 2, 0)
    assert {envelope.job_id for _, envelope in sender.messages} == {job.job_id for job in jobs}
    assert all(queue == "history_live" for queue, _ in sender.messages)
    assert repository.claim_dispatch(owner="third-dispatcher", limit=2, lease_seconds=60) == ()

    first_token = repository.acquire_job_lease(
        job_id=jobs[0].job_id, worker_id="worker-a", lease_seconds=60
    )
    assert first_token == 1
    assert repository.acquire_job_lease(
        job_id=jobs[0].job_id, worker_id="worker-b", lease_seconds=60
    ) is None
    assert repository.renew_job_lease(
        job_id=jobs[0].job_id, worker_id="worker-a", fence_token=first_token,
        lease_seconds=60,
    )
    with psycopg.connect(DSN) as conn:
        conn.execute(
            "UPDATE ingestion.jobs SET lease_expires_at = now() - interval '1 second' "
            "WHERE job_id = %s", (jobs[0].job_id,),
        )
    second_token = repository.acquire_job_lease(
        job_id=jobs[0].job_id, worker_id="worker-b", lease_seconds=60
    )
    assert second_token == 2
    assert not repository.renew_job_lease(
        job_id=jobs[0].job_id, worker_id="worker-a", fence_token=first_token,
        lease_seconds=60,
    )


def test_conflicting_route_rolls_back():
    config, run, jobs = _plan()
    repository = PostgresControlRepository(DSN)
    repository.save_plan(config, run, jobs, queue_class="history_live")
    with pytest.raises(ValueError, match="conflicting queue route"):
        repository.save_plan(config, run, jobs, queue_class="backfill")
    with psycopg.connect(DSN) as conn:
        assert conn.execute(
            "SELECT count(*) FROM ingestion.dispatch_outbox "
            "WHERE job_id = ANY(%s) AND queue_class = 'backfill'",
            ([job.job_id for job in jobs],),
        ).fetchone()[0] == 0


def test_control_cli_persists_plan_from_configuration(monkeypatch, capsys):
    _, run, jobs = _plan()
    monkeypatch.setenv("CONTROL_DATABASE_URL", DSN)
    exit_code = control_main([
        "persist-plan",
        "--profile", str(ROOT / "config/profiles/default.yaml"),
        "--binding", str(ROOT / "config/bindings/example-local.yaml"),
        "--manifest", str(ROOT / "config/manifests/plugins.yaml"),
        "--environment", "local",
        "--resources", str(ROOT / "local/resources.yaml"),
        "--feed", "history",
        "--scheduled-at", run.scheduled_at.isoformat(),
        "--inventory", str(ROOT / "local/fixtures/inventory.json"),
        "--window-start", run.window_start.isoformat(),
        "--window-end", run.window_end.isoformat(),
    ])
    assert exit_code == 0
    output = json.loads(capsys.readouterr().out)
    assert output == {
        "run_id": run.run_id,
        "config_hash": run.config_hash,
        "expected_jobs": len(jobs),
        "new_jobs": len(jobs),
        "queue_class": "history_live",
    }


def test_dispatch_failure_releases_claim_with_backoff():
    config, run, jobs = _plan()
    repository = PostgresControlRepository(DSN)
    repository.save_plan(config, run, jobs, queue_class="history_live")
    with psycopg.connect(DSN) as conn:
        conn.execute(
            "UPDATE ingestion.dispatch_outbox SET next_attempt_at = now() + interval '1 hour' "
            "WHERE state = 'pending' AND job_id <> ALL(%s)",
            ([job.job_id for job in jobs],),
        )

    class FailingSender:
        def send_batch(self, queue_class, envelopes):
            raise TimeoutError("simulated local queue timeout")

    result = dispatch_once(
        repository, FailingSender(), owner="failing-dispatcher", limit=2,
        lease_seconds=60, base_backoff_seconds=5, max_backoff_seconds=300,
    )
    assert (result.claimed, result.sent, result.failed) == (2, 0, 2)
    with psycopg.connect(DSN) as conn:
        rows = conn.execute(
            "SELECT state, last_error, next_attempt_at > now() AS delayed "
            "FROM ingestion.dispatch_outbox WHERE job_id = ANY(%s)",
            ([job.job_id for job in jobs],),
        ).fetchall()
    assert rows == [("pending", "TimeoutError", True)] * len(jobs)


def test_dispatch_claim_attempt_fences_reused_owner():
    config, run, jobs = _plan()
    repository = PostgresControlRepository(DSN)
    repository.save_plan(config, run, jobs, queue_class="history_live")
    with psycopg.connect(DSN) as conn:
        conn.execute(
            "UPDATE ingestion.dispatch_outbox SET next_attempt_at = now() + interval '1 hour' "
            "WHERE state = 'pending' AND job_id <> ALL(%s)",
            ([job.job_id for job in jobs],),
        )
    old = repository.claim_dispatch(owner="reused-owner", limit=2, lease_seconds=60)
    with psycopg.connect(DSN) as conn:
        conn.execute(
            "UPDATE ingestion.dispatch_outbox SET claim_until = now() - interval '1 second' "
            "WHERE job_id = ANY(%s)",
            ([job.job_id for job in jobs],),
        )
    new = repository.claim_dispatch(owner="reused-owner", limit=2, lease_seconds=60)
    old_by_id = {intent.job_id: intent for intent in old}
    assert len(new) == len(jobs)
    for intent in new:
        assert intent.delivery_attempts == old_by_id[intent.job_id].delivery_attempts + 1
        assert not repository.mark_dispatched(
            job_id=intent.job_id, owner="reused-owner",
            delivery_attempts=old_by_id[intent.job_id].delivery_attempts,
            message_id="stale-send",
        )
        assert repository.mark_dispatched(
            job_id=intent.job_id, owner="reused-owner",
            delivery_attempts=intent.delivery_attempts,
            message_id="new-send",
        )


def test_certification_requires_current_lease_full_scope_and_target():
    config, run, jobs = _plan()
    repository = PostgresControlRepository(DSN)
    repository.save_plan(config, run, jobs, queue_class="history_live")
    job = jobs[0]
    token = repository.acquire_job_lease(
        job_id=job.job_id, worker_id="cert-worker", lease_seconds=60
    )
    assert token == 1
    completion = JobCompletion(
        raw=RawArtifact(
            job_id=job.job_id, object_key=f"raw/{job.job_id}.jsonl",
            checksum="raw-checksum", byte_count=100,
        ),
        sink=SinkReceipt(
            job_id=job.job_id, sink_kind="timescale",
            batch_key=f"batch/{job.job_id}", row_count=5,
            checksum="sink-checksum",
        ),
        completed_scope=job.scope_ids,
    )
    with pytest.raises(ValueError, match="full requested scope"):
        repository.certify_job(
            job_id=job.job_id, worker_id="cert-worker", fence_token=token,
            completion=completion.model_copy(update={"completed_scope": ()}),
        )
    with pytest.raises(ValueError, match="sink kind"):
        repository.certify_job(
            job_id=job.job_id, worker_id="cert-worker", fence_token=token,
            completion=completion.model_copy(update={
                "sink": completion.sink.model_copy(update={"sink_kind": "s3"})
            }),
        )
    assert not repository.certify_job(
        job_id=job.job_id, worker_id="old-worker", fence_token=token,
        completion=completion,
    )
    assert repository.certify_job(
        job_id=job.job_id, worker_id="cert-worker", fence_token=token,
        completion=completion,
    )
    assert repository.job_status(job.job_id) == "certified"
    assert not repository.certify_job(
        job_id=job.job_id, worker_id="cert-worker", fence_token=token,
        completion=completion,
    )
    with psycopg.connect(DSN) as conn:
        assert conn.execute(
            "SELECT count(*) FROM ingestion.certifications WHERE job_id = %s",
            (job.job_id,),
        ).fetchone()[0] == 1


def test_bounded_worker_certifies_then_acks_and_skips_duplicate():
    config, run, jobs = _plan()
    repository = PostgresControlRepository(DSN)
    repository.save_plan(config, run, jobs, queue_class="history_live")
    deliveries = tuple(
        Delivery(
            receipt_handle=f"receipt-{index}",
            body=QueueEnvelope(
                job_id=job.job_id, run_id=run.run_id,
                config_hash=run.config_hash,
            ).model_dump_json(),
        )
        for index, job in enumerate(jobs)
    )

    class Queue:
        def __init__(self):
            self.deleted = []
            self.calls = 0

        def receive(self, queue_class, **kwargs):
            assert queue_class in {"history_live", "rules_nightly"}
            assert kwargs["max_messages"] == 2
            self.calls += 1
            return deliveries

        def delete(self, queue_class, receipt_handle):
            self.deleted.append(receipt_handle)

        def extend_visibility(self, queue_class, receipt_handle, seconds):
            pass

    class Handler:
        def __init__(self):
            self.lock = threading.Lock()
            self.active = 0
            self.max_active = 0
            self.calls = 0

        def run(self, job, *, cancel_events=()):
            with self.lock:
                self.active += 1
                self.max_active = max(self.active, self.max_active)
                self.calls += 1
            time.sleep(0.05)
            with self.lock:
                self.active -= 1
            return JobCompletion(
                raw=RawArtifact(
                    job_id=job.job_id, object_key=f"raw/{job.job_id}",
                    checksum="raw", byte_count=10,
                ),
                sink=SinkReceipt(
                    job_id=job.job_id, sink_kind="timescale",
                    batch_key=f"batch/{job.job_id}", row_count=1,
                    checksum="sink",
                ),
                completed_scope=job.scope_ids,
            )

    queue = Queue()
    handler = Handler()
    worker = QueueWorker(
        repository, queue, handler, SyntheticEvidenceVerifier(),
        queue_class="history_live",
        worker_id="worker-integration", allowed_feeds={FeedKind.HISTORY},
        slots=2, batch_size=2,
        wait_seconds=0, visibility_seconds=20,
        lease_seconds=20, heartbeat_seconds=1,
    )
    first = worker.run_once()
    assert {outcome.state for outcome in first} == {"certified"}
    assert handler.max_active == 2
    assert len(queue.deleted) == 2
    second = worker.run_once()
    assert {outcome.state for outcome in second} == {"duplicate_acked"}
    assert handler.calls == 2
    assert len(queue.deleted) == 4
    wrong_queue_worker = QueueWorker(
        repository, queue, handler, SyntheticEvidenceVerifier(),
        queue_class="rules_nightly",
        worker_id="wrong-route-worker", allowed_feeds={FeedKind.RULES},
        slots=2, batch_size=2, wait_seconds=0,
        visibility_seconds=20, lease_seconds=20, heartbeat_seconds=1,
    )
    assert {item.state for item in wrong_queue_worker.run_once()} == {
        "wrong_queue_for_feed"
    }
    assert handler.calls == 2
    with psycopg.connect(DSN) as conn:
        assert conn.execute(
            "SELECT count(*) FROM ingestion.certifications WHERE job_id = ANY(%s)",
            ([job.job_id for job in jobs],),
        ).fetchone()[0] == 2


def test_worker_failure_records_retry_without_acknowledgment():
    config, run, jobs = _plan()
    repository = PostgresControlRepository(DSN)
    repository.save_plan(config, run, jobs, queue_class="history_live")
    job = jobs[0]
    delivery = Delivery(
        receipt_handle="failed-receipt",
        body=QueueEnvelope(
            job_id=job.job_id, run_id=run.run_id, config_hash=run.config_hash
        ).model_dump_json(),
    )

    class Queue:
        deleted = []

        def receive(self, queue_class, **kwargs):
            return (delivery,)

        def delete(self, queue_class, receipt_handle):
            self.deleted.append(receipt_handle)

        def extend_visibility(self, queue_class, receipt_handle, seconds):
            pass

    class Handler:
        def run(self, job, *, cancel_events=()):
            raise TimeoutError("synthetic failure")

    queue = Queue()
    worker = QueueWorker(
        repository, queue, Handler(), SyntheticEvidenceVerifier(),
        queue_class="history_live",
        worker_id="failing-worker", allowed_feeds={FeedKind.HISTORY},
        slots=1, batch_size=1, wait_seconds=0,
        visibility_seconds=20, lease_seconds=20, heartbeat_seconds=1,
    )
    assert worker.run_once()[0].state == "retry"
    assert queue.deleted == []
    assert repository.job_status(job.job_id) == "planned"
    with psycopg.connect(DSN) as conn:
        assert conn.execute(
            "SELECT outcome, error_class FROM ingestion.attempts WHERE job_id = %s",
            (job.job_id,),
        ).fetchone() == ("retry", "TimeoutError")


def test_worker_does_not_certify_when_physical_verification_fails():
    config, run, jobs = _plan()
    repository = PostgresControlRepository(DSN)
    repository.save_plan(config, run, jobs, queue_class="history_live")
    job = jobs[0]
    delivery = Delivery(
        receipt_handle="unverified-receipt",
        body=QueueEnvelope(
            job_id=job.job_id, run_id=run.run_id, config_hash=run.config_hash
        ).model_dump_json(),
    )

    class Queue:
        deleted = []

        def receive(self, queue_class, **kwargs):
            return (delivery,)

        def delete(self, queue_class, receipt_handle):
            self.deleted.append(receipt_handle)

        def extend_visibility(self, queue_class, receipt_handle, seconds):
            pass

    class Handler:
        def run(self, job, *, cancel_events=()):
            return JobCompletion(
                raw=RawArtifact(
                    job_id=job.job_id, object_key="raw/missing",
                    checksum="missing", byte_count=1,
                ),
                sink=SinkReceipt(
                    job_id=job.job_id, sink_kind="timescale",
                    batch_key="batch/missing", row_count=0, checksum="missing",
                ),
                completed_scope=job.scope_ids,
            )

    class RejectMissingEvidence:
        def verify(self, job, completion):
            raise ValueError("physical evidence is missing")

    queue = Queue()
    worker = QueueWorker(
        repository, queue, Handler(), RejectMissingEvidence(),
        queue_class="history_live", worker_id="verifier-worker",
        allowed_feeds={FeedKind.HISTORY}, slots=1, batch_size=1,
        wait_seconds=0, visibility_seconds=20, lease_seconds=20,
        heartbeat_seconds=1,
    )
    assert worker.run_once()[0].state == "retry"
    assert queue.deleted == []
    assert repository.job_status(job.job_id) == "planned"
    with psycopg.connect(DSN) as conn:
        assert conn.execute(
            "SELECT count(*) FROM ingestion.certifications WHERE job_id = %s",
            (job.job_id,),
        ).fetchone()[0] == 0
