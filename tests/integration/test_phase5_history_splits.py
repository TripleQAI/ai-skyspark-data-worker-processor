"""Durable history splitting and contiguous checkpoint evidence."""

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
from ingestion.adapters.control.run_status import PostgresRunStatusReader
from ingestion.config.loader import resolve_config
from ingestion.contracts.config import FeedKind
from ingestion.contracts.jobs import (
    CertifiedInventory, JobCompletion, QueueEnvelope, RawArtifact, SinkReceipt,
    SiteInventory,
)
from ingestion.core.failures import HistorySplitRequired
from ingestion.core.failures import NonRetryableJobError
from ingestion.core.planner import plan_run
from ingestion.core.worker import Delivery, QueueWorker


ROOT = Path(__file__).resolve().parents[2]
DSN = os.environ.get("TEST_CONTROL_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="TEST_CONTROL_DSN is unset")


def _plan(ids=("point-a-1", "point-a-2")):
    base = resolve_config(
        ROOT / "config/profiles/default.yaml", ROOT / "config/bindings/example-local.yaml",
        ROOT / "config/manifests/plugins.yaml", environment="local",
    )
    unique = uuid4().hex
    binding = base.binding.model_copy(update={
        "tenant_id": f"phase5-{unique}", "project_id": f"phase5-{unique}",
        "approved_sites": {"site-a": "demoSiteA"},
    })
    config = replace(base, binding=binding, config_hash=unique + unique)
    inventory = CertifiedInventory(
        version="phase5-inventory", tenant_id=binding.tenant_id,
        project_id=binding.project_id,
        sites={"site-a": SiteInventory(point_ids=ids, historized_point_ids=ids)},
    )
    end = datetime(2026, 9, 28, 10, 5, tzinfo=timezone.utc)
    run, jobs = plan_run(config, FeedKind.HISTORY, end, inventory=inventory,
                         window_start=end - timedelta(minutes=5), window_end=end)
    assert len(jobs) == 1
    return config, run, jobs[0]


def _certify(repo, job):
    token = repo.acquire_job_lease(job_id=job.job_id, worker_id="phase5", lease_seconds=60)
    assert token is not None
    assert repo.certify_job(
        job_id=job.job_id, worker_id="phase5", fence_token=token,
        completion=JobCompletion(
            raw=RawArtifact(job_id=job.job_id, object_key=f"raw/{job.job_id}",
                            checksum="raw", byte_count=1),
            sink=SinkReceipt(job_id=job.job_id, sink_kind="timescale",
                             batch_key=f"batch/{job.job_id}", row_count=0,
                             checksum="sink"), completed_scope=job.scope_ids,
        ),
    )


def test_cap_split_persists_children_and_advances_only_after_both_certify():
    apply_migrations(DSN, ROOT / "migrations/control")
    config, run, parent = _plan()
    repo = PostgresControlRepository(DSN)
    repo.save_plan(config, run, (parent,), queue_class="history_live")
    cursor = PostgresCheckpointReconciler(DSN)
    cursor.seed_checkpoint(config, FeedKind.HISTORY, parent.site_ref, run.window_start)
    fence = repo.acquire_job_lease(job_id=parent.job_id, worker_id="phase5", lease_seconds=60)
    assert fence is not None
    assert repo.split_history_job(
        job_id=parent.job_id, worker_id="phase5", fence_token=fence,
        max_depth=4, min_window_seconds=30,
    )
    assert not repo.split_history_job(
        job_id=parent.job_id, worker_id="phase5", fence_token=fence,
        max_depth=4, min_window_seconds=30,
    )
    with psycopg.connect(DSN) as conn:
        rows = conn.execute(
            """SELECT child_job_id FROM ingestion.job_children
               WHERE parent_job_id = %s ORDER BY child_job_id""", (parent.job_id,),
        ).fetchall()
        assert len(rows) == 2
        assert conn.execute(
            "SELECT count(*) FROM ingestion.dispatch_outbox WHERE job_id = ANY(%s)",
            ([row[0] for row in rows],),
        ).fetchone()[0] == 2
    children = [repo.get_job(row[0]) for row in rows]
    assert {child.scope_ids for child in children} == {("point-a-1",), ("point-a-2",)}
    _certify(repo, children[0])
    assert cursor.reconcile_site(run.run_id, parent.site_ref).state == "waiting_for_certification"
    _certify(repo, children[1])
    assert cursor.reconcile_site(run.run_id, parent.site_ref).state == "already_advanced"
    with psycopg.connect(DSN) as conn:
        assert conn.execute(
            "SELECT certified_through FROM ingestion.checkpoints "
            "WHERE tenant_id = %s AND project_id = %s AND site_ref = %s AND feed = 'history'",
            (parent.tenant_id, parent.project_id, parent.site_ref),
        ).fetchone()[0] == run.window_end
        published = conn.execute(
            "SELECT job_id FROM ingestion.publication_outbox WHERE job_id = ANY(%s)",
            ([parent.job_id, *(child.job_id for child in children)],),
        ).fetchall()
        assert {row[0] for row in published} == {child.job_id for child in children}
    progress = PostgresRunStatusReader(DSN).read(
        run_id=run.run_id, tenant_id=parent.tenant_id, project_id=parent.project_id,
        config_hash=parent.config_hash, feed=FeedKind.HISTORY, max_run_seconds=900,
        now=run.scheduled_at,
    )
    assert progress.state == "certified"


def test_failed_child_keeps_gap_and_single_id_splits_by_time():
    apply_migrations(DSN, ROOT / "migrations/control")
    config, run, parent = _plan(("point-a-1",))
    repo = PostgresControlRepository(DSN)
    repo.save_plan(config, run, (parent,), queue_class="history_live")
    cursor = PostgresCheckpointReconciler(DSN)
    cursor.seed_checkpoint(config, FeedKind.HISTORY, parent.site_ref, run.window_start)
    fence = repo.acquire_job_lease(job_id=parent.job_id, worker_id="phase5", lease_seconds=60)
    assert repo.split_history_job(job_id=parent.job_id, worker_id="phase5", fence_token=fence,
                                  max_depth=4, min_window_seconds=30)
    with psycopg.connect(DSN) as conn:
        ids = [row[0] for row in conn.execute(
            "SELECT child_job_id FROM ingestion.job_children WHERE parent_job_id = %s",
            (parent.job_id,),
        ).fetchall()]
    children = sorted((repo.get_job(identifier) for identifier in ids),
                      key=lambda job: job.window_start)
    assert children[0].window_end == children[1].window_start
    _certify(repo, children[0])
    token = repo.acquire_job_lease(job_id=children[1].job_id,
                                   worker_id="phase5", lease_seconds=60)
    assert repo.quarantine_job(job_id=children[1].job_id,
                               worker_id="phase5", fence_token=token,
                               error_class="SimulatedSourceFailure")
    assert cursor.reconcile_site(run.run_id, parent.site_ref).state == "waiting_for_certification"
    with psycopg.connect(DSN) as conn:
        assert conn.execute(
            "SELECT certified_through FROM ingestion.checkpoints "
            "WHERE tenant_id = %s AND project_id = %s AND site_ref = %s AND feed = 'history'",
            (parent.tenant_id, parent.project_id, parent.site_ref),
        ).fetchone()[0] == run.window_start


def test_late_lookback_certifies_without_rewinding_cursor():
    apply_migrations(DSN, ROOT / "migrations/control")
    config, current, current_job = _plan(("point-a-1",))
    prior, prior_jobs = plan_run(
        config, FeedKind.HISTORY, current.scheduled_at,
        inventory=CertifiedInventory(
            version="phase5-inventory", tenant_id=config.binding.tenant_id,
            project_id=config.binding.project_id,
            sites={"site-a": SiteInventory(
                point_ids=("point-a-1",), historized_point_ids=("point-a-1",),
            )},
        ), window_start=current.window_start - timedelta(minutes=5),
        window_end=current.window_start,
    )
    repo = PostgresControlRepository(DSN)
    repo.save_plan(config, current, (current_job,), queue_class="history_live")
    repo.save_plan(config, prior, prior_jobs, queue_class="history_live")
    cursor = PostgresCheckpointReconciler(DSN)
    cursor.seed_checkpoint(config, FeedKind.HISTORY, current_job.site_ref, current.window_start)
    _certify(repo, current_job)
    _certify(repo, prior_jobs[0])
    assert cursor.reconcile_site(prior.run_id, current_job.site_ref).state == "already_advanced"
    with psycopg.connect(DSN) as conn:
        assert conn.execute(
            "SELECT certified_through FROM ingestion.checkpoints "
            "WHERE tenant_id = %s AND project_id = %s AND site_ref = %s AND feed = 'history'",
            (current_job.tenant_id, current_job.project_id, current_job.site_ref),
        ).fetchone()[0] == current.window_end


def test_worker_splits_cap_acknowledges_duplicate_and_retries_source_outage():
    apply_migrations(DSN, ROOT / "migrations/control")
    config, run, parent = _plan()
    repo = PostgresControlRepository(DSN)
    repo.save_plan(config, run, (parent,), queue_class="history_live")
    envelope = QueueEnvelope(job_id=parent.job_id, run_id=run.run_id,
                             config_hash=config.config_hash).model_dump_json()

    class Queue:
        deleted = 0

        def receive(self, queue_class, **kwargs):
            return (Delivery(receipt_handle="isolated", body=envelope),)

        def delete(self, queue_class, receipt_handle):
            self.deleted += 1

        def extend_visibility(self, queue_class, receipt_handle, seconds):
            pass

    class Handler:
        def run(self, job, *, cancel_events=()):
            raise HistorySplitRequired("simulated source cap")

    class Verifier:
        def verify(self, job, completion):
            raise AssertionError("source cap should not produce a completion")

    queue = Queue()
    worker = QueueWorker(
        repo, queue, Handler(), Verifier(), queue_class="history_live",
        worker_id="phase5-worker", allowed_feeds={FeedKind.HISTORY}, slots=1,
        batch_size=1, wait_seconds=0, visibility_seconds=60,
        lease_seconds=60, heartbeat_seconds=1, history_split_depth=4,
        history_min_split_window_seconds=30,
    )
    assert worker.run_once()[0].state == "split"
    assert worker.run_once()[0].state == "split_acked"
    assert queue.deleted == 2
    with psycopg.connect(DSN) as conn:
        assert conn.execute(
            "SELECT count(*) FROM ingestion.job_children WHERE parent_job_id = %s",
            (parent.job_id,),
        ).fetchone()[0] == 2

    # The second plan has a unique tenant, so its retry cannot affect the split.
    second_config, second_run, second_job = _plan()
    repo.save_plan(second_config, second_run, (second_job,), queue_class="history_live")
    outage_envelope = QueueEnvelope(
        job_id=second_job.job_id, run_id=second_run.run_id,
        config_hash=second_config.config_hash,
    ).model_dump_json()

    class OutageQueue(Queue):
        def receive(self, queue_class, **kwargs):
            return (Delivery(receipt_handle="outage", body=outage_envelope),)

    class OutageHandler:
        def run(self, job, *, cancel_events=()):
            raise RuntimeError("source unavailable")

    outage_queue = OutageQueue()
    outage_worker = QueueWorker(
        repo, outage_queue, OutageHandler(), Verifier(), queue_class="history_live",
        worker_id="phase5-outage", allowed_feeds={FeedKind.HISTORY}, slots=1,
        batch_size=1, wait_seconds=0, visibility_seconds=60,
        lease_seconds=60, heartbeat_seconds=1, history_split_depth=4,
    )
    assert outage_worker.run_once()[0].state == "retry"
    assert repo.job_status(second_job.job_id) == "planned"
    assert outage_queue.deleted == 0


def test_descendant_budget_rejects_unbounded_split():
    apply_migrations(DSN, ROOT / "migrations/control")
    config, run, parent = _plan()
    repo = PostgresControlRepository(DSN)
    repo.save_plan(config, run, (parent,), queue_class="history_live")
    token = repo.acquire_job_lease(job_id=parent.job_id,
                                   worker_id="budget", lease_seconds=60)
    assert repo.split_history_job(
        job_id=parent.job_id, worker_id="budget", fence_token=token,
        max_depth=12, min_window_seconds=30, max_descendant_jobs=2,
    )
    with psycopg.connect(DSN) as conn:
        child_id = conn.execute(
            "SELECT child_job_id FROM ingestion.job_children WHERE parent_job_id = %s LIMIT 1",
            (parent.job_id,),
        ).fetchone()[0]
    child_token = repo.acquire_job_lease(job_id=child_id,
                                         worker_id="budget", lease_seconds=60)
    with pytest.raises(NonRetryableJobError, match="descendant job cap"):
        repo.split_history_job(
            job_id=child_id, worker_id="budget", fence_token=child_token,
            max_depth=12, min_window_seconds=30, max_descendant_jobs=2,
        )
    assert repo.job_status(child_id) == "running"
