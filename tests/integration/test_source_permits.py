"""Distributed source capacity against the local control PostgreSQL database."""

from __future__ import annotations

import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import psycopg
import pytest

from ingestion.adapters.control.source_permits import PostgresSourcePermitPool
from ingestion.contracts.config import FeedKind
from ingestion.contracts.jobs import Job, JobCompletion, RawArtifact, SinkReceipt
from ingestion.core.source_gate import PermitGuardedHandler


DSN = os.environ.get("TEST_CONTROL_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="TEST_CONTROL_DSN is unset")


def _scope():
    unique = uuid4().hex
    return f"tenant-{unique}", f"project-{unique}"


def test_source_permits_are_atomic_scoped_and_fenced():
    tenant, project = _scope()
    pool = PostgresSourcePermitPool(DSN)
    pool.ensure_budget(tenant_id=tenant, project_id=project, max_concurrent_calls=2)
    pool.ensure_budget(tenant_id=tenant, project_id=project, max_concurrent_calls=2)
    with pytest.raises(ValueError, match="differs"):
        pool.ensure_budget(tenant_id=tenant, project_id=project, max_concurrent_calls=3)

    first = pool.try_acquire(
        tenant_id=tenant, project_id=project, job_id="job-a",
        owner="owner-a", slots=1, lease_seconds=60,
    )
    assert first and len(first.slots) == 1
    assert pool.try_acquire(
        tenant_id=tenant, project_id=project, job_id="job-b",
        owner="owner-b", slots=2, lease_seconds=60,
    ) is None
    second = pool.try_acquire(
        tenant_id=tenant, project_id=project, job_id="job-c",
        owner="owner-c", slots=1, lease_seconds=60,
    )
    assert second and second.slots[0][0] != first.slots[0][0]
    assert pool.renew(first, lease_seconds=60)
    with psycopg.connect(DSN) as conn:
        conn.execute(
            "UPDATE ingestion.source_permits SET lease_until = now() - interval '1 second' "
            "WHERE tenant_id = %s AND project_id = %s AND slot_no = %s",
            (tenant, project, first.slots[0][0]),
        )
    reclaimed = pool.try_acquire(
        tenant_id=tenant, project_id=project, job_id="job-d",
        owner="owner-d", slots=1, lease_seconds=60,
    )
    assert reclaimed and reclaimed.slots[0][0] == first.slots[0][0]
    assert reclaimed.slots[0][1] > first.slots[0][1]
    assert not pool.renew(first, lease_seconds=60)
    assert pool.release(first) == 0
    assert pool.release(reclaimed) == 1
    assert pool.release(second) == 1


def test_source_permits_bound_concurrent_script_invocations():
    tenant, project = _scope()
    pool = PostgresSourcePermitPool(DSN)
    pool.ensure_budget(tenant_id=tenant, project_id=project, max_concurrent_calls=2)
    job = Job(
        job_id=uuid4().hex, run_id=uuid4().hex,
        tenant_id=tenant, project_id=project, site_ref="site-a",
        feed=FeedKind.METADATA, scope_ids=(), config_hash="x" * 64,
    )

    class Runner:
        def __init__(self):
            self.lock = threading.Lock()
            self.active = 0
            self.peak = 0

        def source_slots(self, job):
            return 1

        def run(self, job, *, cancel_events=()):
            with self.lock:
                self.active += 1
                self.peak = max(self.active, self.peak)
            time.sleep(0.08)
            with self.lock:
                self.active -= 1
            return JobCompletion(
                raw=RawArtifact(
                    job_id=job.job_id, object_key="raw/test",
                    checksum="checksum", byte_count=1,
                ),
                sink=SinkReceipt(
                    job_id=job.job_id, sink_kind="s3", batch_key="batch/test",
                    row_count=1, checksum="checksum",
                ),
                completed_scope=(job.site_ref,),
            )

    runner = Runner()
    handler = PermitGuardedHandler(
        runner, pool, worker_id="test-worker", lease_seconds=10,
        heartbeat_seconds=1, retry_seconds=0.01,
    )
    with ThreadPoolExecutor(max_workers=5) as executor:
        completions = list(executor.map(handler.run, [job] * 5))
    assert len(completions) == 5
    assert 1 <= runner.peak <= 2
    with psycopg.connect(DSN) as conn:
        assert conn.execute(
            "SELECT count(*) FROM ingestion.source_permits "
            "WHERE tenant_id = %s AND project_id = %s AND lease_owner IS NOT NULL",
            (tenant, project),
        ).fetchone()[0] == 0
