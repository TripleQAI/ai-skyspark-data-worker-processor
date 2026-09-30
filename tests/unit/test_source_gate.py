import threading
import time

import pytest

from ingestion.contracts.config import FeedKind
from ingestion.contracts.jobs import Job
from ingestion.contracts.permits import SourcePermitLease
from ingestion.core.source_gate import PermitGuardedHandler


def test_lost_source_permit_cancels_script_and_releases_reservation():
    job = Job(
        job_id="job-1", run_id="run-1", tenant_id="tenant-a",
        project_id="project-a", site_ref="site-a", feed=FeedKind.METADATA,
        scope_ids=(), config_hash="hash-1",
    )

    class Pool:
        released = False

        def try_acquire(self, **kwargs):
            return SourcePermitLease(
                job.tenant_id, job.project_id, job.job_id,
                kwargs["owner"], ((1, 1),),
            )

        def renew(self, lease, *, lease_seconds):
            return False

        def release(self, lease):
            self.released = True
            return 1

    class Runner:
        def source_slots(self, job):
            return 1

        def run(self, job, *, cancel_events=()):
            while not any(event.is_set() for event in cancel_events):
                time.sleep(0.005)
            raise RuntimeError("script stopped after permit loss")

    pool = Pool()
    handler = PermitGuardedHandler(
        Runner(), pool, worker_id="worker-a", lease_seconds=2,
        heartbeat_seconds=0.05, retry_seconds=0.01,
    )
    with pytest.raises(RuntimeError, match="script stopped"):
        handler.run(job, cancel_events=(threading.Event(),))
    assert pool.released
