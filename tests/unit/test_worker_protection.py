import threading

import pytest

from ingestion.contracts.config import FeedKind
from ingestion.contracts.jobs import Job, JobCompletion, QueueEnvelope, RawArtifact, SinkReceipt
from ingestion.core.task_protection import TaskProtectionError, TaskProtectionManager
from ingestion.core.worker import Delivery, QueueWorker


def _job(index):
    return Job(
        job_id=f"job-{index}", run_id="run", tenant_id="tenant",
        project_id="project", site_ref="site", feed=FeedKind.HISTORY,
        scope_ids=(f"point-{index}",), config_hash="hash",
    )


def _completion(job):
    return JobCompletion(
        raw=RawArtifact(
            job_id=job.job_id, object_key=f"raw/{job.job_id}",
            checksum="raw", byte_count=1,
        ),
        sink=SinkReceipt(
            job_id=job.job_id, sink_kind="s3", batch_key=f"batch/{job.job_id}",
            row_count=1, checksum="sink",
        ),
        completed_scope=job.scope_ids,
    )


def _worker(repository, queue, handler, manager):
    class Verifier:
        def verify(self, job, completion):
            pass

    return QueueWorker(
        repository, queue, handler, Verifier(),
        queue_class="history_live", worker_id="worker",
        allowed_feeds={FeedKind.HISTORY}, slots=2, batch_size=2,
        wait_seconds=0, visibility_seconds=20, lease_seconds=20,
        heartbeat_seconds=1, task_protection=manager,
    )


def test_worker_protects_before_receive_through_certification_and_ack():
    class Client:
        enabled = False
        calls = []

        def set_protection(self, enabled, *, expires_minutes):
            self.enabled = enabled
            self.calls.append(enabled)

    client = Client()
    jobs = {_job(index).job_id: _job(index) for index in range(2)}

    class Repository:
        certified = []

        def get_job(self, job_id):
            return jobs[job_id]

        def acquire_job_lease(self, **kwargs):
            return 1

        def certify_job(self, **kwargs):
            assert client.enabled
            self.certified.append(kwargs["job_id"])
            return True

    class Queue:
        deleted = []

        def receive(self, queue_class, **kwargs):
            assert client.enabled
            return tuple(
                Delivery(
                    receipt_handle=f"receipt-{index}",
                    body=QueueEnvelope(
                        job_id=f"job-{index}", run_id="run", config_hash="hash",
                    ).model_dump_json(),
                )
                for index in range(2)
            )

        def delete(self, queue_class, receipt_handle):
            assert client.enabled
            self.deleted.append(receipt_handle)

        def extend_visibility(self, queue_class, receipt_handle, seconds):
            pass

    class Handler:
        barrier = threading.Barrier(2)

        def run(self, job, *, cancel_events=()):
            assert client.enabled
            assert len(cancel_events) == 3
            self.barrier.wait(timeout=1)
            return _completion(job)

    manager = TaskProtectionManager(client, expires_minutes=10, refresh_seconds=60)
    try:
        repository, queue = Repository(), Queue()
        result = _worker(repository, queue, Handler(), manager).run_once()
        assert [item.state for item in result] == ["certified", "certified"]
        assert set(repository.certified) == set(jobs)
        assert set(queue.deleted) == {"receipt-0", "receipt-1"}
        assert client.calls == [True, False]
    finally:
        manager.close()


def test_worker_never_receives_when_protection_fails():
    class Client:
        def set_protection(self, enabled, *, expires_minutes):
            raise TimeoutError("agent unavailable")

    class Queue:
        calls = 0

        def receive(self, queue_class, **kwargs):
            self.calls += 1
            return ()

    manager = TaskProtectionManager(Client(), expires_minutes=10, refresh_seconds=60)
    try:
        queue = Queue()
        worker = _worker(object(), queue, object(), manager)
        with pytest.raises(TaskProtectionError, match="cannot protect"):
            worker.run_once()
        assert queue.calls == 0
    finally:
        manager.close()


def test_worker_retries_if_protection_refresh_is_lost():
    class Client:
        true_calls = 0

        def set_protection(self, enabled, *, expires_minutes):
            if enabled:
                self.true_calls += 1
                if self.true_calls == 2:
                    raise TimeoutError("refresh failed")

    class Repository:
        certified = False
        released = False

        def get_job(self, job_id):
            return _job(0)

        def acquire_job_lease(self, **kwargs):
            return 1

        def certify_job(self, **kwargs):
            self.certified = True
            return True

        def release_job_lease(self, **kwargs):
            self.released = True
            return True

    class Queue:
        deleted = False

        def receive(self, queue_class, **kwargs):
            return (Delivery(
                receipt_handle="receipt-0",
                body=QueueEnvelope(
                    job_id="job-0", run_id="run", config_hash="hash",
                ).model_dump_json(),
            ),)

        def delete(self, queue_class, receipt_handle):
            self.deleted = True

        def extend_visibility(self, queue_class, receipt_handle, seconds):
            pass

    class Handler:
        def run(self, job, *, cancel_events=()):
            assert cancel_events[-1].wait(1)
            return _completion(job)

    manager = TaskProtectionManager(
        Client(), expires_minutes=1, refresh_seconds=0.02,
    )
    try:
        repository, queue = Repository(), Queue()
        assert _worker(repository, queue, Handler(), manager).run_once()[0].state == "retry"
        assert repository.released and not repository.certified and not queue.deleted
    finally:
        manager.close()


def test_stopping_during_receive_does_not_admit_a_new_job():
    class Client:
        calls = []

        def set_protection(self, enabled, *, expires_minutes):
            self.calls.append(enabled)

    stop = threading.Event()

    class Queue:
        def receive(self, queue_class, **kwargs):
            stop.set()
            return (Delivery(
                receipt_handle="receipt-0",
                body=QueueEnvelope(
                    job_id="job-0", run_id="run", config_hash="hash",
                ).model_dump_json(),
            ),)

    class Handler:
        def run(self, job, *, cancel_events=()):
            raise AssertionError("no job should start after stop")

    client = Client()
    manager = TaskProtectionManager(client, expires_minutes=10, refresh_seconds=60)
    try:
        worker = _worker(object(), Queue(), Handler(), manager)
        worker.serve_forever(stop)
        assert client.calls == [True, False]
    finally:
        manager.close()
