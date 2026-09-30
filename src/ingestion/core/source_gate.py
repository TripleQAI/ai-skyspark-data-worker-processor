"""Reserve source-call slots while a registered script executes."""

from __future__ import annotations

import threading
from typing import Protocol
from uuid import uuid4

from ingestion.contracts.jobs import Job, JobCompletion
from ingestion.contracts.permits import SourcePermitLease


class SourcePermitPool(Protocol):
    def try_acquire(
        self, *, tenant_id: str, project_id: str, job_id: str,
        owner: str, slots: int, lease_seconds: int,
    ) -> SourcePermitLease | None: ...

    def renew(self, lease: SourcePermitLease, *, lease_seconds: int) -> bool: ...

    def release(self, lease: SourcePermitLease) -> int: ...


class SourceRunner(Protocol):
    def source_slots(self, job: Job) -> int: ...

    def run(
        self, job: Job, *, cancel_events: tuple[threading.Event, ...] = ()
    ) -> JobCompletion: ...


class SourcePermitLost(RuntimeError):
    """The script must not certify after losing its source reservation."""


class PermitGuardedHandler:
    def __init__(
        self, runner: SourceRunner, pool: SourcePermitPool, *,
        worker_id: str, lease_seconds: int,
        heartbeat_seconds: int, retry_seconds: float,
    ):
        if not worker_id or lease_seconds < 1 or not (
            0 < heartbeat_seconds < lease_seconds / 2
        ) or retry_seconds <= 0:
            raise ValueError("invalid source permit worker or timing policy")
        self._runner = runner
        self._pool = pool
        self._worker_id = worker_id
        self._lease_seconds = lease_seconds
        self._heartbeat_seconds = heartbeat_seconds
        self._retry_seconds = retry_seconds

    def run(
        self, job: Job, *, cancel_events: tuple[threading.Event, ...] = ()
    ) -> JobCompletion:
        owner = f"{self._worker_id}:{job.job_id}:{uuid4().hex}"
        slots = self._runner.source_slots(job)
        while True:
            if any(event.is_set() for event in cancel_events):
                raise SourcePermitLost("job lease ended before source admission")
            lease = self._pool.try_acquire(
                tenant_id=job.tenant_id, project_id=job.project_id,
                job_id=job.job_id, owner=owner, slots=slots,
                lease_seconds=self._lease_seconds,
            )
            if lease is not None:
                break
            threading.Event().wait(self._retry_seconds)

        stop_renewing = threading.Event()
        permit_lost = threading.Event()

        def renew_loop() -> None:
            while not stop_renewing.wait(self._heartbeat_seconds):
                try:
                    if not self._pool.renew(
                        lease, lease_seconds=self._lease_seconds
                    ):
                        permit_lost.set()
                        return
                except Exception:
                    permit_lost.set()
                    return

        heartbeat = threading.Thread(target=renew_loop, daemon=True)
        heartbeat.start()
        try:
            completion = self._runner.run(
                job, cancel_events=cancel_events + (permit_lost,)
            )
            if permit_lost.is_set() or any(event.is_set() for event in cancel_events):
                raise SourcePermitLost("source reservation ended before completion")
            return completion
        finally:
            stop_renewing.set()
            heartbeat.join()
            self._pool.release(lease)
