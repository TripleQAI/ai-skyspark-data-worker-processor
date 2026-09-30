"""Bounded queue worker; each slot admits one full job pipeline."""

from __future__ import annotations

import threading
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from typing import Protocol

from pydantic import ValidationError

from ingestion.contracts.config import FeedKind
from ingestion.contracts.jobs import Job, JobCompletion, QueueEnvelope
from ingestion.core.failures import HistorySplitRequired, NonRetryableJobError
from ingestion.core.task_protection import TaskProtectionError, TaskProtectionManager


@dataclass(frozen=True, slots=True)
class Delivery:
    receipt_handle: str
    body: str


@dataclass(frozen=True, slots=True)
class WorkerOutcome:
    job_id: str | None
    state: str


class QueueTransport(Protocol):
    def receive(
        self, queue_class: str, *, max_messages: int,
        wait_seconds: int, visibility_seconds: int,
    ) -> tuple[Delivery, ...]: ...

    def delete(self, queue_class: str, receipt_handle: str) -> None: ...

    def extend_visibility(
        self, queue_class: str, receipt_handle: str, seconds: int
    ) -> None: ...


class JobHandler(Protocol):
    def run(
        self, job: Job, *, cancel_events: tuple[threading.Event, ...] = ()
    ) -> JobCompletion: ...


class EvidenceVerifier(Protocol):
    def verify(self, job: Job, completion: JobCompletion) -> None: ...


class WorkerRepository(Protocol):
    def get_job(self, job_id: str) -> Job | None: ...

    def job_status(self, job_id: str) -> str | None: ...

    def acquire_job_lease(
        self, *, job_id: str, worker_id: str, lease_seconds: int
    ) -> int | None: ...

    def renew_job_lease(
        self, *, job_id: str, worker_id: str, fence_token: int, lease_seconds: int
    ) -> bool: ...

    def release_job_lease(
        self, *, job_id: str, worker_id: str, fence_token: int, error_class: str
    ) -> bool: ...

    def quarantine_job(
        self, *, job_id: str, worker_id: str, fence_token: int, error_class: str
    ) -> bool: ...

    def certify_job(
        self, *, job_id: str, worker_id: str, fence_token: int,
        completion: JobCompletion,
    ) -> bool: ...

    def split_history_job(
        self, *, job_id: str, worker_id: str, fence_token: int,
        max_depth: int, min_window_seconds: int,
        max_descendant_jobs: int,
    ) -> bool: ...


class _Heartbeat:
    def __init__(
        self, repository: WorkerRepository, queue: QueueTransport,
        queue_class: str, receipt_handle: str, job_id: str, worker_id: str,
        fence_token: int, lease_seconds: int, visibility_seconds: int,
        interval_seconds: int,
    ):
        self._repository = repository
        self._queue = queue
        self._queue_class = queue_class
        self._receipt_handle = receipt_handle
        self._job_id = job_id
        self._worker_id = worker_id
        self._fence_token = fence_token
        self._lease_seconds = lease_seconds
        self._visibility_seconds = visibility_seconds
        self._interval_seconds = interval_seconds
        self._stop = threading.Event()
        self.lost = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        while not self._stop.wait(self._interval_seconds):
            try:
                renewed = self._repository.renew_job_lease(
                    job_id=self._job_id, worker_id=self._worker_id,
                    fence_token=self._fence_token,
                    lease_seconds=self._lease_seconds,
                )
                if not renewed:
                    self.lost.set()
                    return
                self._queue.extend_visibility(
                    self._queue_class, self._receipt_handle, self._visibility_seconds
                )
            except Exception:
                self.lost.set()
                return

    def __enter__(self) -> "_Heartbeat":
        self._thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        self._stop.set()
        self._thread.join()


class QueueWorker:
    def __init__(
        self, repository: WorkerRepository, queue: QueueTransport,
        handler: JobHandler, verifier: EvidenceVerifier, *,
        queue_class: str, worker_id: str,
        allowed_feeds: set[FeedKind],
        slots: int, batch_size: int, wait_seconds: int,
        visibility_seconds: int, lease_seconds: int,
        heartbeat_seconds: int,
        task_protection: TaskProtectionManager | None = None,
        history_split_depth: int = 0,
        history_min_split_window_seconds: int = 30,
        history_max_descendant_jobs: int = 8192,
    ):
        if not queue_class or not worker_id or not allowed_feeds:
            raise ValueError("queue_class, worker_id, and allowed_feeds are required")
        if not (1 <= slots <= 1000 and 1 <= batch_size <= 10):
            raise ValueError("slots and SQS batch_size must be bounded")
        if not (0 <= wait_seconds <= 20 and 1 <= visibility_seconds <= 43200):
            raise ValueError("invalid SQS wait or visibility setting")
        if not (1 <= heartbeat_seconds < min(lease_seconds, visibility_seconds) / 2):
            raise ValueError("heartbeat must be less than half both lease periods")
        self._repository = repository
        self._queue = queue
        self._handler = handler
        self._verifier = verifier
        self._queue_class = queue_class
        self._worker_id = worker_id
        self._allowed_feeds = frozenset(allowed_feeds)
        self._slots = slots
        self._batch_size = batch_size
        self._wait_seconds = wait_seconds
        self._visibility_seconds = visibility_seconds
        self._lease_seconds = lease_seconds
        self._heartbeat_seconds = heartbeat_seconds
        self._task_protection = task_protection
        self._history_split_depth = history_split_depth
        self._history_min_split_window_seconds = history_min_split_window_seconds
        self._history_max_descendant_jobs = history_max_descendant_jobs
        self._stop = threading.Event()

    def _process(self, delivery: Delivery) -> WorkerOutcome:
        try:
            envelope = QueueEnvelope.model_validate_json(delivery.body)
        except ValidationError:
            return WorkerOutcome(None, "invalid_envelope")
        job = self._repository.get_job(envelope.job_id)
        if job is None or (
            job.run_id != envelope.run_id or job.config_hash != envelope.config_hash
        ):
            return WorkerOutcome(envelope.job_id, "unknown_or_mismatched_job")
        if job.feed not in self._allowed_feeds:
            return WorkerOutcome(job.job_id, "wrong_queue_for_feed")
        fence_token = self._repository.acquire_job_lease(
            job_id=job.job_id, worker_id=self._worker_id,
            lease_seconds=self._lease_seconds,
        )
        if fence_token is None:
            status = self._repository.job_status(job.job_id)
            if status in {"certified", "quarantined", "split"}:
                self._queue.delete(self._queue_class, delivery.receipt_handle)
                return WorkerOutcome(
                    job.job_id,
                    "duplicate_acked" if status == "certified" else f"{status}_acked",
                )
            return WorkerOutcome(job.job_id, "busy")

        try:
            with _Heartbeat(
                self._repository, self._queue, self._queue_class,
                delivery.receipt_handle, job.job_id, self._worker_id, fence_token,
                self._lease_seconds, self._visibility_seconds,
                self._heartbeat_seconds,
            ) as heartbeat:
                cancel_events = (heartbeat.lost, self._stop)
                if self._task_protection is not None:
                    cancel_events += (self._task_protection.lost,)
                completion = self._handler.run(job, cancel_events=cancel_events)
                if heartbeat.lost.is_set():
                    return WorkerOutcome(job.job_id, "lease_lost")
                if self._task_protection is not None and self._task_protection.lost.is_set():
                    raise TaskProtectionError("task protection was lost during processing")
                self._verifier.verify(job, completion)
                certified = self._repository.certify_job(
                    job_id=job.job_id, worker_id=self._worker_id,
                    fence_token=fence_token, completion=completion,
                )
                if not certified:
                    return WorkerOutcome(job.job_id, "lease_lost")
            self._queue.delete(self._queue_class, delivery.receipt_handle)
            return WorkerOutcome(job.job_id, "certified")
        except HistorySplitRequired:
            if job.feed != FeedKind.HISTORY:
                self._repository.release_job_lease(
                    job_id=job.job_id, worker_id=self._worker_id,
                    fence_token=fence_token, error_class="UnexpectedHistorySplit",
                )
                return WorkerOutcome(job.job_id, "retry")
            try:
                split = self._repository.split_history_job(
                    job_id=job.job_id, worker_id=self._worker_id,
                    fence_token=fence_token, max_depth=self._history_split_depth,
                    min_window_seconds=self._history_min_split_window_seconds,
                    max_descendant_jobs=self._history_max_descendant_jobs,
                )
            except NonRetryableJobError as exc:
                quarantined = self._repository.quarantine_job(
                    job_id=job.job_id, worker_id=self._worker_id,
                    fence_token=fence_token, error_class=type(exc).__name__,
                )
                if quarantined:
                    self._queue.delete(self._queue_class, delivery.receipt_handle)
                return WorkerOutcome(job.job_id, "quarantined" if quarantined else "lease_lost")
            except Exception as exc:
                self._repository.release_job_lease(
                    job_id=job.job_id, worker_id=self._worker_id,
                    fence_token=fence_token, error_class=type(exc).__name__,
                )
                return WorkerOutcome(job.job_id, "retry")
            if not split:
                return WorkerOutcome(job.job_id, "lease_lost")
            self._queue.delete(self._queue_class, delivery.receipt_handle)
            return WorkerOutcome(job.job_id, "split")
        except NonRetryableJobError as exc:
            quarantined = self._repository.quarantine_job(
                job_id=job.job_id, worker_id=self._worker_id,
                fence_token=fence_token, error_class=type(exc).__name__,
            )
            if not quarantined:
                return WorkerOutcome(job.job_id, "lease_lost")
            self._queue.delete(self._queue_class, delivery.receipt_handle)
            return WorkerOutcome(job.job_id, "quarantined")
        except Exception as exc:
            self._repository.release_job_lease(
                job_id=job.job_id, worker_id=self._worker_id,
                fence_token=fence_token, error_class=type(exc).__name__,
            )
            return WorkerOutcome(job.job_id, "retry")

    def _run_protected(self, delivery: Delivery) -> WorkerOutcome:
        try:
            return self._process(delivery)
        finally:
            if self._task_protection is not None:
                self._task_protection.release()

    def _poll_and_submit(
        self, pool: ThreadPoolExecutor, available: int,
    ) -> list[Future[WorkerOutcome]]:
        protection = self._task_protection
        if protection is not None:
            protection.acquire()
        try:
            deliveries = self._queue.receive(
                self._queue_class,
                max_messages=min(available, self._batch_size),
                wait_seconds=self._wait_seconds,
                visibility_seconds=self._visibility_seconds,
            )
            if self._stop.is_set():
                return []
            submitted: list[Future[WorkerOutcome]] = []
            for delivery in deliveries:
                if protection is not None:
                    protection.acquire()
                try:
                    submitted.append(pool.submit(self._run_protected, delivery))
                except Exception:
                    if protection is not None:
                        protection.release()
                    raise
            return submitted
        finally:
            if protection is not None:
                protection.release()

    def run_once(self) -> tuple[WorkerOutcome, ...]:
        """Poll one bounded batch and wait for its slots to finish."""

        with ThreadPoolExecutor(max_workers=self._slots) as pool:
            pending = self._poll_and_submit(pool, self._slots)
            return tuple(future.result() for future in pending)

    def serve_forever(self, stop: threading.Event) -> None:
        """Continuously refill free slots while prior jobs remain in progress."""

        self._stop = stop
        with ThreadPoolExecutor(max_workers=self._slots) as pool:
            pending: set[Future[WorkerOutcome]] = set()
            while not stop.is_set():
                if self._task_protection is not None and self._task_protection.lost.is_set():
                    raise TaskProtectionError("task protection was lost")
                done = {future for future in pending if future.done()}
                for future in done:
                    future.result()
                pending.difference_update(done)
                available = self._slots - len(pending)
                if available == 0:
                    wait(pending, timeout=1, return_when=FIRST_COMPLETED)
                    continue
                pending.update(self._poll_and_submit(pool, available))
            for future in pending:
                future.result()
