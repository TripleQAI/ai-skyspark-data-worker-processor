"""Recoverable outbox-to-queue delivery."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from ingestion.contracts.jobs import DispatchIntent, QueueEnvelope


class MessageSender(Protocol):
    def send_batch(
        self, queue_class: str, envelopes: tuple[QueueEnvelope, ...]
    ) -> tuple["DispatchReceipt", ...]: ...


class DispatchRepository(Protocol):
    def claim_dispatch(
        self, *, owner: str, limit: int, lease_seconds: int
    ) -> tuple[DispatchIntent, ...]: ...

    def mark_dispatched(
        self, *, job_id: str, owner: str, delivery_attempts: int, message_id: str
    ) -> bool: ...

    def release_dispatch(
        self, *, job_id: str, owner: str, delivery_attempts: int,
        delay_seconds: int, error_class: str
    ) -> bool: ...


@dataclass(frozen=True, slots=True)
class DispatchResult:
    claimed: int
    sent: int
    failed: int


@dataclass(frozen=True, slots=True)
class DispatchReceipt:
    message_id: str | None
    error_class: str | None


def dispatch_once(
    repository: DispatchRepository,
    sender: MessageSender,
    *,
    owner: str,
    limit: int,
    lease_seconds: int,
    base_backoff_seconds: int,
    max_backoff_seconds: int,
) -> DispatchResult:
    """Send a bounded claimed batch; stale claims can be retried later."""

    if base_backoff_seconds < 1 or max_backoff_seconds < base_backoff_seconds:
        raise ValueError("invalid dispatch backoff policy")
    intents = repository.claim_dispatch(
        owner=owner, limit=limit, lease_seconds=lease_seconds
    )
    grouped: dict[str, list[DispatchIntent]] = {}
    for intent in intents:
        grouped.setdefault(intent.queue_class, []).append(intent)
    sent = failed = 0
    for queue_class, group in grouped.items():
        for start in range(0, len(group), 10):
            batch = group[start:start + 10]
            try:
                receipts = sender.send_batch(
                    queue_class, tuple(intent.envelope for intent in batch)
                )
                if len(receipts) != len(batch):
                    raise ValueError("SQS returned an incomplete batch response")
            except Exception as exc:
                receipts = tuple(
                    DispatchReceipt(message_id=None, error_class=type(exc).__name__)
                    for _ in batch
                )
            for intent, receipt in zip(batch, receipts, strict=True):
                if receipt.message_id and not receipt.error_class:
                    if not repository.mark_dispatched(
                        job_id=intent.job_id, owner=owner,
                        delivery_attempts=intent.delivery_attempts,
                        message_id=receipt.message_id,
                    ):
                        # A sent reference may be resent after a claim timeout;
                        # the worker treats job_id as its idempotency key.
                        raise RuntimeError(f"dispatch claim expired after send: {intent.job_id}")
                    sent += 1
                    continue
                delay = min(
                    max_backoff_seconds,
                    base_backoff_seconds * (2 ** min(intent.delivery_attempts - 1, 20)),
                )
                if not repository.release_dispatch(
                    job_id=intent.job_id, owner=owner,
                    delivery_attempts=intent.delivery_attempts,
                    delay_seconds=delay,
                    error_class=receipt.error_class or "MissingMessageId",
                ):
                    raise RuntimeError(f"dispatch claim expired before retry: {intent.job_id}")
                failed += 1
    return DispatchResult(claimed=len(intents), sent=sent, failed=failed)
