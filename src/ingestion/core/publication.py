"""Bounded, recoverable publication of certified batch references."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from ingestion.contracts.publication import PublicationIntent, PublicationReceipt


class PublicationRepository(Protocol):
    def claim(
        self, *, owner: str, limit: int, lease_seconds: int,
    ) -> tuple[PublicationIntent, ...]: ...

    def mark_delivered(
        self, *, publication_id: str, owner: str,
        delivery_attempts: int, event_id: str,
    ) -> bool: ...

    def release(
        self, *, publication_id: str, owner: str,
        delivery_attempts: int, delay_seconds: int, error_class: str,
    ) -> bool: ...


class PublicationSender(Protocol):
    def validate_destination(self) -> None: ...

    def send_batch(
        self, intents: tuple[PublicationIntent, ...],
    ) -> tuple[PublicationReceipt, ...]: ...


@dataclass(frozen=True, slots=True)
class PublicationResult:
    claimed: int
    delivered: int
    failed: int


def publish_once(
    repository: PublicationRepository,
    sender: PublicationSender,
    *, owner: str, limit: int, lease_seconds: int,
    base_backoff_seconds: int, max_backoff_seconds: int,
) -> PublicationResult:
    if not owner or limit < 1 or lease_seconds < 1:
        raise ValueError("owner, limit, and lease_seconds must be positive")
    if base_backoff_seconds < 1 or max_backoff_seconds < base_backoff_seconds:
        raise ValueError("invalid publication backoff policy")
    sender.validate_destination()
    intents = repository.claim(owner=owner, limit=limit, lease_seconds=lease_seconds)
    delivered = failed = 0
    for start in range(0, len(intents), 10):
        batch = intents[start:start + 10]
        try:
            receipts = sender.send_batch(batch)
            if len(receipts) != len(batch):
                raise ValueError("publisher returned an incomplete batch")
        except Exception as exc:
            receipts = tuple(
                PublicationReceipt(event_id=None, error_class=type(exc).__name__)
                for _ in batch
            )
        for intent, receipt in zip(batch, receipts, strict=True):
            if receipt.event_id and not receipt.error_class:
                if not repository.mark_delivered(
                    publication_id=intent.publication_id, owner=owner,
                    delivery_attempts=intent.delivery_attempts,
                    event_id=receipt.event_id,
                ):
                    # The event may already be on the bus; a later attempt can
                    # resend it. Consumers must deduplicate by publication_id.
                    raise RuntimeError(
                        f"publication claim expired after send: {intent.publication_id}"
                    )
                delivered += 1
            else:
                delay = min(
                    max_backoff_seconds,
                    base_backoff_seconds * (2 ** min(intent.delivery_attempts - 1, 20)),
                )
                if not repository.release(
                    publication_id=intent.publication_id, owner=owner,
                    delivery_attempts=intent.delivery_attempts,
                    delay_seconds=delay,
                    error_class=receipt.error_class or "MissingEventId",
                ):
                    raise RuntimeError(
                        f"publication claim expired before retry: {intent.publication_id}"
                    )
                failed += 1
    return PublicationResult(claimed=len(intents), delivered=delivered, failed=failed)
