"""EventBridge delivery for certified-batch references."""

from __future__ import annotations

import json
from typing import Any

import boto3

from ingestion.contracts.publication import PublicationIntent, PublicationReceipt


class EventBridgePublicationSender:
    def __init__(
        self, *, event_bus: str, source: str, detail_type: str,
        region_name: str, endpoint_url: str | None = None,
        client: Any | None = None,
    ):
        if not event_bus or not source or not detail_type:
            raise ValueError("event bus, source, and detail type are required")
        self._event_bus = event_bus
        self._source = source
        self._detail_type = detail_type
        self._client = client or boto3.client(
            "events", region_name=region_name, endpoint_url=endpoint_url
        )

    def validate_destination(self) -> None:
        # PutEvents can report success for a missing bus, so check explicitly.
        self._client.describe_event_bus(Name=self._event_bus)

    def send_batch(
        self, intents: tuple[PublicationIntent, ...],
    ) -> tuple[PublicationReceipt, ...]:
        if not 1 <= len(intents) <= 10:
            raise ValueError("EventBridge accepts 1 to 10 entries per batch")
        entries = [
            {
                "EventBusName": self._event_bus,
                "Source": self._source,
                "DetailType": self._detail_type,
                "Detail": json.dumps(
                    intent.detail(), sort_keys=True, separators=(",", ":")
                ),
            }
            for intent in intents
        ]
        response = self._client.put_events(Entries=entries)
        results = response.get("Entries", ())
        if len(results) != len(intents):
            raise ValueError("EventBridge returned an incomplete batch response")
        receipts = tuple(
            PublicationReceipt(
                event_id=item.get("EventId"),
                error_class=item.get("ErrorCode") or (
                    None if item.get("EventId") else "MissingEventId"
                ),
            )
            for item in results
        )
        if response.get("FailedEntryCount", 0) != sum(
            receipt.event_id is None for receipt in receipts
        ):
            raise ValueError("EventBridge failure count conflicts with entries")
        return receipts
