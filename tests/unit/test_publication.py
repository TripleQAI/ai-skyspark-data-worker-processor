"""EventBridge entry mapping and per-entry failure handling."""

from datetime import datetime, timezone
import json
from pathlib import Path

import pytest

from ingestion.adapters.aws.eventbridge import EventBridgePublicationSender
from ingestion.contracts.publication import PublicationIntent
from ingestion.contracts.resources import load_resources
from ingestion.core.publication import publish_once


ROOT = Path(__file__).resolve().parents[2]


def _intent(publication_id: str) -> PublicationIntent:
    now = datetime(2026, 9, 27, tzinfo=timezone.utc)
    return PublicationIntent(
        publication_id=publication_id, job_id=f"job-{publication_id}",
        run_id="run-1", tenant_id="tenant-1", project_id="project-1",
        site_ref="site-1", feed="history", config_hash="hash-1",
        inventory_version="inventory-1", window_start=now,
        window_end=now, certified_at=now, raw_key="raw/key",
        raw_checksum="raw-checksum", sink_kind="timescale",
        batch_key="batch/key", sink_checksum="sink-checksum",
        row_count=3, delivery_attempts=1,
    )


def test_eventbridge_sender_maps_reference_events_and_partial_failure():
    policy = load_resources(ROOT / "local/resources.yaml").publication

    class FakeClient:
        entries = None

        def describe_event_bus(self, **kwargs):
            assert kwargs == {"Name": policy.event_bus}
            return {"Name": policy.event_bus}

        def put_events(self, **kwargs):
            self.entries = kwargs["Entries"]
            return {
                "FailedEntryCount": 1,
                "Entries": [
                    {"EventId": "event-1"},
                    {"ErrorCode": "ThrottlingException", "ErrorMessage": "slow"},
                ],
            }

    client = FakeClient()
    sender = EventBridgePublicationSender(
        event_bus=policy.event_bus, source=policy.source,
        detail_type=policy.detail_type, region_name="us-east-1", client=client,
    )
    sender.validate_destination()
    receipts = sender.send_batch((_intent("publication-1"), _intent("publication-2")))
    assert receipts[0].event_id == "event-1"
    assert receipts[1].error_class == "ThrottlingException"
    assert all(entry["EventBusName"] == policy.event_bus for entry in client.entries)
    detail = json.loads(client.entries[0]["Detail"])
    assert detail["publication_id"] == "publication-1"
    assert detail["sink"]["batch_key"] == "batch/key"
    assert "rows" not in detail
    assert "secret_ref" not in detail


def test_eventbridge_rejects_incomplete_response():
    class BadClient:
        def put_events(self, **_kwargs):
            return {"FailedEntryCount": 0, "Entries": []}

    sender = EventBridgePublicationSender(
        event_bus="bus", source="source", detail_type="type",
        region_name="us-east-1", client=BadClient(),
    )
    with pytest.raises(ValueError, match="incomplete batch"):
        sender.send_batch((_intent("publication-1"),))


def test_missing_destination_prevents_publication_claim():
    class MissingBus:
        def validate_destination(self):
            raise ValueError("event bus is missing")

    class UnusedRepository:
        def claim(self, **_kwargs):
            pytest.fail("must not claim before validating the event bus")

    with pytest.raises(ValueError, match="event bus is missing"):
        publish_once(
            UnusedRepository(), MissingBus(), owner="publisher", limit=10,
            lease_seconds=60, base_backoff_seconds=1,
            max_backoff_seconds=10,
        )
