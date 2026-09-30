import json
from pathlib import Path

import pytest

from ingestion.adapters.aws.sqs import SQSMessageSender
from ingestion.contracts.jobs import QueueEnvelope
from ingestion.contracts.resources import ResourceConfig, load_resources


ROOT = Path(__file__).resolve().parents[2]


def test_local_resources_cover_three_feeds_with_approved_routes():
    resources = load_resources(ROOT / "local/resources.yaml")
    assert set(resources.feed_routes.values()) == {
        "history_live", "rules_nightly", "metadata_sweep"
    }
    assert len(resources.work_queues) == 4


def test_unknown_feed_route_is_rejected():
    resources = load_resources(ROOT / "local/resources.yaml")
    data = resources.model_dump(mode="json")
    data["feed_routes"]["history"] = "unapproved_queue"
    with pytest.raises(ValueError, match="unapproved queue"):
        ResourceConfig.model_validate(data)


def test_stale_recovery_thresholds_are_ordered():
    resources = load_resources(ROOT / "local/resources.yaml")
    data = resources.model_dump(mode="json")
    data["recovery"]["never_started_after_seconds"] = 10
    with pytest.raises(ValueError, match="never-started threshold"):
        ResourceConfig.model_validate(data)


def test_every_feed_requires_a_workflow_deadline():
    resources = load_resources(ROOT / "local/resources.yaml")
    data = resources.model_dump(mode="json")
    del data["workflow"]["max_run_seconds"]["rules"]
    with pytest.raises(ValueError, match="every feed requires"):
        ResourceConfig.model_validate(data)


def test_sqs_sender_sends_only_small_reference_envelopes():
    class FakeClient:
        sent = None

        def get_queue_url(self, **kwargs):
            assert kwargs == {"QueueName": "history_live"}
            return {"QueueUrl": "http://local/queue/history_live"}

        def send_message_batch(self, **kwargs):
            self.sent = kwargs
            return {"Successful": [{"Id": "0", "MessageId": "message-1"}], "Failed": []}

    client = FakeClient()
    sender = SQSMessageSender(
        queue_names={"history_live"}, region_name="us-east-1", client=client
    )
    envelope = QueueEnvelope(job_id="job-1", run_id="run-1", config_hash="hash-1")
    assert sender.send("history_live", envelope) == "message-1"
    assert json.loads(client.sent["Entries"][0]["MessageBody"]) == envelope.model_dump(mode="json")
    assert "scope_ids" not in client.sent["Entries"][0]["MessageBody"]
    with pytest.raises(ValueError, match="unapproved queue"):
        sender.send("backfill", envelope)


def test_sqs_batch_maps_partial_results_and_rejects_ambiguous_responses():
    class FakeClient:
        response = {
            "Successful": [{"Id": "1", "MessageId": "m2"}],
            "Failed": [{"Id": "0", "Code": "ThrottlingException"}],
        }
        calls = 0

        def get_queue_url(self, **kwargs):
            return {"QueueUrl": "http://local/queue/history_live"}

        def send_message_batch(self, **kwargs):
            self.calls += 1
            assert [entry["Id"] for entry in kwargs["Entries"]] == ["0", "1"]
            return self.response

    client = FakeClient()
    sender = SQSMessageSender(
        queue_names={"history_live"}, region_name="us-east-1", client=client,
    )
    envelopes = tuple(
        QueueEnvelope(job_id=f"job-{i}", run_id="run-1", config_hash="hash-1")
        for i in range(2)
    )
    receipts = sender.send_batch("history_live", envelopes)
    assert [(item.message_id, item.error_class) for item in receipts] == [
        (None, "ThrottlingException"), ("m2", None),
    ]
    client.response = {"Successful": [{"Id": "1", "MessageId": "m2"}]}
    with pytest.raises(ValueError, match="missing or unknown"):
        sender.send_batch("history_live", envelopes)
    assert client.calls == 2
