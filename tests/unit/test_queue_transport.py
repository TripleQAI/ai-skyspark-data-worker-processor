import pytest

from ingestion.adapters.aws.sqs_consumer import SQSQueueTransport


def test_sqs_transport_uses_approved_queue_and_bounded_receive():
    class FakeClient:
        calls = []

        def get_queue_url(self, **kwargs):
            self.calls.append(("get", kwargs))
            return {"QueueUrl": "http://local/history_live"}

        def receive_message(self, **kwargs):
            self.calls.append(("receive", kwargs))
            return {"Messages": [{"ReceiptHandle": "receipt-1", "Body": "{}"}]}

        def delete_message(self, **kwargs):
            self.calls.append(("delete", kwargs))

        def change_message_visibility(self, **kwargs):
            self.calls.append(("visibility", kwargs))

    client = FakeClient()
    queue = SQSQueueTransport(
        queue_names={"history_live"}, region_name="us-east-1", client=client
    )
    delivery = queue.receive(
        "history_live", max_messages=2, wait_seconds=10,
        visibility_seconds=180,
    )[0]
    assert (delivery.receipt_handle, delivery.body) == ("receipt-1", "{}")
    queue.extend_visibility("history_live", "receipt-1", 180)
    queue.delete("history_live", "receipt-1")
    assert [name for name, _ in client.calls] == [
        "get", "receive", "visibility", "delete"
    ]
    with pytest.raises(ValueError, match="unapproved"):
        queue.receive(
            "rules_nightly", max_messages=1, wait_seconds=0,
            visibility_seconds=30,
        )
    with pytest.raises(ValueError, match="at most ten"):
        queue.receive(
            "history_live", max_messages=11, wait_seconds=0,
            visibility_seconds=30,
        )
