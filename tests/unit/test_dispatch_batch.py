from ingestion.contracts.jobs import DispatchIntent, QueueEnvelope
from ingestion.core.dispatch import DispatchReceipt, dispatch_once


def test_dispatch_groups_by_queue_chunks_of_ten_and_retries_only_failed_entry():
    intents = tuple(
        DispatchIntent(
            job_id=f"job-{index}",
            queue_class="history_live" if index < 23 else "rules_nightly",
            envelope=QueueEnvelope(
                job_id=f"job-{index}", run_id="run", config_hash="hash",
            ),
            delivery_attempts=1,
        )
        for index in range(25)
    )

    class Repository:
        sent = []
        released = []

        def claim_dispatch(self, **kwargs):
            assert kwargs == {"owner": "one", "limit": 25, "lease_seconds": 120}
            return intents

        def mark_dispatched(self, **kwargs):
            self.sent.append(kwargs)
            return True

        def release_dispatch(self, **kwargs):
            self.released.append(kwargs)
            return True

    class Sender:
        calls = []

        def send_batch(self, queue_class, envelopes):
            self.calls.append((queue_class, tuple(e.job_id for e in envelopes)))
            return tuple(
                DispatchReceipt(
                    message_id=None if e.job_id == "job-10" else f"m-{e.job_id}",
                    error_class="ThrottlingException" if e.job_id == "job-10" else None,
                )
                for e in envelopes
            )

    repository, sender = Repository(), Sender()
    result = dispatch_once(
        repository, sender, owner="one", limit=25, lease_seconds=120,
        base_backoff_seconds=5, max_backoff_seconds=300,
    )
    assert (result.claimed, result.sent, result.failed) == (25, 24, 1)
    assert [(queue, len(ids)) for queue, ids in sender.calls] == [
        ("history_live", 10), ("history_live", 10),
        ("history_live", 3), ("rules_nightly", 2),
    ]
    assert repository.released == [{
        "job_id": "job-10", "owner": "one", "delivery_attempts": 1,
        "delay_seconds": 5, "error_class": "ThrottlingException",
    }]
    assert {item["job_id"] for item in repository.sent} == {
        f"job-{index}" for index in range(25)
    } - {"job-10"}
