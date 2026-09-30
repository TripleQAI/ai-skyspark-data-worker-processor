"""SQS sender for small job-reference envelopes."""

from __future__ import annotations

import json
from typing import Any

import boto3

from ingestion.contracts.jobs import QueueEnvelope
from ingestion.core.dispatch import DispatchReceipt


class SQSMessageSender:
    def __init__(
        self,
        *,
        queue_names: set[str],
        region_name: str,
        endpoint_url: str | None = None,
        client: Any | None = None,
    ):
        if not queue_names:
            raise ValueError("at least one approved queue name is required")
        self._allowed = frozenset(queue_names)
        self._client = client or boto3.client(
            "sqs", region_name=region_name, endpoint_url=endpoint_url
        )
        self._urls: dict[str, str] = {}

    def _queue_url(self, queue_class: str) -> str:
        if queue_class not in self._allowed:
            raise ValueError(f"unapproved queue class: {queue_class}")
        url = self._urls.get(queue_class)
        if url is None:
            url = self._client.get_queue_url(QueueName=queue_class)["QueueUrl"]
            self._urls[queue_class] = url
        return url

    def send_batch(
        self, queue_class: str, envelopes: tuple[QueueEnvelope, ...]
    ) -> tuple[DispatchReceipt, ...]:
        if not 1 <= len(envelopes) <= 10:
            raise ValueError("SQS batch must contain 1 to 10 references")
        url = self._queue_url(queue_class)
        response = self._client.send_message_batch(
            QueueUrl=url,
            Entries=[
                {
                    "Id": str(index),
                    "MessageBody": json.dumps(
                        envelope.model_dump(mode="json"),
                        sort_keys=True, separators=(",", ":"),
                    ),
                }
                for index, envelope in enumerate(envelopes)
            ],
        )
        by_id: dict[str, DispatchReceipt] = {}
        for item in response.get("Successful", ()):
            entry_id = item["Id"]
            if entry_id in by_id:
                raise ValueError("SQS returned a duplicate batch ID")
            by_id[entry_id] = DispatchReceipt(
                message_id=item.get("MessageId"),
                error_class=None if item.get("MessageId") else "MissingMessageId",
            )
        for item in response.get("Failed", ()):
            entry_id = item["Id"]
            if entry_id in by_id:
                raise ValueError("SQS returned a duplicate batch ID")
            by_id[entry_id] = DispatchReceipt(
                message_id=None, error_class=item.get("Code") or "UnknownSQSFailure",
            )
        expected = {str(index) for index in range(len(envelopes))}
        if set(by_id) != expected:
            raise ValueError("SQS returned missing or unknown batch IDs")
        return tuple(by_id[str(index)] for index in range(len(envelopes)))

    def send(self, queue_class: str, envelope: QueueEnvelope) -> str:
        """Compatibility helper for a single reference."""
        receipt = self.send_batch(queue_class, (envelope,))[0]
        if not receipt.message_id or receipt.error_class:
            raise RuntimeError(receipt.error_class or "MissingMessageId")
        return receipt.message_id
