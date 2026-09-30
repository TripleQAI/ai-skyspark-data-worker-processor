"""SQS transport for a bounded worker queue."""

from __future__ import annotations

from typing import Any

import boto3

from ingestion.core.worker import Delivery


class SQSQueueTransport:
    def __init__(
        self, *, queue_names: set[str], region_name: str,
        endpoint_url: str | None = None, client: Any | None = None,
    ):
        if not queue_names:
            raise ValueError("at least one approved queue is required")
        self._allowed = frozenset(queue_names)
        self._client = client or boto3.client(
            "sqs", region_name=region_name, endpoint_url=endpoint_url
        )
        self._urls: dict[str, str] = {}

    def _url(self, queue_class: str) -> str:
        if queue_class not in self._allowed:
            raise ValueError(f"unapproved queue class: {queue_class}")
        url = self._urls.get(queue_class)
        if url is None:
            url = self._client.get_queue_url(QueueName=queue_class)["QueueUrl"]
            self._urls[queue_class] = url
        return url

    def receive(
        self, queue_class: str, *, max_messages: int,
        wait_seconds: int, visibility_seconds: int,
    ) -> tuple[Delivery, ...]:
        if not 1 <= max_messages <= 10:
            raise ValueError("SQS receives at most ten messages")
        response = self._client.receive_message(
            QueueUrl=self._url(queue_class), MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=wait_seconds, VisibilityTimeout=visibility_seconds,
        )
        return tuple(
            Delivery(receipt_handle=item["ReceiptHandle"], body=item["Body"])
            for item in response.get("Messages", ())
        )

    def delete(self, queue_class: str, receipt_handle: str) -> None:
        self._client.delete_message(
            QueueUrl=self._url(queue_class), ReceiptHandle=receipt_handle
        )

    def extend_visibility(
        self, queue_class: str, receipt_handle: str, seconds: int
    ) -> None:
        self._client.change_message_visibility(
            QueueUrl=self._url(queue_class), ReceiptHandle=receipt_handle,
            VisibilityTimeout=seconds,
        )
