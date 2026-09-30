"""Opt-in physical SQS test against a local LocalStack endpoint."""

from __future__ import annotations

import json
import os
from uuid import uuid4

import boto3
import pytest

from ingestion.adapters.aws.sqs import SQSMessageSender
from ingestion.contracts.jobs import QueueEnvelope


ENDPOINT = os.environ.get("TEST_LOCALSTACK_ENDPOINT_URL")
pytestmark = pytest.mark.skipif(not ENDPOINT, reason="TEST_LOCALSTACK_ENDPOINT_URL is unset")


def test_localstack_sqs_batch_preserves_small_job_references():
    if not ENDPOINT.startswith((
        "http://localhost:", "http://127.0.0.1:", "http://localstack:",
    )):
        raise ValueError("test requires a local LocalStack endpoint")
    client = boto3.client(
        "sqs", region_name="us-east-1", endpoint_url=ENDPOINT,
        aws_access_key_id="test", aws_secret_access_key="test",
    )
    queue_name = f"skyspark-dispatch-test-{uuid4().hex}"
    url = client.create_queue(QueueName=queue_name)["QueueUrl"]
    try:
        envelopes = tuple(
            QueueEnvelope(job_id=f"job-{index}", run_id="run", config_hash="hash")
            for index in range(2)
        )
        sender = SQSMessageSender(
            queue_names={queue_name}, region_name="us-east-1", client=client,
        )
        receipts = sender.send_batch(queue_name, envelopes)
        assert all(receipt.message_id and not receipt.error_class for receipt in receipts)
        received = client.receive_message(
            QueueUrl=url, MaxNumberOfMessages=2, WaitTimeSeconds=1,
        ).get("Messages", [])
        assert {json.loads(item["Body"])["job_id"] for item in received} == {
            "job-0", "job-1",
        }
        assert all("scope_ids" not in item["Body"] for item in received)
    finally:
        client.delete_queue(QueueUrl=url)
