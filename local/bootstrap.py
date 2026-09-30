"""Create the local AWS resources needed by later integration tests.

This script targets LocalStack only. It does not create production resources.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from ingestion.contracts.resources import load_resources


def main() -> None:
    endpoint = os.environ["LOCALSTACK_ENDPOINT_URL"]
    if not endpoint.startswith(("http://localhost:", "http://127.0.0.1:", "http://localstack:")):
        raise ValueError("local bootstrap requires a LocalStack endpoint")
    resource_file = Path(os.environ["LOCAL_RESOURCE_CONFIG"])
    resources = load_resources(resource_file)
    region = resources.region
    session = boto3.Session(
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name=region,
    )
    sqs = session.client("sqs", endpoint_url=endpoint)
    s3 = session.client("s3", endpoint_url=endpoint)
    events = session.client("events", endpoint_url=endpoint)
    for bucket in resources.buckets:
        create_args: dict[str, object] = {"Bucket": bucket}
        if region != "us-east-1":
            create_args["CreateBucketConfiguration"] = {"LocationConstraint": region}
        s3.create_bucket(**create_args)
        if bucket == resources.storage.certified_bucket:
            s3.put_bucket_versioning(
                Bucket=bucket, VersioningConfiguration={"Status": "Enabled"},
            )
    for name in resources.work_queues:
        dlq_url = sqs.create_queue(QueueName=f"{name}_dlq")["QueueUrl"]
        dlq_arn = sqs.get_queue_attributes(
            QueueUrl=dlq_url, AttributeNames=["QueueArn"]
        )["Attributes"]["QueueArn"]
        sqs.create_queue(
            QueueName=name,
            Attributes={
                "RedrivePolicy": json.dumps(
                    {
                        "deadLetterTargetArn": dlq_arn,
                        "maxReceiveCount": str(resources.max_receive_count),
                    }
                )
            },
        )
    try:
        events.describe_event_bus(Name=resources.publication.event_bus)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") != "ResourceNotFoundException":
            raise
        events.create_event_bus(Name=resources.publication.event_bus)
    print(f"Created {len(resources.buckets)} local buckets, {len(resources.work_queues)} work queues with DLQs, and one publication bus")


if __name__ == "__main__":
    main()
