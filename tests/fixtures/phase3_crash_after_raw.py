"""Test-only worker process that dies after durable raw upload."""

from __future__ import annotations

import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

import boto3  # noqa: E402

from ingestion.adapters.aws.s3_evidence import S3EvidenceVerifier, S3ObjectStore, S3RawStore  # noqa: E402
from ingestion.adapters.aws.sqs_consumer import SQSQueueTransport  # noqa: E402
from ingestion.adapters.control.postgres import PostgresControlRepository  # noqa: E402
from ingestion.contracts.config import FeedKind  # noqa: E402
from ingestion.contracts.resources import load_resources  # noqa: E402
from ingestion.core.worker import QueueWorker  # noqa: E402


RAW_BODY = b'{"rows":[{"source_id":"equip-phase3"}]}'


def main() -> None:
    resources = load_resources(ROOT / "local/resources.yaml")
    endpoint = os.environ["TEST_LOCALSTACK_ENDPOINT_URL"]
    queue_name = os.environ["PHASE3_QUEUE_NAME"]
    session = boto3.Session(
        aws_access_key_id="test", aws_secret_access_key="test",
        region_name=resources.region,
    )
    objects = S3ObjectStore(
        region_name=resources.region,
        client=session.client("s3", endpoint_url=endpoint),
    )

    class CrashAfterRaw:
        def run(self, job, *, cancel_events=()):
            S3RawStore(objects, resources.storage).put(
                job, (RAW_BODY,), query_id="phase3/crash-test",
                completed_scope=(job.site_ref,), row_count=1,
            )
            print("RAW_UPLOADED", flush=True)
            os._exit(74)

    queue = SQSQueueTransport(
        queue_names={queue_name}, region_name=resources.region,
        client=session.client("sqs", endpoint_url=endpoint),
    )
    worker = QueueWorker(
        PostgresControlRepository(os.environ["TEST_CONTROL_DSN"]),
        queue, CrashAfterRaw(), S3EvidenceVerifier(objects, resources.storage),
        queue_class=queue_name, worker_id="phase3-crashed-worker",
        allowed_feeds={FeedKind.METADATA}, slots=1, batch_size=1,
        wait_seconds=2, visibility_seconds=6, lease_seconds=6,
        heartbeat_seconds=1,
    )
    if not worker.run_once():
        raise RuntimeError("test worker did not receive its job reference")


if __name__ == "__main__":
    main()
