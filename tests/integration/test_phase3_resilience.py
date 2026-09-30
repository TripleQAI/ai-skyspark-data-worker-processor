"""Opt-in physical Phase 3 failure and recovery checks with disposable DB."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from uuid import uuid4

import boto3
import psycopg
import pytest
import yaml

from ingestion.adapters.aws.s3_evidence import S3EvidenceVerifier, S3JsonlSink, S3ObjectStore, S3RawStore
from ingestion.adapters.aws.sqs import SQSMessageSender
from ingestion.adapters.aws.sqs_consumer import SQSQueueTransport
from ingestion.adapters.control.migrations import apply_migrations
from ingestion.adapters.control.postgres import PostgresControlRepository
from ingestion.config.loader import resolve_config
from ingestion.contracts.config import FeedKind
from ingestion.contracts.jobs import JobCompletion, QueueEnvelope
from ingestion.contracts.resources import load_resources
from ingestion.core.planner import plan_run
from ingestion.core.worker import QueueWorker


ROOT = Path(__file__).resolve().parents[2]
DSN = os.environ.get("TEST_CONTROL_DSN")
ENDPOINT = os.environ.get("TEST_LOCALSTACK_ENDPOINT_URL")
pytestmark = pytest.mark.skipif(
    not DSN or not ENDPOINT,
    reason="TEST_CONTROL_DSN and TEST_LOCALSTACK_ENDPOINT_URL are required",
)
RAW_BODY = b'{"rows":[{"source_id":"equip-phase3"}]}'


def _clients():
    if not ENDPOINT.startswith(("http://localhost:", "http://127.0.0.1:")):
        raise ValueError("Phase 3 integration requires a host-local LocalStack endpoint")
    session = boto3.Session(
        aws_access_key_id="test", aws_secret_access_key="test", region_name="us-east-1",
    )
    return session.client("sqs", endpoint_url=ENDPOINT), session.client("s3", endpoint_url=ENDPOINT)


def _one_site_plan(tmp_path):
    binding = yaml.safe_load((ROOT / "config/bindings/example-local.yaml").read_text(encoding="utf-8"))
    binding["project_id"] = f"phase3-{uuid4().hex}"
    binding["approved_sites"] = {"site-a": "demoSiteA"}
    path = tmp_path / "binding.yaml"
    path.write_text(yaml.safe_dump(binding), encoding="utf-8")
    config = resolve_config(
        ROOT / "config/profiles/default.yaml", path,
        ROOT / "local/manifests/synthetic-metadata.yaml", environment="local",
    )
    run, jobs = plan_run(config, FeedKind.METADATA, datetime.now(timezone.utc))
    assert len(jobs) == 1
    return config, run, jobs[0]


def _queue_pair(client, *, max_receive_count):
    name = f"phase3-{uuid4().hex}"
    dlq_name = f"{name}-dlq"
    dlq_url = client.create_queue(QueueName=dlq_name)["QueueUrl"]
    dlq_arn = client.get_queue_attributes(
        QueueUrl=dlq_url, AttributeNames=["QueueArn"],
    )["Attributes"]["QueueArn"]
    url = client.create_queue(
        QueueName=name,
        Attributes={"RedrivePolicy": json.dumps({
            "deadLetterTargetArn": dlq_arn,
            "maxReceiveCount": str(max_receive_count),
        })},
    )["QueueUrl"]
    return name, url, dlq_url


def _delete_objects(client, bucket, prefix):
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            client.delete_object(Bucket=bucket, Key=item["Key"])


def test_worker_kill_after_raw_retries_once_and_fences_stale_attempt(tmp_path):
    resources = load_resources(ROOT / "local/resources.yaml")
    apply_migrations(DSN, ROOT / "migrations/control")
    config, run, job = _one_site_plan(tmp_path)
    sqs, s3 = _clients()
    name, url, dlq_url = _queue_pair(sqs, max_receive_count=resources.max_receive_count)
    prefix = f"{job.tenant_id}/{job.project_id}/metadata/{run.run_id}/{job.job_id}/"
    repository = PostgresControlRepository(DSN)
    try:
        repository.save_plan(config, run, (job,), queue_class=name)
        envelope = QueueEnvelope(job_id=job.job_id, run_id=run.run_id, config_hash=run.config_hash)
        sender = SQSMessageSender(queue_names={name}, region_name=resources.region, client=sqs)
        assert sender.send(name, envelope)
        child_env = os.environ.copy()
        child_env.update({
            "TEST_CONTROL_DSN": DSN,
            "TEST_LOCALSTACK_ENDPOINT_URL": ENDPOINT,
            "PHASE3_QUEUE_NAME": name,
            "AWS_ACCESS_KEY_ID": "test",
            "AWS_SECRET_ACCESS_KEY": "test",
        })
        child = subprocess.run(
            [sys.executable, str(ROOT / "tests/fixtures/phase3_crash_after_raw.py")],
            env=child_env, cwd=ROOT, capture_output=True, text=True, timeout=20,
        )
        assert child.returncode == 74, child.stderr
        assert "RAW_UPLOADED" in child.stdout
        assert repository.job_status(job.job_id) == "running"
        with psycopg.connect(DSN) as conn:
            assert conn.execute(
                "SELECT count(*) FROM ingestion.certifications WHERE job_id = %s",
                (job.job_id,),
            ).fetchone()[0] == 0
            conn.execute(
                "UPDATE ingestion.jobs SET lease_expires_at = now() - interval '1 second' "
                "WHERE job_id = %s", (job.job_id,),
            )
        assert s3.list_objects_v2(
            Bucket=resources.storage.raw_bucket, Prefix=f"raw/{prefix}",
        )["KeyCount"] == 2

        objects = S3ObjectStore(region_name=resources.region, client=s3)
        raw_store = S3RawStore(objects, resources.storage)
        sink = S3JsonlSink(objects, resources.storage)

        class Recover:
            calls = 0
            completion = None

            def run(self, item, *, cancel_events=()):
                self.calls += 1
                self.completion = JobCompletion(
                    raw=raw_store.put(
                        item, (RAW_BODY,), query_id="phase3/crash-test",
                        completed_scope=(item.site_ref,), row_count=1,
                    ),
                    sink=sink.put(item, [{"source_id": "equip-phase3"}]),
                    completed_scope=(item.site_ref,),
                )
                return self.completion

        handler = Recover()
        queue = SQSQueueTransport(queue_names={name}, region_name=resources.region, client=sqs)
        worker = QueueWorker(
            repository, queue, handler, S3EvidenceVerifier(objects, resources.storage),
            queue_class=name, worker_id="phase3-recovery-worker",
            allowed_feeds={FeedKind.METADATA}, slots=1, batch_size=1,
            wait_seconds=1, visibility_seconds=6, lease_seconds=6, heartbeat_seconds=1,
        )
        deadline = time.monotonic() + 15
        outcomes = ()
        while time.monotonic() < deadline and not outcomes:
            outcomes = worker.run_once()
        assert [item.state for item in outcomes] == ["certified"]
        assert handler.calls == 1
        assert repository.job_status(job.job_id) == "certified"
        assert not repository.certify_job(
            job_id=job.job_id, worker_id="phase3-crashed-worker",
            fence_token=1, completion=handler.completion,
        )
        assert not repository.quarantine_job(
            job_id=job.job_id, worker_id="phase3-crashed-worker",
            fence_token=1, error_class="StaleWorker",
        )
        assert s3.list_objects_v2(
            Bucket=resources.storage.raw_bucket, Prefix=f"raw/{prefix}",
        )["KeyCount"] == 2
        assert s3.list_objects_v2(
            Bucket=resources.storage.certified_bucket, Prefix=f"certified/{prefix}",
        )["KeyCount"] == 2
        assert sender.send(name, envelope)
        assert [item.state for item in worker.run_once()] == ["duplicate_acked"]
        assert handler.calls == 1
        with psycopg.connect(DSN) as conn:
            assert conn.execute(
                "SELECT count(*) FROM ingestion.certifications WHERE job_id = %s",
                (job.job_id,),
            ).fetchone()[0] == 1
            assert conn.execute(
                "SELECT outcome FROM ingestion.attempts WHERE job_id = %s ORDER BY attempt_no",
                (job.job_id,),
            ).fetchall() == [("expired",), ("certified",)]
    finally:
        _delete_objects(s3, resources.storage.raw_bucket, f"raw/{prefix}")
        _delete_objects(s3, resources.storage.certified_bucket, f"certified/{prefix}")
        sqs.delete_queue(QueueUrl=url)
        sqs.delete_queue(QueueUrl=dlq_url)


def test_retry_exhaustion_routes_to_localstack_dlq(tmp_path):
    resources = load_resources(ROOT / "local/resources.yaml")
    apply_migrations(DSN, ROOT / "migrations/control")
    config, run, job = _one_site_plan(tmp_path)
    sqs, s3 = _clients()
    name, url, dlq_url = _queue_pair(sqs, max_receive_count=2)
    repository = PostgresControlRepository(DSN)
    prefix = f"{job.tenant_id}/{job.project_id}/metadata/{run.run_id}/{job.job_id}/"
    try:
        repository.save_plan(config, run, (job,), queue_class=name)
        envelope = QueueEnvelope(job_id=job.job_id, run_id=run.run_id, config_hash=run.config_hash)
        SQSMessageSender(queue_names={name}, region_name=resources.region, client=sqs).send(name, envelope)

        class Failing:
            calls = 0

            def run(self, item, *, cancel_events=()):
                self.calls += 1
                raise TimeoutError("fixture source timeout")

        class NoEvidence:
            def verify(self, item, completion):
                raise AssertionError("failure must not produce evidence")

        handler = Failing()
        worker = QueueWorker(
            repository,
            SQSQueueTransport(queue_names={name}, region_name=resources.region, client=sqs),
            handler, NoEvidence(), queue_class=name, worker_id="phase3-failing-worker",
            allowed_feeds={FeedKind.METADATA}, slots=1, batch_size=1,
            wait_seconds=1, visibility_seconds=4, lease_seconds=4, heartbeat_seconds=1,
        )
        dlq_message = None
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline and dlq_message is None:
            worker.run_once()
            messages = sqs.receive_message(
                QueueUrl=dlq_url, MaxNumberOfMessages=1, WaitTimeSeconds=1,
            ).get("Messages", [])
            if messages:
                dlq_message = messages[0]
        assert dlq_message is not None
        assert json.loads(dlq_message["Body"])["job_id"] == job.job_id
        assert handler.calls == 2
        assert repository.job_status(job.job_id) == "planned"
        with psycopg.connect(DSN) as conn:
            assert conn.execute(
                "SELECT outcome, error_class FROM ingestion.attempts "
                "WHERE job_id = %s ORDER BY attempt_no", (job.job_id,),
            ).fetchall() == [("retry", "TimeoutError"), ("retry", "TimeoutError")]
            assert conn.execute(
                "SELECT count(*) FROM ingestion.certifications WHERE job_id = %s",
                (job.job_id,),
            ).fetchone()[0] == 0

        # Operator-style redrive: verify the tiny reference against control
        # state, send it to the original work queue, then delete the DLQ copy.
        redrive_envelope = QueueEnvelope.model_validate_json(dlq_message["Body"])
        assert repository.get_job(redrive_envelope.job_id) == job
        SQSMessageSender(queue_names={name}, region_name=resources.region, client=sqs).send(
            name, redrive_envelope,
        )
        sqs.delete_message(QueueUrl=dlq_url, ReceiptHandle=dlq_message["ReceiptHandle"])
        objects = S3ObjectStore(region_name=resources.region, client=s3)

        class RecoveredSource:
            def run(self, item, *, cancel_events=()):
                return JobCompletion(
                    raw=S3RawStore(objects, resources.storage).put(
                        item, (RAW_BODY,), query_id="phase3/redriven",
                        completed_scope=(item.site_ref,), row_count=1,
                    ),
                    sink=S3JsonlSink(objects, resources.storage).put(
                        item, [{"source_id": "equip-phase3"}],
                    ),
                    completed_scope=(item.site_ref,),
                )

        recovered = QueueWorker(
            repository,
            SQSQueueTransport(queue_names={name}, region_name=resources.region, client=sqs),
            RecoveredSource(), S3EvidenceVerifier(objects, resources.storage),
            queue_class=name, worker_id="phase3-redrive-worker",
            allowed_feeds={FeedKind.METADATA}, slots=1, batch_size=1,
            wait_seconds=2, visibility_seconds=4, lease_seconds=4, heartbeat_seconds=1,
        )
        assert [item.state for item in recovered.run_once()] == ["certified"]
        assert repository.job_status(job.job_id) == "certified"
        with psycopg.connect(DSN) as conn:
            assert conn.execute(
                "SELECT count(*) FROM ingestion.certifications WHERE job_id = %s",
                (job.job_id,),
            ).fetchone()[0] == 1
            assert conn.execute(
                "SELECT count(*) FROM ingestion.attempts WHERE job_id = %s",
                (job.job_id,),
            ).fetchone()[0] == 3
    finally:
        _delete_objects(s3, resources.storage.raw_bucket, f"raw/{prefix}")
        _delete_objects(s3, resources.storage.certified_bucket, f"certified/{prefix}")
        sqs.delete_queue(QueueUrl=url)
        sqs.delete_queue(QueueUrl=dlq_url)


def test_raw_response_near_configured_cap_is_complete_or_rejected(tmp_path):
    resources = load_resources(ROOT / "local/resources.yaml")
    _, _, job = _one_site_plan(tmp_path)
    _, s3 = _clients()
    prefix = f"raw/{job.tenant_id}/{job.project_id}/metadata/{job.run_id}/{job.job_id}/"
    policy = resources.storage.model_copy(update={"max_raw_bytes": 1024})
    store = S3RawStore(S3ObjectStore(region_name=resources.region, client=s3), policy)
    try:
        artifact = store.put(
            job, (b"x" * 1023, b"y"), query_id="phase3/near-cap",
            completed_scope=(job.site_ref,), row_count=1,
        )
        assert artifact.byte_count == policy.max_raw_bytes
        with pytest.raises(ValueError, match="byte cap"):
            store.put(
                job, (b"x" * 1024, b"z"), query_id="phase3/over-cap",
                completed_scope=(job.site_ref,), row_count=1,
            )
        assert s3.list_objects_v2(
            Bucket=resources.storage.raw_bucket, Prefix=prefix,
        )["KeyCount"] == 2
    finally:
        _delete_objects(s3, resources.storage.raw_bucket, prefix)


def test_nonretryable_cap_failure_quarantines_and_acks(tmp_path):
    resources = load_resources(ROOT / "local/resources.yaml")
    apply_migrations(DSN, ROOT / "migrations/control")
    config, run, job = _one_site_plan(tmp_path)
    sqs, s3 = _clients()
    name, url, dlq_url = _queue_pair(sqs, max_receive_count=2)
    repository = PostgresControlRepository(DSN)
    prefix = f"raw/{job.tenant_id}/{job.project_id}/metadata/{run.run_id}/{job.job_id}/"
    try:
        repository.save_plan(config, run, (job,), queue_class=name)
        envelope = QueueEnvelope(job_id=job.job_id, run_id=run.run_id, config_hash=run.config_hash)
        sender = SQSMessageSender(queue_names={name}, region_name=resources.region, client=sqs)
        sender.send(name, envelope)
        tiny_policy = resources.storage.model_copy(update={"max_raw_bytes": 16})
        objects = S3ObjectStore(region_name=resources.region, client=s3)

        class Oversized:
            calls = 0

            def run(self, item, *, cancel_events=()):
                self.calls += 1
                S3RawStore(objects, tiny_policy).put(
                    item, (b"x" * 17,), query_id="phase3/oversized",
                    completed_scope=(item.site_ref,), row_count=1,
                )
                raise AssertionError("oversized upload should fail before writing")

        handler = Oversized()
        worker = QueueWorker(
            repository,
            SQSQueueTransport(queue_names={name}, region_name=resources.region, client=sqs),
            handler, S3EvidenceVerifier(objects, tiny_policy),
            queue_class=name, worker_id="phase3-cap-worker",
            allowed_feeds={FeedKind.METADATA}, slots=1, batch_size=1,
            wait_seconds=1, visibility_seconds=4, lease_seconds=4, heartbeat_seconds=1,
        )
        assert [item.state for item in worker.run_once()] == ["quarantined"]
        assert repository.job_status(job.job_id) == "quarantined"
        sender.send(name, envelope)
        assert [item.state for item in worker.run_once()] == ["quarantined_acked"]
        assert handler.calls == 1
        assert s3.list_objects_v2(
            Bucket=resources.storage.raw_bucket, Prefix=prefix,
        ).get("KeyCount", 0) == 0
        with psycopg.connect(DSN) as conn:
            assert conn.execute(
                "SELECT outcome, error_class FROM ingestion.attempts WHERE job_id = %s",
                (job.job_id,),
            ).fetchone() == ("quarantined", "RawResponseTooLarge")
            assert conn.execute(
                "SELECT count(*) FROM ingestion.certifications WHERE job_id = %s",
                (job.job_id,),
            ).fetchone()[0] == 0
    finally:
        sqs.delete_queue(QueueUrl=url)
        sqs.delete_queue(QueueUrl=dlq_url)
