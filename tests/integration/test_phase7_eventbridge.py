"""LocalStack accepts certified publication IDs from the durable outbox."""

from __future__ import annotations

from datetime import datetime, timezone
import os
from pathlib import Path
from uuid import uuid4

import boto3
import psycopg
import pytest
import yaml

from ingestion.adapters.aws.eventbridge import EventBridgePublicationSender
from ingestion.adapters.control.migrations import apply_migrations
from ingestion.adapters.control.postgres import PostgresControlRepository
from ingestion.adapters.control.publications import PostgresPublicationRepository
from ingestion.config.loader import resolve_config_documents
from ingestion.contracts.config import FeedKind
from ingestion.contracts.jobs import JobCompletion, RawArtifact, SinkReceipt
from ingestion.core.planner import plan_run
from ingestion.core.publication import publish_once


ROOT = Path(__file__).resolve().parents[2]
DSN = os.environ.get("TEST_CONTROL_DSN")
ENDPOINT = os.environ.get("TEST_LOCALSTACK_ENDPOINT_URL")
pytestmark = pytest.mark.skipif(not DSN or not ENDPOINT,
                                reason="TEST_CONTROL_DSN and TEST_LOCALSTACK_ENDPOINT_URL are required")


def test_localstack_eventbridge_marks_only_accepted_entries_delivered():
    assert ENDPOINT.startswith(("http://127.0.0.1:", "http://localhost:"))
    apply_migrations(DSN, ROOT / "migrations/control")
    profile = yaml.safe_load((ROOT / "config/profiles/default.yaml").read_text())
    binding = yaml.safe_load((ROOT / "config/bindings/example-local.yaml").read_text())
    binding["tenant_id"] = f"events-{uuid4().hex}"
    binding["project_id"] = f"events-{uuid4().hex}"
    manifest = yaml.safe_load((ROOT / "local/manifests/contract-phase7.yaml").read_text())
    config = resolve_config_documents(profile, binding, manifest, environment="local")
    run, jobs = plan_run(config, FeedKind.METADATA, datetime.now(timezone.utc))
    repo = PostgresControlRepository(DSN)
    repo.save_plan(config, run, jobs, queue_class="metadata_sweep")
    for job in jobs:
        token = repo.acquire_job_lease(job_id=job.job_id, worker_id="phase7-event",
                                       lease_seconds=60)
        assert token is not None
        assert repo.certify_job(
            job_id=job.job_id, worker_id="phase7-event", fence_token=token,
            completion=JobCompletion(
                raw=RawArtifact(job_id=job.job_id, object_key=f"raw/{job.job_id}",
                                checksum="raw", byte_count=1),
                sink=SinkReceipt(job_id=job.job_id, sink_kind="s3",
                                 batch_key=f"batch/{job.job_id}", row_count=0,
                                 checksum="sink"), completed_scope=(job.site_ref,),
            ),
        )
    with psycopg.connect(DSN) as conn:
        conn.execute(
            """UPDATE ingestion.publication_outbox
               SET next_attempt_at = now() + interval '1 hour'
               WHERE state = 'pending' AND job_id <> ALL(%s)""",
            ([job.job_id for job in jobs],),
        )
    client = boto3.Session(aws_access_key_id="test", aws_secret_access_key="test",
                           region_name="us-east-1").client("events", endpoint_url=ENDPOINT)
    bus = f"phase7-{uuid4().hex}"
    client.create_event_bus(Name=bus)
    try:
        result = publish_once(
            PostgresPublicationRepository(DSN),
            EventBridgePublicationSender(
                event_bus=bus, source="insite.skyspark.ingestion",
                detail_type="SkySparkCertifiedBatch", region_name="us-east-1",
                client=client,
            ), owner="phase7-event", limit=10, lease_seconds=60,
            base_backoff_seconds=1, max_backoff_seconds=10,
        )
        assert (result.claimed, result.delivered, result.failed) == (2, 2, 0)
        with psycopg.connect(DSN) as conn:
            rows = conn.execute(
                """SELECT state, event_id FROM ingestion.publication_outbox
                   WHERE job_id = ANY(%s)""", ([job.job_id for job in jobs],),
            ).fetchall()
            assert len(rows) == 2 and all(state == "delivered" and event_id
                                          for state, event_id in rows)
    finally:
        client.delete_event_bus(Name=bus)
