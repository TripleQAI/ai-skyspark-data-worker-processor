"""Publication outbox recovery and metadata certification in PostgreSQL."""

from dataclasses import replace
from datetime import datetime, timezone
import os
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest

from ingestion.adapters.control.migrations import apply_migrations
from ingestion.adapters.control.postgres import PostgresControlRepository
from ingestion.adapters.control.publications import PostgresPublicationRepository
from ingestion.config.loader import resolve_config
from ingestion.contracts.config import FeedKind
from ingestion.contracts.jobs import JobCompletion, RawArtifact, SinkReceipt
from ingestion.contracts.publication import PublicationReceipt
from ingestion.core.planner import plan_run
from ingestion.core.publication import publish_once


ROOT = Path(__file__).resolve().parents[2]
DSN = os.environ.get("TEST_CONTROL_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="TEST_CONTROL_DSN is unset")


def test_metadata_publication_partial_failure_and_crash_recovery():
    apply_migrations(DSN, ROOT / "migrations/control")
    base = resolve_config(
        ROOT / "config/profiles/default.yaml",
        ROOT / "config/bindings/example-local.yaml",
        ROOT / "config/manifests/plugins.yaml", environment="local",
    )
    unique = uuid4().hex
    config = replace(
        base,
        binding=base.binding.model_copy(update={"project_id": f"publication-{unique}"}),
        config_hash=unique + unique,
    )
    run, jobs = plan_run(config, FeedKind.METADATA, datetime.now(timezone.utc))
    control = PostgresControlRepository(DSN)
    control.save_plan(config, run, jobs, queue_class="metadata_sweep")
    for job in jobs:
        token = control.acquire_job_lease(
            job_id=job.job_id, worker_id="publication-test", lease_seconds=60
        )
        assert token is not None
        assert control.certify_job(
            job_id=job.job_id, worker_id="publication-test", fence_token=token,
            completion=JobCompletion(
                raw=RawArtifact(job_id=job.job_id,
                                object_key=f"raw/{job.job_id}", checksum="raw",
                                byte_count=1),
                sink=SinkReceipt(job_id=job.job_id, sink_kind="s3",
                                 batch_key=f"batch/{job.job_id}", row_count=1,
                                 checksum="sink"),
                completed_scope=(job.site_ref,),
            ),
        )
    with psycopg.connect(DSN) as conn:
        assert conn.execute(
            "SELECT status FROM ingestion.runs WHERE run_id = %s", (run.run_id,)
        ).fetchone()[0] == "certified"
        new_ids = [row[0] for row in conn.execute(
            "SELECT publication_id FROM ingestion.publication_outbox "
            "WHERE job_id = ANY(%s) ORDER BY publication_id",
            ([job.job_id for job in jobs],),
        ).fetchall()]
        assert len(new_ids) == 2
        conn.execute(
            "UPDATE ingestion.publication_outbox "
            "SET next_attempt_at = now() + interval '1 hour' "
            "WHERE state = 'pending' AND publication_id <> ALL(%s)",
            (new_ids,),
        )

    repository = PostgresPublicationRepository(DSN)
    crashed = repository.claim(owner="crashed", limit=2, lease_seconds=60)
    assert len(crashed) == 2
    assert repository.claim(owner="other", limit=2, lease_seconds=60) == ()
    with psycopg.connect(DSN) as conn:
        conn.execute(
            "UPDATE ingestion.publication_outbox "
            "SET claim_until = now() - interval '1 second' "
            "WHERE publication_id = ANY(%s)", (new_ids,),
        )

    class Sender:
        calls = 0
        failed_id = None

        def validate_destination(self):
            pass

        def send_batch(self, intents):
            self.calls += 1
            assert len(intents) == (2 if self.calls == 1 else 1)
            if self.calls == 1:
                assert all(intent.delivery_attempts == 2 for intent in intents)
                self.failed_id = intents[1].publication_id
                return (
                    PublicationReceipt(event_id="event-1"),
                    PublicationReceipt(event_id=None, error_class="ThrottlingException"),
                )
            return (PublicationReceipt(event_id="event-2"),)

    sender = Sender()
    first = publish_once(
        repository, sender, owner="recovery", limit=2, lease_seconds=60,
        base_backoff_seconds=1, max_backoff_seconds=10,
    )
    assert (first.claimed, first.delivered, first.failed) == (2, 1, 1)
    with psycopg.connect(DSN) as conn:
        rows = conn.execute(
            "SELECT publication_id, state, event_id, delivery_attempts, last_error "
            "FROM ingestion.publication_outbox "
            "WHERE publication_id = ANY(%s) ORDER BY publication_id", (new_ids,),
        ).fetchall()
        by_id = {row[0]: row for row in rows}
        assert {row[1] for row in rows} == {"delivered", "pending"}
        assert [row[3] for row in rows] == [2, 2]
        assert by_id[sender.failed_id][1] == "pending"
        assert by_id[sender.failed_id][4] == "ThrottlingException"
        conn.execute(
            "UPDATE ingestion.publication_outbox SET next_attempt_at = now() - interval '1 second' "
            "WHERE publication_id = %s", (sender.failed_id,),
        )
    second = publish_once(
        repository, sender, owner="recovery", limit=2, lease_seconds=60,
        base_backoff_seconds=1, max_backoff_seconds=10,
    )
    assert (second.claimed, second.delivered, second.failed) == (1, 1, 0)
    assert repository.claim(owner="again", limit=2, lease_seconds=60) == ()
    with psycopg.connect(DSN) as conn:
        assert conn.execute(
            "SELECT count(*) FROM ingestion.publication_outbox "
            "WHERE publication_id = ANY(%s) AND state = 'delivered'",
            (new_ids,),
        ).fetchone()[0] == 2
