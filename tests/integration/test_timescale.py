"""History batch semantics against local TimescaleDB."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import psycopg
import pytest

from ingestion.adapters.control.migrations import apply_migrations
from ingestion.adapters.db.timescale import TimescaleEvidenceVerifier, TimescaleHistorySink
from ingestion.config.loader import resolve_config
from ingestion.contracts.config import FeedKind
from ingestion.contracts.history import HistoryObservation
from ingestion.contracts.jobs import CertifiedInventory, JobCompletion, RawArtifact
from ingestion.contracts.resources import load_resources
from ingestion.core.planner import plan_run


ROOT = Path(__file__).resolve().parents[2]
DSN = os.environ.get("TEST_TARGET_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="TEST_TARGET_DSN is unset")


def _job():
    config = resolve_config(
        ROOT / "config/profiles/default.yaml",
        ROOT / "config/bindings/example-local.yaml",
        ROOT / "config/manifests/plugins.yaml",
        environment="local",
    )
    inventory = CertifiedInventory.model_validate_json(
        (ROOT / "local/fixtures/inventory.json").read_text(encoding="utf-8")
    )
    end = datetime.now(timezone.utc)
    _, jobs = plan_run(
        config, FeedKind.HISTORY, end, inventory=inventory,
        window_start=end - timedelta(minutes=5), window_end=end,
    )
    return jobs[0]


def test_timescale_history_batch_is_typed_atomic_and_idempotent():
    apply_migrations(DSN, ROOT / "migrations/target", registry="target_schema_migrations")
    assert apply_migrations(
        DSN, ROOT / "migrations/target", registry="target_schema_migrations"
    ) == ()
    job = _job()
    policy = load_resources(ROOT / "local/resources.yaml").storage
    sink = TimescaleHistorySink(
        DSN, policy.model_copy(update={"history_correction_policy": "reject"}),
    )
    ts = job.window_start + timedelta(minutes=1)
    rows = [
        HistoryObservation(point_id=job.scope_ids[0], observed_at=ts, val_bool=False),
        HistoryObservation(
            point_id=job.scope_ids[1], observed_at=ts, val_num=Decimal("23.125")
        ),
    ]
    first = sink.put(job, rows)
    second = sink.put(job, reversed(rows))
    assert first == second
    assert first.row_count == 2
    with psycopg.connect(DSN) as conn:
        assert conn.execute(
            "SELECT count(*) FROM ingestion_target.history_observations "
            "WHERE first_job_id = %s", (job.job_id,),
        ).fetchone()[0] == 2
        assert conn.execute(
            "SELECT count(*) FROM ingestion_target.batch_receipts WHERE job_id = %s",
            (job.job_id,),
        ).fetchone()[0] == 1
        assert conn.execute(
            "SELECT count(*) FROM timescaledb_information.hypertables "
            "WHERE hypertable_schema = 'ingestion_target' "
            "AND hypertable_name = 'history_observations'"
        ).fetchone()[0] == 1

    class RawVerifier:
        called = False

        def verify_raw(self, job, completion):
            self.called = True

    raw_verifier = RawVerifier()
    completion = JobCompletion(
        raw=RawArtifact(
            job_id=job.job_id, object_key="raw/test", checksum="x", byte_count=1
        ),
        sink=first, completed_scope=job.scope_ids,
    )
    TimescaleEvidenceVerifier(DSN, raw_verifier).verify(job, completion)
    assert raw_verifier.called

    corrected = [
        HistoryObservation(point_id=job.scope_ids[0], observed_at=ts, val_bool=True),
        rows[1],
    ]
    with pytest.raises(ValueError, match="explicit correction policy"):
        sink.put(job, corrected)
    with psycopg.connect(DSN) as conn:
        assert conn.execute(
            "SELECT count(*) FROM ingestion_target.batch_receipts WHERE job_id = %s",
            (job.job_id,),
        ).fetchone()[0] == 1

    append_sink = TimescaleHistorySink(
        DSN, policy.model_copy(update={"history_correction_policy": "append_revision"}),
    )
    revision = append_sink.put(job, corrected)
    assert revision.row_count == 2
    assert append_sink.put(job, corrected) == revision
    with psycopg.connect(DSN) as conn:
        assert conn.execute(
            "SELECT count(*) FROM ingestion_target.history_observation_revisions "
            "WHERE tenant_id = %s AND project_id = %s AND point_id = %s "
            "AND observed_at = %s",
            (job.tenant_id, job.project_id, job.scope_ids[0], ts),
        ).fetchone()[0] == 1


def test_timescale_rejects_out_of_window_and_duplicate_samples():
    job = _job()
    sink = TimescaleHistorySink(
        DSN, load_resources(ROOT / "local/resources.yaml").storage
    )
    outside = HistoryObservation(
        point_id=job.scope_ids[0], observed_at=job.window_end, val_num=Decimal("1")
    )
    with pytest.raises(ValueError, match="outside requested"):
        sink.put(job, [outside])
    inside = outside.model_copy(update={"observed_at": job.window_start})
    with pytest.raises(ValueError, match="duplicate point/timestamp"):
        sink.put(job, [inside, inside])
