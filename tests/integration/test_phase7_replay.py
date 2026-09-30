"""Audited, idempotent replay uses the bounded backfill route and permits."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
import yaml

from ingestion.adapters.control.migrations import apply_migrations
from ingestion.adapters.control.postgres import PostgresControlRepository
from ingestion.adapters.control.source_permits import PostgresSourcePermitPool
from ingestion.config.loader import resolve_config_documents
from ingestion.contracts.jobs import CertifiedInventory, SiteInventory
from ingestion.contracts.replay import ReplayRequest
from ingestion.contracts.resources import load_resources
from ingestion.core.replay import persist_replay_request


ROOT = Path(__file__).resolve().parents[2]
DSN = os.environ.get("TEST_CONTROL_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="TEST_CONTROL_DSN is unset")


def test_explicit_replay_is_atomic_deduplicated_and_source_limited():
    apply_migrations(DSN, ROOT / "migrations/control")
    profile = yaml.safe_load((ROOT / "config/profiles/default.yaml").read_text())
    binding = yaml.safe_load((ROOT / "config/bindings/example-local.yaml").read_text())
    binding["tenant_id"] = f"replay-{uuid4().hex}"
    binding["project_id"] = f"replay-{uuid4().hex}"
    manifest = yaml.safe_load((ROOT / "local/manifests/contract-phase7.yaml").read_text())
    config = resolve_config_documents(profile, binding, manifest, environment="local")
    inventory = CertifiedInventory(
        version="reviewed-snapshot", tenant_id=binding["tenant_id"],
        project_id=binding["project_id"],
        sites={"site-a": SiteInventory(
            point_ids=("p1", "p2"), historized_point_ids=("p1", "p2"),
        ), "site-b": SiteInventory()},
    )
    resources = load_resources(ROOT / "local/resources.yaml")
    now = datetime.now(timezone.utc).replace(microsecond=0)
    end = now - timedelta(minutes=10)
    request = ReplayRequest(
        request_id=uuid4(), tenant_id=binding["tenant_id"],
        project_id=binding["project_id"], config_hash=config.config_hash,
        feed="history", inventory_version=inventory.version,
        window_start=end - timedelta(minutes=5), window_end=end,
        requested_at=now, requested_by="local-operator",
        approval_ref="reviewed-change-123", reason="late source correction",
    )
    repository = PostgresControlRepository(DSN)
    with pytest.raises(ValueError, match="approval grant"):
        persist_replay_request(
            request, config=config, inventory=inventory, resources=resources,
            repository=repository, now=now,
        )
    with psycopg.connect(DSN) as conn:
        conn.execute(
            """INSERT INTO ingestion.replay_approval_grants
               (approval_ref, tenant_id, project_id, config_hash, feed,
                inventory_version, window_start, window_end, max_jobs,
                approved_by, expires_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (request.approval_ref, request.tenant_id, request.project_id,
             request.config_hash, request.feed.value, request.inventory_version,
             request.window_start, request.window_end, 2, "reviewer",
             now + timedelta(hours=1)),
        )
    first = persist_replay_request(
        request, config=config, inventory=inventory, resources=resources,
        repository=repository, now=now,
    )
    assert first.new_jobs == first.expected_jobs == 2
    second = persist_replay_request(
        request, config=config, inventory=inventory, resources=resources,
        repository=repository, now=now,
    )
    assert second.run_id == first.run_id and second.new_jobs == 0
    with psycopg.connect(DSN) as conn:
        assert conn.execute(
            "SELECT count(*) FROM ingestion.approved_replay_requests WHERE run_id = %s",
            (first.run_id,),
        ).fetchone()[0] == 1
        assert conn.execute(
            """SELECT DISTINCT queue_class FROM ingestion.dispatch_outbox
               WHERE job_id IN (SELECT job_id FROM ingestion.jobs WHERE run_id = %s)""",
            (first.run_id,),
        ).fetchall() == [(resources.backfill_queue,)]
    with pytest.raises(ValueError, match="conflicts with existing audit"):
        persist_replay_request(
            request.model_copy(update={"reason": "different reason"}),
            config=config, inventory=inventory, resources=resources,
            repository=repository, now=now,
        )
    with pytest.raises(ValueError, match="scope differs"):
        persist_replay_request(
            request.model_copy(update={"inventory_version": "wrong-version"}),
            config=config, inventory=inventory, resources=resources,
            repository=repository, now=now,
        )
    backfill = PostgresSourcePermitPool(
        DSN, max_slot_no=resources.backfill.max_source_calls_per_project,
    )
    live = PostgresSourcePermitPool(DSN)
    scope = dict(tenant_id=binding["tenant_id"], project_id=binding["project_id"],
                 job_id="job-replay", lease_seconds=60)
    lease = backfill.try_acquire(**scope, owner="backfill", slots=2)
    assert lease is not None
    assert backfill.try_acquire(**scope, owner="backfill-other", slots=1) is None
    live_lease = live.try_acquire(**scope, owner="live", slots=1)
    assert live_lease is not None
    assert live.release(live_lease) == 1
    assert backfill.release(lease) == 2
