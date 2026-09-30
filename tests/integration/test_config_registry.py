"""A shared queue worker must never reuse another project's source binding."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

import pytest

from ingestion.adapters.control.config_registry import PostgresConfigRegistry
from ingestion.adapters.control.migrations import apply_migrations
from ingestion.adapters.control.postgres import PostgresControlRepository
from ingestion.adapters.scripts import ScopedRegisteredScriptHandler
from ingestion.config.loader import resolve_config, resolve_config_documents
from ingestion.contracts.config import FeedKind
from ingestion.core.job_config import ScopedJobConfigResolver
from ingestion.core.planner import plan_run


ROOT = Path(__file__).resolve().parents[2]
DSN = os.environ.get("TEST_CONTROL_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="TEST_CONTROL_DSN is unset")


def _configs():
    first = resolve_config(
        ROOT / "config/profiles/default.yaml",
        ROOT / "config/bindings/example-local.yaml",
        ROOT / "tests/fixtures/script_manifest.yaml",
        environment="local",
    )
    second_binding = first.binding.model_dump(mode="json")
    second_binding["project_id"] = "demo-project-two"
    second = resolve_config_documents(
        first.profile.model_dump(mode="json"), second_binding,
        first.manifest.model_dump(mode="json"), environment="local",
    )
    return first, second


def test_shared_worker_resolves_each_job_config_and_rejects_cross_project_scope():
    apply_migrations(DSN, ROOT / "migrations/control")
    repo = PostgresControlRepository(DSN)
    configs = _configs()
    jobs = []
    for config in configs:
        run, planned = plan_run(
            config, FeedKind.METADATA,
            datetime(2026, 9, 27, 3, tzinfo=timezone.utc),
        )
        repo.save_plan(config, run, planned, queue_class="metadata_sweep")
        jobs.append(planned[0])

    registry = PostgresConfigRegistry(DSN, environment="local")
    resolver = ScopedJobConfigResolver(registry, cache_limit=1)
    handler = ScopedRegisteredScriptHandler(
        resolver, ROOT / "tests/fixtures", cache_limit=1,
    )
    assert [handler.source_slots(job) for job in jobs] == [1, 1]
    assert [handler.run(job).raw.job_id for job in jobs] == [job.job_id for job in jobs]
    assert resolver.for_job(jobs[0]).binding.project_id == configs[0].binding.project_id
    assert resolver.for_job(jobs[1]).binding.project_id == configs[1].binding.project_id
    with pytest.raises(ValueError, match="configuration is missing from job scope"):
        registry.load(
            config_hash=jobs[0].config_hash,
            tenant_id=jobs[0].tenant_id,
            project_id=jobs[1].project_id,
        )
    resolver.for_job(jobs[0])
    with pytest.raises(ValueError, match="outside its stored configuration scope"):
        resolver.for_job(jobs[0].model_copy(update={"project_id": jobs[1].project_id}))
