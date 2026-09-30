"""Persist one reviewed Scheduler trigger and return its durable run scope."""

from __future__ import annotations

from typing import Any

from ingestion.adapters.aws.inventory import S3VersionedInventoryStore
from ingestion.adapters.control.inventory import PostgresInventoryRegistry
from ingestion.adapters.control.postgres import PostgresControlRepository
from ingestion.config.versioned_s3 import S3VersionedConfigStore
from ingestion.contracts.config import FeedKind
from ingestion.contracts.resources import ResourceConfig
from ingestion.contracts.scheduled import ScheduledTrigger
from ingestion.core.scheduled_planner import plan_scheduled_runs, validate_scheduled_trigger


def persist_scheduled_trigger(
    trigger: ScheduledTrigger, *, resources: ResourceConfig,
    environment: str, dsn: str, endpoint_url: str | None = None,
    config_store: Any | None = None, inventory_registry: Any | None = None,
    inventory_store: Any | None = None, repository: Any | None = None,
) -> dict[str, object]:
    """Load pinned inputs, plan idempotently, and expose only a scoped run reference."""
    if environment not in {"local", "aws"}:
        raise ValueError("environment must be local or aws")
    if not dsn:
        raise ValueError("control database DSN is required")
    endpoint = endpoint_url if environment == "local" else None
    config_store = config_store or S3VersionedConfigStore(
        region_name=resources.region,
        max_bytes=resources.config_artifacts.max_bundle_bytes,
        endpoint_url=endpoint,
    )
    config = config_store.load(trigger.config_ref, environment=environment)
    validate_scheduled_trigger(trigger, config)
    inventory = None
    if trigger.feed != FeedKind.METADATA:
        inventory_registry = inventory_registry or PostgresInventoryRegistry(dsn)
        record = inventory_registry.pin_for_due(
            tenant_id=config.binding.tenant_id,
            project_id=config.binding.project_id,
            feed=trigger.feed, scheduled_at=trigger.scheduled_at,
            config_hash=config.config_hash,
            max_age_hours=resources.inventory_artifacts.max_age_hours,
        )
        inventory_store = inventory_store or S3VersionedInventoryStore(
            region_name=resources.region,
            policy=resources.inventory_artifacts,
            endpoint_url=endpoint,
        )
        inventory = inventory_store.load(record, config=config)
    planned = plan_scheduled_runs(trigger, config, inventory=inventory)
    repository = repository or PostgresControlRepository(dsn)
    queue_class = resources.feed_routes[trigger.feed]
    saved = [repository.save_plan(config, run, jobs, queue_class=queue_class)
             for run, jobs in planned]
    return {
        "run_id": saved[0].run_id,
        "tenant_id": config.binding.tenant_id,
        "project_id": config.binding.project_id,
        "config_hash": config.config_hash,
        "feed": trigger.feed.value,
        "expected_jobs": sum(item.expected_jobs for item in saved),
        "new_jobs": sum(item.new_jobs for item in saved),
        "lookback_run_ids": [item.run_id for item in saved[1:]],
        "queue_class": queue_class,
        "inventory_version": planned[0][0].inventory_version,
    }
