"""Plan reviewed history/rules replays onto the isolated backfill queue."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from ingestion.adapters.control.postgres import PlanSaveResult, PostgresControlRepository
from ingestion.config.loader import EffectiveConfig
from ingestion.contracts.config import FeedKind
from ingestion.contracts.jobs import CertifiedInventory
from ingestion.contracts.replay import ReplayRequest
from ingestion.contracts.resources import ResourceConfig
from ingestion.core.planner import plan_run
from ingestion.core.scheduled_planner import _rules_day_window


def persist_replay_request(
    request: ReplayRequest, *, config: EffectiveConfig,
    inventory: CertifiedInventory, resources: ResourceConfig,
    repository: PostgresControlRepository,
    now: datetime | None = None,
) -> PlanSaveResult:
    policy = resources.backfill
    if policy is None or resources.backfill_queue not in resources.work_queues:
        raise ValueError("reviewed backfill policy and queue are required")
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None or current.utcoffset() is None:
        raise ValueError("current time must be timezone-aware")
    if not (current - timedelta(hours=policy.max_request_age_hours)
            <= request.requested_at
            <= current + timedelta(minutes=policy.clock_skew_minutes)):
        raise ValueError("replay request time is outside the reviewed admission window")
    if current - request.window_start > timedelta(days=policy.max_window_age_days):
        raise ValueError("replay window exceeds reviewed lookback age")
    if (request.tenant_id, request.project_id, request.config_hash,
            request.inventory_version) != (
                config.binding.tenant_id, config.binding.project_id,
                config.config_hash, inventory.version,
            ) or (inventory.tenant_id, inventory.project_id) != (
                request.tenant_id, request.project_id,
            ):
        raise ValueError("replay scope differs from pinned configuration or inventory")
    if request.feed == FeedKind.RULES:
        zone = ZoneInfo(config.binding.rules_source_timezone)
        day = request.window_start.astimezone(zone).date()
        if _rules_day_window(day, zone) != (
            request.window_start.astimezone(timezone.utc),
            request.window_end.astimezone(timezone.utc),
        ):
            raise ValueError("rules replay requires one complete source-local day")
    run, jobs = plan_run(
        config, request.feed, request.requested_at,
        inventory=inventory, window_start=request.window_start,
        window_end=request.window_end,
    )
    if len(jobs) > policy.max_jobs_per_request:
        raise ValueError("replay exceeds reviewed job cap")
    return repository.save_plan(
        config, run, jobs, queue_class=resources.backfill_queue,
        replay_request=request,
    )
