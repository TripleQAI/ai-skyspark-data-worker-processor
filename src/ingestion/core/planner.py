"""Deterministic bounded jobs from certified inventory.

This pure planning slice is reusable by the later PostgreSQL planner. It neither
queries SkySpark nor sends SQS messages, so planning can be tested locally.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone

from ingestion.config.loader import EffectiveConfig
from ingestion.contracts.config import FeedKind
from ingestion.contracts.jobs import CertifiedInventory, Job, Run


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("timestamps must be timezone-aware")
    return value.astimezone(timezone.utc)


def _identity(data: dict[str, object]) -> str:
    body = json.dumps(data, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def _chunks(values: tuple[str, ...], size: int) -> list[tuple[str, ...]]:
    ordered = tuple(sorted(values))
    return [ordered[start : start + size] for start in range(0, len(ordered), size)]


def plan_run(
    config: EffectiveConfig,
    feed_kind: FeedKind,
    scheduled_at: datetime,
    *,
    inventory: CertifiedInventory | None = None,
    window_start: datetime | None = None,
    window_end: datetime | None = None,
) -> tuple[Run, tuple[Job, ...]]:
    """Plan one feed run without crossing approved site or project scope."""

    feed = config.profile.feeds.get(feed_kind)
    if feed is None:
        raise ValueError(f"feed is not enabled: {feed_kind}")
    scheduled_at = _utc(scheduled_at)

    if feed_kind == FeedKind.METADATA:
        if window_start is not None or window_end is not None:
            raise ValueError("metadata does not accept a time window")
    else:
        if window_start is None or window_end is None:
            raise ValueError(f"{feed_kind} requires an explicit time window")
        window_start, window_end = _utc(window_start), _utc(window_end)
        if window_start >= window_end:
            raise ValueError("time window must be nonempty and half-open")
        if feed_kind == FeedKind.HISTORY and (
            window_end - window_start
        ) != timedelta(minutes=feed.partition.window_minutes):
            raise ValueError("history window does not match configured duration")

    if feed_kind != FeedKind.METADATA:
        if inventory is None:
            raise ValueError(f"{feed_kind} requires certified inventory")
        if (inventory.tenant_id, inventory.project_id) != (
            config.binding.tenant_id,
            config.binding.project_id,
        ):
            raise ValueError("inventory tenant/project is outside approved binding")
        unknown = set(inventory.sites) - set(config.binding.approved_sites)
        if unknown:
            raise ValueError(f"inventory has unapproved sites: {sorted(unknown)}")

    run_data = {
        "tenant_id": config.binding.tenant_id,
        "project_id": config.binding.project_id,
        "feed": feed_kind.value,
        "scheduled_at": scheduled_at.isoformat(),
        "window_start": window_start.isoformat() if window_start else None,
        "window_end": window_end.isoformat() if window_end else None,
        "config_hash": config.config_hash,
        "inventory_version": inventory.version if inventory else None,
    }
    run_id = _identity(run_data)
    run = Run(run_id=run_id, **run_data)

    jobs: list[Job] = []
    for site_ref in sorted(config.binding.approved_sites):
        if feed_kind == FeedKind.METADATA:
            groups: list[tuple[str, ...]] = [()]
        else:
            site = inventory.sites.get(site_ref) if inventory else None
            if site is None:
                raise ValueError(f"certified inventory is missing site: {site_ref}")
            ids = (
                site.equipment_ids
                if feed_kind == FeedKind.RULES
                else site.historized_point_ids
            )
            # A site with no eligible IDs still needs a certified result for
            # this window, otherwise its checkpoint can never advance.
            groups = _chunks(ids, feed.partition.max_ids) or [()]
        for group in groups:
            job_data = {
                "run_id": run_id,
                "tenant_id": config.binding.tenant_id,
                "project_id": config.binding.project_id,
                "site_ref": site_ref,
                "feed": feed_kind.value,
                "scope_ids": group,
                "window_start": window_start.isoformat() if window_start else None,
                "window_end": window_end.isoformat() if window_end else None,
                "config_hash": config.config_hash,
                "inventory_version": inventory.version if inventory else None,
            }
            jobs.append(Job(job_id=_identity(job_data), **job_data))
    return run, tuple(jobs)
