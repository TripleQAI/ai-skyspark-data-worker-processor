"""Turn a verified scheduled due time into an existing deterministic plan."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from ingestion.config.loader import EffectiveConfig
from ingestion.contracts.config import FeedKind, Schedule
from ingestion.contracts.jobs import CertifiedInventory, Job, Run
from ingestion.contracts.scheduled import ScheduledTrigger
from ingestion.core.planner import plan_run


_WEEKDAYS = ("MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN")


def _rules_day_window(day: date, zone: ZoneInfo) -> tuple[datetime, datetime]:
    start = datetime.combine(day, time.min, tzinfo=zone)
    end = datetime.combine(day + timedelta(days=1), time.min, tzinfo=zone)
    return start.astimezone(timezone.utc), end.astimezone(timezone.utc)


def _previous_due(schedule: Schedule, due: datetime) -> datetime:
    if schedule.kind == "interval":
        minutes = schedule.every_minutes
        if minutes is None or minutes > 60 or 60 % minutes or due.minute % minutes:
            raise ValueError("scheduled time is off the configured interval grid")
        return due - timedelta(minutes=minutes)
    if schedule.at_utc is None or (
        due.hour, due.minute
    ) != (schedule.at_utc.hour, schedule.at_utc.minute):
        raise ValueError("scheduled time does not match configured UTC time")
    if schedule.kind == "daily":
        return due - timedelta(days=1)
    if schedule.day != _WEEKDAYS[due.weekday()]:
        raise ValueError("scheduled time does not match configured weekday")
    return due - timedelta(days=7)


def plan_scheduled_run(
    trigger: ScheduledTrigger,
    config: EffectiveConfig,
    *,
    inventory: CertifiedInventory | None = None,
) -> tuple[Run, tuple[Job, ...]]:
    """Reject stale/cross-project input and plan exactly the due-time window."""
    previous = validate_scheduled_trigger(trigger, config)
    if trigger.feed == FeedKind.METADATA:
        return plan_run(config, trigger.feed, trigger.scheduled_at)
    if trigger.feed == FeedKind.HISTORY:
        lag = timedelta(minutes=config.binding.history_source_lag_minutes)
        return plan_run(
            config, trigger.feed, trigger.scheduled_at,
            inventory=inventory, window_start=previous - lag,
            window_end=trigger.scheduled_at - lag,
        )
    if trigger.feed == FeedKind.RULES:
        zone = ZoneInfo(config.binding.rules_source_timezone)
        settled = trigger.scheduled_at - timedelta(minutes=config.binding.rules_settlement_minutes)
        day = settled.astimezone(zone).date() - timedelta(days=1)
        start, end = _rules_day_window(day, zone)
        return plan_run(
            config, trigger.feed, trigger.scheduled_at,
            inventory=inventory, window_start=start, window_end=end,
        )
    return plan_run(
        config, trigger.feed, trigger.scheduled_at,
        inventory=inventory, window_start=previous, window_end=trigger.scheduled_at,
    )


def plan_scheduled_runs(
    trigger: ScheduledTrigger, config: EffectiveConfig, *,
    inventory: CertifiedInventory | None = None,
) -> tuple[tuple[Run, tuple[Job, ...]], ...]:
    """Plan the due window and bounded prior history windows for late data."""
    primary = plan_scheduled_run(trigger, config, inventory=inventory)
    if trigger.feed not in (FeedKind.HISTORY, FeedKind.RULES):
        return (primary,)
    if trigger.feed == FeedKind.RULES:
        zone = ZoneInfo(config.binding.rules_source_timezone)
        current_day = primary[0].window_start.astimezone(zone).date()
        runs = [primary]
        for offset in range(1, config.binding.rules_lookback_days + 1):
            start, end = _rules_day_window(current_day - timedelta(days=offset), zone)
            runs.append(plan_run(
                config, FeedKind.RULES, trigger.scheduled_at,
                inventory=inventory, window_start=start, window_end=end,
            ))
        return tuple(runs)
    window = timedelta(minutes=config.profile.feeds[FeedKind.HISTORY].partition.window_minutes)
    runs = [primary]
    for offset in range(1, config.binding.history_lookback_windows + 1):
        end = primary[0].window_start - (offset - 1) * window
        start = end - window
        runs.append(plan_run(
            config, FeedKind.HISTORY, trigger.scheduled_at,
            inventory=inventory, window_start=start, window_end=end,
        ))
    return tuple(runs)


def validate_scheduled_trigger(trigger: ScheduledTrigger, config: EffectiveConfig) -> datetime:
    """Validate scope and cadence before looking up durable inventory."""
    if (
        trigger.tenant_id != config.binding.tenant_id
        or trigger.project_id != config.binding.project_id
        or trigger.profile_id != config.profile.profile_id
        or trigger.config_hash != config.config_hash
    ):
        raise ValueError("scheduled input does not match pinned configuration")
    feed = config.profile.feeds.get(trigger.feed)
    if feed is None:
        raise ValueError("scheduled feed is not enabled")
    previous = _previous_due(feed.schedule, trigger.scheduled_at)
    if trigger.feed == FeedKind.HISTORY and (
        trigger.scheduled_at - previous
    ) != timedelta(minutes=feed.partition.window_minutes):
        raise ValueError("history schedule and window lengths differ")
    if trigger.feed == FeedKind.HISTORY and (
        config.binding.history_source_lag_minutes % feed.partition.window_minutes
    ):
        raise ValueError("history source lag must align to the window grid")
    return previous
