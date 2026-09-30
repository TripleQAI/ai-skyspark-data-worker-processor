"""Deterministic, reviewed EventBridge Scheduler definitions.

This module only builds schedules. It does not contact AWS or run a planner.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any

from ingestion.config.loader import EffectiveConfig
from ingestion.config.versioned_s3 import parse_versioned_s3_ref
from ingestion.contracts.config import FeedKind, Schedule


_NAME = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")
_STATE_MACHINE_ARN = re.compile(
    r"^arn:aws(?:-[a-z]+)?:states:[a-z0-9-]+:\d{12}:stateMachine:[A-Za-z0-9_.-]+(?::[A-Za-z0-9_.-]+)?$"
)
_ROLE_ARN = re.compile(r"^arn:aws(?:-[a-z]+)?:iam::\d{12}:role/[A-Za-z0-9+=,.@_/-]+$")
_QUEUE_ARN = re.compile(r"^arn:aws(?:-[a-z]+)?:sqs:[a-z0-9-]+:\d{12}:[A-Za-z0-9_-]+$")
_MANAGED_DESCRIPTION = "insite-skyspark-schedule-v1"


@dataclass(frozen=True, slots=True)
class ScheduleSpec:
    name: str
    group_name: str
    feed: FeedKind
    request: dict[str, Any]

    def summary(self) -> dict[str, Any]:
        return dict(self.request)


def _expression(schedule: Schedule) -> str:
    if schedule.kind == "interval":
        minutes = schedule.every_minutes
        if minutes is None or minutes > 60 or 60 % minutes:
            raise ValueError("interval must divide 60 minutes for UTC-aligned windows")
        return "cron(0 * * * ? *)" if minutes == 60 else f"cron(0/{minutes} * * * ? *)"
    assert schedule.at_utc is not None
    if schedule.at_utc.second or schedule.at_utc.microsecond:
        raise ValueError("AWS schedule times must be minute aligned")
    minute, hour = schedule.at_utc.minute, schedule.at_utc.hour
    if schedule.kind == "daily":
        return f"cron({minute} {hour} * * ? *)"
    assert schedule.day is not None
    return f"cron({minute} {hour} ? * {schedule.day} *)"


def build_schedule_specs(
    config: EffectiveConfig,
    *,
    config_ref: str,
    group_name: str,
    state_machine_arn: str,
    role_arn: str,
    dead_letter_arn: str,
    enabled: bool = False,
) -> tuple[ScheduleSpec, ...]:
    """Build one schedule per configured feed, with a pinned config reference.

    The caller provisions the group, role, state machine and DLQ separately.
    Disabled is the safe initial state until those resources are verified.
    """
    if not _NAME.fullmatch(group_name):
        raise ValueError("invalid Scheduler group name")
    if not _STATE_MACHINE_ARN.fullmatch(state_machine_arn):
        raise ValueError("invalid Step Functions state machine ARN")
    if not _ROLE_ARN.fullmatch(role_arn):
        raise ValueError("invalid Scheduler execution role ARN")
    if not _QUEUE_ARN.fullmatch(dead_letter_arn):
        raise ValueError("invalid Scheduler dead-letter queue ARN")
    try:
        parse_versioned_s3_ref(config_ref)
    except ValueError as error:
        raise ValueError("config_ref must be a version-pinned S3 URI") from error

    specs: list[ScheduleSpec] = []
    for feed in sorted(config.profile.feeds, key=lambda value: value.value):
        identity = f"{config.binding.tenant_id}/{config.binding.project_id}/{feed.value}"
        suffix = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:20]
        name = f"skyspark-{feed.value}-{suffix}"
        target_input = {
            "schema_version": 1,
            "tenant_id": config.binding.tenant_id,
            "project_id": config.binding.project_id,
            "feed": feed.value,
            "profile_id": config.profile.profile_id,
            "config_hash": config.config_hash,
            "config_ref": config_ref,
            "scheduled_at": "<aws.scheduler.scheduled-time>",
        }
        request = {
            "Name": name,
            "GroupName": group_name,
            "Description": _MANAGED_DESCRIPTION,
            "ScheduleExpression": _expression(config.profile.feeds[feed].schedule),
            "ScheduleExpressionTimezone": "UTC",
            "FlexibleTimeWindow": {"Mode": "OFF"},
            "State": "ENABLED" if enabled else "DISABLED",
            "Target": {
                "Arn": state_machine_arn,
                "RoleArn": role_arn,
                "Input": json.dumps(target_input, sort_keys=True, separators=(",", ":")),
                "DeadLetterConfig": {"Arn": dead_letter_arn},
            },
        }
        specs.append(ScheduleSpec(name, group_name, feed, request))
    return tuple(specs)
