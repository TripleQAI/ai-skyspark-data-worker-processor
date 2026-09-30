"""Candidate equipment rule output with explicit source and revision identity."""

from __future__ import annotations

from datetime import date, datetime

from pydantic import Field

from ingestion.contracts.config import StrictModel


class RuleDetection(StrictModel):
    detection_key: str = Field(pattern=r"^[0-9a-f]{64}$")
    revision_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    tenant_id: str
    project_id: str
    site_ref: str
    equipment_id: str
    rule_id: str
    source_date: date
    source_tz_tag: str
    source_timezone: str
    window_start: datetime
    window_end: datetime
    spark: object
    duration: object | None = None
    periods: object | None = None
    point_ids: tuple[str, ...] = ()
    points: object | None = None
    priority: object | None = None
    severity: object | None = None
    alarm_message_text: str | None = None
    help_message_text: str | None = None
    tags: dict[str, object]
