"""Small, scope-bound EventBridge Scheduler input contract."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal

from pydantic import Field, field_validator

from ingestion.config.versioned_s3 import parse_versioned_s3_ref
from ingestion.contracts.config import FeedKind, StrictModel


class ScheduledTrigger(StrictModel):
    schema_version: Literal[1]
    tenant_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]*$")
    project_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]*$")
    feed: FeedKind
    profile_id: str = Field(pattern=r"^[a-z][a-z0-9_-]*$")
    config_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    config_ref: str
    scheduled_at: datetime

    @field_validator("config_ref")
    @classmethod
    def pinned_reference(cls, value: str) -> str:
        parse_versioned_s3_ref(value)
        return value

    @field_validator("scheduled_at")
    @classmethod
    def utc_minute(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("scheduled_at must have a timezone")
        utc = value.astimezone(timezone.utc)
        if utc.second or utc.microsecond:
            raise ValueError("scheduled_at must align to a UTC minute")
        return utc
