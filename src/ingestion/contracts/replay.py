"""Explicit operator replay scope; request IDs are stable across retries."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import Field, model_validator

from ingestion.contracts.config import FeedKind, StrictModel


class ReplayRequest(StrictModel):
    request_id: UUID
    tenant_id: str = Field(min_length=1)
    project_id: str = Field(min_length=1)
    config_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    feed: FeedKind
    inventory_version: str = Field(min_length=1)
    window_start: datetime
    window_end: datetime
    requested_at: datetime
    requested_by: str = Field(min_length=1)
    approval_ref: str = Field(min_length=1)
    reason: str = Field(min_length=1, max_length=1000)

    @model_validator(mode="after")
    def valid_window(self) -> "ReplayRequest":
        if self.feed not in (FeedKind.HISTORY, FeedKind.RULES):
            raise ValueError("replay supports only windowed feeds")
        if any(value.tzinfo is None or value.utcoffset() is None for value in (
            self.window_start, self.window_end, self.requested_at,
        )) or not self.window_start < self.window_end <= self.requested_at:
            raise ValueError("replay requires a completed aware window")
        return self
