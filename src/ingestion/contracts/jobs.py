"""Pinned run/job records and small queue envelopes."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import Field, model_validator

from ingestion.contracts.config import FeedKind, StrictModel


class SiteInventory(StrictModel):
    equipment_ids: tuple[str, ...] = ()
    point_ids: tuple[str, ...] = ()
    historized_point_ids: tuple[str, ...] = ()

    @model_validator(mode="after")
    def unique_ids(self) -> "SiteInventory":
        for name in ("equipment_ids", "point_ids", "historized_point_ids"):
            values = getattr(self, name)
            if len(values) != len(set(values)) or any(not item for item in values):
                raise ValueError(f"{name} must contain unique nonempty IDs")
        if self.point_ids and not set(self.historized_point_ids) <= set(self.point_ids):
            raise ValueError("historized points must be present in point_ids")
        return self


class CertifiedInventory(StrictModel):
    version: str = Field(min_length=1)
    tenant_id: str
    project_id: str
    sites: dict[str, SiteInventory]


class Run(StrictModel):
    run_id: str
    tenant_id: str
    project_id: str
    feed: FeedKind
    scheduled_at: datetime
    window_start: datetime | None = None
    window_end: datetime | None = None
    config_hash: str
    inventory_version: str | None = None


class Job(StrictModel):
    job_id: str
    run_id: str
    tenant_id: str
    project_id: str
    site_ref: str
    feed: FeedKind
    scope_ids: tuple[str, ...]
    window_start: datetime | None = None
    window_end: datetime | None = None
    config_hash: str
    inventory_version: str | None = None


class QueueEnvelope(StrictModel):
    job_id: str
    run_id: str
    config_hash: str
    trace_id: str | None = None


class DispatchIntent(StrictModel):
    job_id: str
    queue_class: str
    envelope: QueueEnvelope
    delivery_attempts: int = Field(ge=1)


class SourceBatch(StrictModel):
    job_id: str
    query_id: str
    requested_ids: tuple[str, ...]
    completed_ids: tuple[str, ...]
    row_count: int = Field(ge=0)
    truncated: bool
    raw_checksum: str


class RawArtifact(StrictModel):
    job_id: str
    object_key: str
    checksum: str
    byte_count: int = Field(ge=0)


class SinkReceipt(StrictModel):
    job_id: str
    sink_kind: str
    batch_key: str
    row_count: int = Field(ge=0)
    checksum: str


class Certification(StrictModel):
    job_id: str
    raw_artifact_key: str
    sink_batch_key: str
    completed_ids: tuple[str, ...]
    certified_at: datetime


class JobCompletion(StrictModel):
    """Evidence supplied only after a handler has written raw and target data."""

    raw: RawArtifact
    sink: SinkReceipt
    completed_scope: tuple[str, ...]


class ScriptProvenance(StrictModel):
    """Source receipt identifiers returned by a reviewed reader."""

    query_ids: tuple[str, ...] = Field(min_length=1, max_length=1000)
    source_artifact_key: str = Field(min_length=1, max_length=2048)


class ScriptArtifactManifestV2(StrictModel):
    """Versioned subprocess result; the worker still verifies physical evidence."""

    schema_version: Literal[2]
    script_id: str = Field(min_length=1)
    config_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    job_id: str = Field(min_length=1)
    provenance: ScriptProvenance
    completion: JobCompletion
