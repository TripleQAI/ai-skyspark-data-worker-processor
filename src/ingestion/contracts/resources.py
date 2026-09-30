"""Reviewed resource names and bounded dispatcher policy."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import yaml
from pydantic import Field, model_validator

from ingestion.contracts.config import FeedKind, StrictModel


class DispatchPolicy(StrictModel):
    batch_limit: int = Field(ge=1, le=1000)
    lease_seconds: int = Field(ge=1)
    base_backoff_seconds: int = Field(ge=1)
    max_backoff_seconds: int = Field(ge=1)

    @model_validator(mode="after")
    def valid_backoff(self) -> "DispatchPolicy":
        if self.max_backoff_seconds < self.base_backoff_seconds:
            raise ValueError("max_backoff_seconds must be at least base_backoff_seconds")
        return self


class WorkerPolicy(StrictModel):
    job_slots: int = Field(ge=1, le=1000)
    config_cache_entries: int = Field(default=128, ge=1, le=10000)
    sqs_batch_size: int = Field(ge=1, le=10)
    long_poll_seconds: int = Field(ge=0, le=20)
    visibility_seconds: int = Field(ge=1, le=43200)
    lease_seconds: int = Field(ge=1)
    heartbeat_seconds: int = Field(ge=1)
    source_permit_lease_seconds: int = Field(ge=1)
    source_permit_heartbeat_seconds: int = Field(ge=1)
    source_permit_retry_seconds: float = Field(gt=0, le=60)
    scale_in_protection: "ScaleInProtectionPolicy"

    @model_validator(mode="after")
    def heartbeat_fits_leases(self) -> "WorkerPolicy":
        if self.heartbeat_seconds >= min(
            self.visibility_seconds, self.lease_seconds
        ) / 2:
            raise ValueError("heartbeat must be less than half both lease periods")
        if self.source_permit_heartbeat_seconds >= self.source_permit_lease_seconds / 2:
            raise ValueError("source permit heartbeat must fit its lease")
        return self


class ScaleInProtectionPolicy(StrictModel):
    enabled: bool
    expires_minutes: int = Field(ge=1, le=2880)
    refresh_seconds: int = Field(ge=1, le=3600)
    http_timeout_seconds: float = Field(gt=0, le=10)

    @model_validator(mode="after")
    def refresh_before_expiry(self) -> "ScaleInProtectionPolicy":
        if self.refresh_seconds >= self.expires_minutes * 30:
            raise ValueError("task protection refresh must be less than half its expiry")
        return self


class StoragePolicy(StrictModel):
    raw_bucket: str = Field(min_length=1)
    certified_bucket: str = Field(min_length=1)
    max_raw_bytes: int = Field(ge=1, le=5_000_000_000)
    max_certified_bytes: int = Field(ge=1, le=5_000_000_000)
    max_certified_rows: int = Field(ge=1)
    history_correction_policy: Literal["reject", "append_revision"] = "reject"


class PublicationPolicy(DispatchPolicy):
    event_bus: str = Field(min_length=1)
    source: str = Field(min_length=1)
    detail_type: str = Field(min_length=1)


class RecoveryPolicy(StrictModel):
    batch_limit: int = Field(ge=1, le=1000)
    expired_running_after_seconds: int = Field(ge=1)
    never_started_after_seconds: int = Field(ge=1)
    max_redrives: int = Field(ge=1)

    @model_validator(mode="after")
    def ordered_thresholds(self) -> "RecoveryPolicy":
        if self.never_started_after_seconds < self.expired_running_after_seconds:
            raise ValueError("never-started threshold must be at least expired-running threshold")
        return self


class BackfillPolicy(StrictModel):
    max_source_calls_per_project: int = Field(ge=1, le=1000)
    job_slots_per_task: int = Field(ge=1, le=1000)
    max_jobs_per_request: int = Field(ge=1, le=1_000_000)
    max_window_age_days: int = Field(ge=1, le=3650)
    max_request_age_hours: int = Field(ge=1, le=168)
    clock_skew_minutes: int = Field(ge=0, le=60)


class RetentionPolicy(StrictModel):
    raw_object_days: int = Field(ge=1, le=3650)
    certified_object_days: int = Field(ge=1, le=3650)
    inventory_object_days: int = Field(ge=1, le=3650)
    history_observation_days: int = Field(ge=1, le=3650)


class MeasurementPolicy(StrictModel):
    max_s3_objects_per_prefix: int = Field(ge=1, le=1_000_000)


class MetadataInspectionPolicy(StrictModel):
    max_csv_bytes: int = Field(ge=1)
    max_equipment_rows: int = Field(ge=1)
    max_point_rows: int = Field(ge=1)
    max_issue_examples: int = Field(ge=0, le=1000)
    historized_true_values: tuple[str, ...] = Field(min_length=1)
    historized_false_values: tuple[str, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def distinct_marker_values(self) -> "MetadataInspectionPolicy":
        truthy = {value.strip().casefold() for value in self.historized_true_values}
        falsy = {value.strip().casefold() for value in self.historized_false_values}
        if truthy & falsy:
            raise ValueError("historized marker sets overlap")
        return self


class MetadataReadPolicy(StrictModel):
    max_pages_per_kind: int = Field(ge=1, le=10000)
    max_rows_per_page: int = Field(ge=1)
    max_entities_per_site: int = Field(ge=1)
    max_page_bytes: int = Field(ge=1)
    request_timeout_seconds: float = Field(gt=0, le=300)


class HistoryReadPolicy(StrictModel):
    max_ids: int = Field(ge=1, le=100000)
    max_rows: int = Field(ge=1)
    max_observations: int = Field(ge=1)
    max_response_bytes: int = Field(ge=1)
    request_timeout_seconds: float = Field(gt=0, le=300)
    max_split_depth: int = Field(default=12, ge=0, le=32)
    max_descendant_jobs: int = Field(default=8192, ge=2, le=1_000_000)
    min_split_window_seconds: int = Field(default=30, ge=1)


class RulesReadPolicy(StrictModel):
    max_ids: int = Field(ge=1, le=100000)
    max_rows: int = Field(ge=1)
    max_response_bytes: int = Field(ge=1)
    request_timeout_seconds: float = Field(gt=0, le=300)


class ConfigArtifactPolicy(StrictModel):
    max_bundle_bytes: int = Field(ge=1, le=16_777_216)


class InventoryArtifactPolicy(StrictModel):
    max_snapshot_bytes: int = Field(ge=1, le=1_073_741_824)
    max_age_hours: int = Field(ge=1, le=8760)


class WorkflowPolicy(StrictModel):
    poll_seconds: int = Field(ge=1, le=3600)
    max_run_seconds: dict[FeedKind, int]

    @model_validator(mode="after")
    def complete_deadlines(self) -> "WorkflowPolicy":
        if set(self.max_run_seconds) != set(FeedKind):
            raise ValueError("every feed requires a workflow deadline")
        if any(value < self.poll_seconds for value in self.max_run_seconds.values()):
            raise ValueError("workflow deadline must be at least one poll interval")
        return self


class ControlLoopPolicy(StrictModel):
    dispatch_idle_seconds: float = Field(gt=0, le=3600)
    publication_idle_seconds: float = Field(gt=0, le=3600)
    recovery_idle_seconds: float = Field(gt=0, le=3600)


class ResourceConfig(StrictModel):
    region: str = Field(min_length=1)
    buckets: tuple[str, ...] = Field(min_length=1)
    work_queues: tuple[str, ...] = Field(min_length=1)
    backfill_queue: str = Field(min_length=1)
    feed_routes: dict[FeedKind, str]
    max_receive_count: int = Field(ge=1)
    dispatch: DispatchPolicy
    publication: PublicationPolicy
    recovery: RecoveryPolicy
    backfill: BackfillPolicy | None = None
    retention: RetentionPolicy | None = None
    measurement: MeasurementPolicy | None = None
    metadata_inspection: MetadataInspectionPolicy
    metadata_read: MetadataReadPolicy
    history_read: HistoryReadPolicy | None = None
    rules_read: RulesReadPolicy | None = None
    config_artifacts: ConfigArtifactPolicy
    inventory_artifacts: InventoryArtifactPolicy
    workflow: WorkflowPolicy
    control_loop: ControlLoopPolicy
    worker: WorkerPolicy
    storage: StoragePolicy

    @model_validator(mode="after")
    def approved_routes(self) -> "ResourceConfig":
        if len(set(self.work_queues)) != len(self.work_queues):
            raise ValueError("work_queues must be unique")
        if set(self.feed_routes) != set(FeedKind):
            raise ValueError("every feed requires one queue route")
        if set(self.feed_routes.values()) - set(self.work_queues):
            raise ValueError("feed route references an unapproved queue")
        if self.backfill_queue not in self.work_queues:
            raise ValueError("backfill queue is not approved")
        if {self.storage.raw_bucket, self.storage.certified_bucket} - set(self.buckets):
            raise ValueError("storage bucket is not approved")
        return self


def load_resources(path: Path) -> ResourceConfig:
    with path.open("r", encoding="utf-8") as stream:
        return ResourceConfig.model_validate(yaml.safe_load(stream))
