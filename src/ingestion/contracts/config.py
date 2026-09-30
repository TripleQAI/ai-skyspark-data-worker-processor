"""Strict, immutable configuration contracts.

Configuration describes approved work. It never contains credentials or arbitrary
Python/Axon code supplied by a job message.
"""

from __future__ import annotations

from datetime import time
from enum import StrEnum
from typing import Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)


class FeedKind(StrEnum):
    METADATA = "metadata"
    RULES = "rules"
    HISTORY = "history"


class TargetKind(StrEnum):
    S3 = "s3"
    TIMESCALE = "timescale"


class Schedule(StrictModel):
    kind: Literal["interval", "daily", "weekly"]
    every_minutes: int | None = Field(default=None, ge=1)
    at_utc: time | None = None
    day: Literal["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"] | None = None

    @model_validator(mode="after")
    def validate_shape(self) -> "Schedule":
        if self.kind == "interval":
            if self.every_minutes is None or self.at_utc is not None or self.day is not None:
                raise ValueError("interval needs every_minutes only")
        elif self.kind == "daily":
            if self.at_utc is None or self.every_minutes is not None or self.day is not None:
                raise ValueError("daily needs at_utc only")
        elif self.at_utc is None or self.day is None or self.every_minutes is not None:
            raise ValueError("weekly needs day and at_utc only")
        return self


class PartitionPolicy(StrictModel):
    by: Literal["siteRef"] = "siteRef"
    max_ids: int = Field(ge=1, le=100_000)
    window_minutes: int | None = Field(default=None, ge=1)


class FeedProfile(StrictModel):
    schedule: Schedule
    reader: str = Field(pattern=r"^[a-z][a-z0-9_.-]+@[1-9][0-9]*$")
    validator: str = Field(pattern=r"^[a-z][a-z0-9_.-]+@[1-9][0-9]*$")
    selector: Literal["approved_sites", "all_certified_equipment", "certified_historized_points"]
    partition: PartitionPolicy
    target: TargetKind


class SourcePolicy(StrictModel):
    max_concurrent_calls: int = Field(ge=1, le=10_000)
    max_attempts: int = Field(ge=1, le=100)


class PipelineProfile(StrictModel):
    schema_version: Literal[1]
    profile_id: str = Field(pattern=r"^[a-z][a-z0-9_-]*$")
    source_policy: SourcePolicy
    feeds: dict[FeedKind, FeedProfile] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_feed_shapes(self) -> "PipelineProfile":
        selectors = {
            FeedKind.METADATA: "approved_sites",
            FeedKind.RULES: "all_certified_equipment",
            FeedKind.HISTORY: "certified_historized_points",
        }
        for kind, feed in self.feeds.items():
            if feed.selector != selectors[kind]:
                raise ValueError(f"{kind} requires selector {selectors[kind]}")
            if kind == FeedKind.HISTORY:
                if feed.partition.window_minutes is None:
                    raise ValueError("history requires a partition window")
                if feed.schedule.kind != "interval":
                    raise ValueError("history requires an interval schedule")
            elif feed.partition.window_minutes is not None:
                raise ValueError(f"{kind} does not use a partition window")
        return self


class SourceBinding(StrictModel):
    schema_version: Literal[1]
    profile_id: str
    tenant_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]*$")
    project_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]*$")
    endpoint: HttpUrl
    secret_ref: str = Field(min_length=1)
    approved_sites: dict[str, str] = Field(min_length=1)
    excluded_history_point_ids_by_site: dict[str, tuple[str, ...]] = Field(default_factory=dict)
    history_source_lag_minutes: int = Field(default=0, ge=0, le=1440)
    history_lookback_windows: int = Field(default=0, ge=0, le=12)
    rules_source_timezone: str = "UTC"
    rules_settlement_minutes: int = Field(default=0, ge=0, le=1440)
    rules_lookback_days: int = Field(default=0, ge=0, le=7)
    rules_source_tz_tags: tuple[str, ...] = ("UTC",)

    @field_validator("approved_sites")
    @classmethod
    def validate_sites(cls, sites: dict[str, str]) -> dict[str, str]:
        if any(not site.strip() or not uri.strip() for site, uri in sites.items()):
            raise ValueError("site references and URIs must be nonempty")
        if any("://" in uri or ".." in uri for uri in sites.values()):
            raise ValueError("site URIs must be relative paths without traversal")
        return sites

    @field_validator("secret_ref")
    @classmethod
    def validate_secret_ref(cls, value: str) -> str:
        if not value.startswith(("aws-secretsmanager://", "local-secret://")):
            raise ValueError("secret_ref must be a secret-provider reference")
        return value

    @field_validator("rules_source_timezone")
    @classmethod
    def validate_rules_timezone(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except (ValueError, ZoneInfoNotFoundError) as exc:
            raise ValueError("rules_source_timezone must be an IANA time zone") from exc
        return value

    @model_validator(mode="after")
    def validate_history_exclusions(self) -> "SourceBinding":
        if set(self.excluded_history_point_ids_by_site) - set(self.approved_sites):
            raise ValueError("history exclusions reference an unapproved site")
        for ids in self.excluded_history_point_ids_by_site.values():
            if len(ids) != len(set(ids)) or any(not identifier for identifier in ids):
                raise ValueError("history exclusions require unique nonempty IDs")
        if (not self.rules_source_tz_tags or len(self.rules_source_tz_tags) != len(set(self.rules_source_tz_tags))
                or any(not tag for tag in self.rules_source_tz_tags)):
            raise ValueError("rules_source_tz_tags require unique nonempty tags")
        return self


class ScriptExecution(StrictModel):
    path: str = Field(min_length=4)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    timeout_seconds: int = Field(ge=1, le=3600)
    max_output_bytes: int = Field(ge=256, le=1_048_576)
    env_names: tuple[str, ...] = ()
    optional_env_names: tuple[str, ...] = ()
    source_call_slots: int = Field(default=1, ge=1, le=1000)
    context_transport: Literal["stdin", "file"] = "stdin"
    output_contract: Literal["completion-v1", "artifact-provenance-v2", "utility-result-v1"] = "completion-v1"
    max_context_bytes: int = Field(default=1_048_576, ge=256, le=16_777_216)

    @model_validator(mode="after")
    def reviewed_path_and_env(self) -> "ScriptExecution":
        parts = self.path.split("/")
        if (
            self.path.startswith("/") or "\\" in self.path
            or ":" in self.path
            or any(part in ("", ".", "..") for part in parts)
            or not self.path.endswith(".py")
        ):
            raise ValueError("script path must be a relative .py path within the approved root")
        all_names = self.env_names + self.optional_env_names
        if len(set(all_names)) != len(all_names) or any(
            not name.isidentifier() for name in all_names
        ):
            raise ValueError("script environment names must be unique identifiers")
        return self


class ReaderManifest(StrictModel):
    id: str
    feed: FeedKind
    validators: tuple[str, ...] = Field(min_length=1)
    targets: tuple[TargetKind, ...] = Field(min_length=1)
    execution: ScriptExecution | None = None
    source_capabilities: tuple[FeedKind, ...] = ()
    sink_capabilities: tuple[TargetKind, ...] = ()

    @model_validator(mode="after")
    def validate_capabilities(self) -> "ReaderManifest":
        if self.execution and self.execution.output_contract == "utility-result-v1":
            raise ValueError("utility output cannot be used as an ingestion reader")
        if (self.execution and self.execution.output_contract == "artifact-provenance-v2"
                and self.execution.context_transport != "file"):
            raise ValueError("v2 ingestion readers require file context")
        if len(set(self.source_capabilities)) != len(self.source_capabilities):
            raise ValueError("source capabilities must be unique")
        if len(set(self.sink_capabilities)) != len(self.sink_capabilities):
            raise ValueError("sink capabilities must be unique")
        if self.source_capabilities and self.feed not in self.source_capabilities:
            raise ValueError("reader feed is not an approved source capability")
        if self.sink_capabilities and not set(self.targets) <= set(self.sink_capabilities):
            raise ValueError("reader targets exceed approved sink capabilities")
        return self


class UtilityScriptManifest(StrictModel):
    id: str = Field(pattern=r"^[a-z][a-z0-9_.-]+@[1-9][0-9]*$")
    accepted_context_schema: Literal["utility-context-v1"]
    targets: tuple[TargetKind, ...] = ()
    execution: ScriptExecution

    @model_validator(mode="after")
    def utility_contract(self) -> "UtilityScriptManifest":
        if (self.execution.context_transport != "file"
                or self.execution.output_contract != "utility-result-v1"):
            raise ValueError("utility scripts require file context and utility result contract")
        if len(set(self.targets)) != len(self.targets):
            raise ValueError("utility targets must be unique")
        return self


class PluginManifest(StrictModel):
    schema_version: Literal[1]
    readers: tuple[ReaderManifest, ...] = Field(min_length=1)
    utilities: tuple[UtilityScriptManifest, ...] = ()

    @model_validator(mode="after")
    def unique_readers(self) -> "PluginManifest":
        ids = [item.id for item in self.readers]
        if len(ids) != len(set(ids)):
            raise ValueError("reader IDs must be unique")
        utility_ids = [item.id for item in self.utilities]
        if len(utility_ids) != len(set(utility_ids)) or set(ids) & set(utility_ids):
            raise ValueError("utility IDs must be unique and distinct from readers")
        return self
