"""Context and bounded result for registered non-certifying scripts."""

from typing import Literal
from uuid import UUID

from pydantic import Field, field_validator

from ingestion.contracts.config import StrictModel, TargetKind


class UtilityContext(StrictModel):
    schema_version: Literal[1]
    run_id: str = Field(min_length=1)
    script_id: str = Field(min_length=1)
    config_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    target: TargetKind | None = None
    parameters: dict[str, str | int | float | bool | None] = Field(default_factory=dict)

    @field_validator("run_id")
    @classmethod
    def uuid_run_id(cls, value: str) -> str:
        if str(UUID(value)) != value:
            raise ValueError("run_id must be a canonical UUID")
        return value


class UtilityArtifact(StrictModel):
    object_key: str = Field(min_length=1, max_length=2048)
    checksum_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    byte_count: int = Field(ge=0)


class UtilityResult(StrictModel):
    schema_version: Literal[1]
    run_id: str = Field(min_length=1)
    script_id: str = Field(min_length=1)
    artifacts: tuple[UtilityArtifact, ...] = Field(default=(), max_length=1000)
