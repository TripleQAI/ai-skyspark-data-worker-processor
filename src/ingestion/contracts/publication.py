"""Small certified-batch references; no source rows or credentials."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class PublicationIntent:
    publication_id: str
    job_id: str
    run_id: str
    tenant_id: str
    project_id: str
    site_ref: str
    feed: str
    config_hash: str
    inventory_version: str | None
    window_start: datetime | None
    window_end: datetime | None
    certified_at: datetime
    raw_key: str
    raw_checksum: str
    sink_kind: str
    batch_key: str
    sink_checksum: str
    row_count: int
    delivery_attempts: int

    def detail(self) -> dict[str, object]:
        return {
            "schema_version": 1,
            "publication_id": self.publication_id,
            "job_id": self.job_id,
            "run_id": self.run_id,
            "tenant_id": self.tenant_id,
            "project_id": self.project_id,
            "site_ref": self.site_ref,
            "feed": self.feed,
            "config_hash": self.config_hash,
            "inventory_version": self.inventory_version,
            "window_start": self.window_start.isoformat() if self.window_start else None,
            "window_end": self.window_end.isoformat() if self.window_end else None,
            "certified_at": self.certified_at.isoformat(),
            "raw": {"object_key": self.raw_key, "checksum": self.raw_checksum},
            "sink": {
                "kind": self.sink_kind,
                "batch_key": self.batch_key,
                "checksum": self.sink_checksum,
                "row_count": self.row_count,
            },
        }


@dataclass(frozen=True, slots=True)
class PublicationReceipt:
    event_id: str | None
    error_class: str | None = None
