"""Fenced source-call reservations shared across worker tasks."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class SourcePermitLease:
    tenant_id: str
    project_id: str
    job_id: str
    owner: str
    slots: tuple[tuple[int, int], ...]  # (slot_no, fence_token)
