"""Public protocol catalogue for the reusable worker pipeline."""

from ingestion.core.dispatch import DispatchRepository, MessageSender
from ingestion.core.source_gate import SourcePermitPool, SourceRunner
from ingestion.core.worker import (
    EvidenceVerifier, JobHandler, QueueTransport, WorkerRepository,
)

__all__ = [
    "DispatchRepository", "MessageSender", "SourcePermitPool", "SourceRunner",
    "EvidenceVerifier", "JobHandler", "QueueTransport", "WorkerRepository",
]
