"""Bounded control-plane polling for outbox and stale-job services."""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import asdict, dataclass
from threading import Event
from typing import Protocol


class BatchResult(Protocol):
    claimed: int


@dataclass(frozen=True, slots=True)
class RecoveryBatch:
    claimed: int
    recovered: int


def serve_control_loop(
    *,
    role: str,
    run_batch: Callable[[], BatchResult],
    stop: Event,
    batch_limit: int,
    idle_seconds: float,
    log: Callable[[str], None] = print,
) -> None:
    """Drain full batches promptly and wait only when there is no full batch.

    Each repository claim is finite. Exceptions escape so the supervisor can
    restart the process, while leases make unfinished claims reclaimable.
    """

    if not role or batch_limit < 1 or not 0 < idle_seconds <= 3600:
        raise ValueError("role, batch_limit, and idle_seconds must be valid")
    while not stop.is_set():
        result = run_batch()
        counts = asdict(result)
        if not 0 <= result.claimed <= batch_limit:
            raise ValueError("control batch exceeded its configured limit")
        if result.claimed:
            log(json.dumps({"role": role, **counts}, sort_keys=True))
        if result.claimed < batch_limit:
            stop.wait(idle_seconds)
