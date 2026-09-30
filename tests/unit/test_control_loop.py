import json
from pathlib import Path
from threading import Event

import pytest

from ingestion.contracts.resources import ResourceConfig, load_resources
from ingestion.core.control_loop import RecoveryBatch, serve_control_loop
from ingestion.core.dispatch import DispatchResult


def test_control_loop_drains_full_batches_and_stops_before_next_claim():
    stop = Event()
    results = iter((
        DispatchResult(claimed=2, sent=2, failed=0),
        DispatchResult(claimed=1, sent=1, failed=0),
    ))
    calls = []
    logs = []

    def run_batch():
        calls.append(1)
        result = next(results)
        if len(calls) == 2:
            stop.set()
        return result

    serve_control_loop(
        role="dispatch-service", run_batch=run_batch, stop=stop,
        batch_limit=2, idle_seconds=1, log=logs.append,
    )
    assert len(calls) == 2
    assert [json.loads(line)["sent"] for line in logs] == [2, 1]


def test_control_loop_waits_when_idle_and_propagates_failures():
    class Stop:
        waits = []

        def is_set(self):
            return bool(self.waits)

        def wait(self, seconds):
            self.waits.append(seconds)

    stop = Stop()
    serve_control_loop(
        role="recovery-service", run_batch=lambda: RecoveryBatch(0, 0),
        stop=stop, batch_limit=5, idle_seconds=0.25,
    )
    assert stop.waits == [0.25]
    with pytest.raises(RuntimeError, match="database unavailable"):
        serve_control_loop(
            role="recovery-service",
            run_batch=lambda: (_ for _ in ()).throw(RuntimeError("database unavailable")),
            stop=Event(), batch_limit=5, idle_seconds=1,
        )


def test_control_loop_rejects_oversized_batch_and_invalid_config():
    with pytest.raises(ValueError, match="exceeded"):
        serve_control_loop(
            role="publisher", run_batch=lambda: RecoveryBatch(11, 11),
            stop=Event(), batch_limit=10, idle_seconds=1,
        )
    resources = load_resources(Path(__file__).resolve().parents[2] / "local/resources.yaml")
    data = resources.model_dump(mode="json")
    data["control_loop"]["dispatch_idle_seconds"] = 0
    with pytest.raises(ValueError):
        ResourceConfig.model_validate(data)
