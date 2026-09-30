from datetime import datetime, timezone
import hashlib
from pathlib import Path
import os
import threading
import time

import pytest

from ingestion.adapters.scripts import RegisteredScriptHandler, ScriptExecutionError
from ingestion.config.loader import EffectiveConfig, resolve_config
from ingestion.contracts.config import FeedKind, ScriptExecution
from ingestion.core.planner import plan_run
from ingestion.core.failures import NonRetryableJobError


ROOT = Path(__file__).resolve().parents[2]


def test_reviewed_python_script_receives_job_and_returns_completion():
    config = resolve_config(
        ROOT / "config/profiles/default.yaml",
        ROOT / "config/bindings/example-local.yaml",
        ROOT / "tests/fixtures/script_manifest.yaml",
        environment="local",
    )
    handler = RegisteredScriptHandler(config, ROOT / "tests/fixtures")
    _, jobs = plan_run(
        config, FeedKind.METADATA,
        datetime(2026, 9, 26, 15, tzinfo=timezone.utc),
    )
    completion = handler.run(jobs[0])
    assert completion.raw.job_id == jobs[0].job_id
    assert completion.sink.sink_kind == "s3"
    assert completion.completed_scope == (jobs[0].site_ref,)


def test_script_path_traversal_and_wrong_digest_are_rejected():
    with pytest.raises(ValueError, match="relative"):
        ScriptExecution(
            path="../secrets.py", sha256="0" * 64,
            timeout_seconds=5, max_output_bytes=4096,
        )
    config = resolve_config(
        ROOT / "config/profiles/default.yaml",
        ROOT / "config/bindings/example-local.yaml",
        ROOT / "tests/fixtures/script_manifest.yaml",
        environment="local",
    )
    first = config.manifest.readers[0]
    changed = first.model_copy(update={
        "execution": first.execution.model_copy(update={"sha256": "0" * 64})
    })
    changed_manifest = config.manifest.model_copy(update={
        "readers": (changed,) + config.manifest.readers[1:]
    })
    altered = EffectiveConfig(
        config.profile, config.binding, changed_manifest, config.config_hash
    )
    with pytest.raises(ValueError, match="checksum changed"):
        RegisteredScriptHandler(altered, ROOT / "tests/fixtures")


def test_running_script_is_killed_after_lease_cancellation():
    config = resolve_config(
        ROOT / "config/profiles/default.yaml",
        ROOT / "config/bindings/example-local.yaml",
        ROOT / "config/manifests/plugins.yaml",
        environment="local",
    )
    handler = RegisteredScriptHandler(config, ROOT / "tests/fixtures")
    execution = ScriptExecution(
        path="slow_script.py", sha256="0" * 64,
        timeout_seconds=4, max_output_bytes=4096,
    )
    cancelled = threading.Event()
    timer = threading.Timer(0.1, cancelled.set)
    timer.start()
    started = time.monotonic()
    try:
        with pytest.raises(ScriptExecutionError, match="lost its execution lease"):
            handler._bounded_run(
                ROOT / "tests/fixtures/slow_script.py", b"{}",
                {name: os.environ[name] for name in ("PATH", "SYSTEMROOT") if name in os.environ},
                execution, (cancelled,),
            )
    finally:
        timer.join()
    assert time.monotonic() - started < 2


def test_reviewed_script_exit_65_is_nonretryable_without_exposing_stderr():
    config = resolve_config(
        ROOT / "config/profiles/default.yaml",
        ROOT / "config/bindings/example-local.yaml",
        ROOT / "config/manifests/plugins.yaml", environment="local",
    )
    handler = RegisteredScriptHandler(config, ROOT / "tests/fixtures")
    execution = ScriptExecution(
        path="nonretryable_script.py", sha256="0" * 64,
        timeout_seconds=5, max_output_bytes=4096,
    )
    with pytest.raises(NonRetryableJobError, match="nonretryable"):
        handler._bounded_run(
            ROOT / "tests/fixtures/nonretryable_script.py", b"{}",
            {name: os.environ[name] for name in ("PATH", "SYSTEMROOT") if name in os.environ},
            execution, (),
        )


def test_invalid_script_completion_is_nonretryable():
    config = resolve_config(
        ROOT / "config/profiles/default.yaml",
        ROOT / "config/bindings/example-local.yaml",
        ROOT / "tests/fixtures/script_manifest.yaml", environment="local",
    )
    script = ROOT / "tests/fixtures/invalid_completion_script.py"
    reader = config.manifest.readers[0]
    execution = reader.execution.model_copy(update={
        "path": script.name,
        "sha256": hashlib.sha256(script.read_bytes()).hexdigest(),
    })
    manifest = config.manifest.model_copy(update={
        "readers": (reader.model_copy(update={"execution": execution}),)
        + config.manifest.readers[1:],
    })
    changed = EffectiveConfig(config.profile, config.binding, manifest, config.config_hash)
    handler = RegisteredScriptHandler(changed, ROOT / "tests/fixtures")
    _, jobs = plan_run(
        changed, FeedKind.METADATA, datetime(2026, 9, 26, 16, tzinfo=timezone.utc),
    )
    with pytest.raises(NonRetryableJobError, match="invalid completion"):
        handler.run(jobs[0])
