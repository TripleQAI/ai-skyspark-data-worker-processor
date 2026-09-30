"""Registered file-context and non-certifying utility contracts."""

from datetime import datetime, timezone
import hashlib
from pathlib import Path
from uuid import uuid4

import pytest
import yaml

from ingestion.adapters.scripts import RegisteredScriptHandler, ScriptExecutionError
from ingestion.adapters.standalone_scripts import RegisteredUtilityRunner
from ingestion.config.loader import resolve_config_documents
from ingestion.contracts.config import FeedKind
from ingestion.contracts.standalone import UtilityContext
from ingestion.core.failures import NonRetryableJobError
from ingestion.core.planner import plan_run


ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "tests/fixtures"


def _config():
    profile = yaml.safe_load((ROOT / "config/profiles/default.yaml").read_text(encoding="utf-8"))
    binding = yaml.safe_load((ROOT / "config/bindings/example-local.yaml").read_text(encoding="utf-8"))
    manifest = yaml.safe_load((FIXTURES / "script_manifest.yaml").read_text(encoding="utf-8"))
    reader = manifest["readers"][0]
    execution = reader["execution"]
    execution.update({
        "path": "v2_script.py",
        "sha256": hashlib.sha256((FIXTURES / "v2_script.py").read_bytes()).hexdigest(),
        "timeout_seconds": 1,
        "context_transport": "file",
        "output_contract": "artifact-provenance-v2",
        "optional_env_names": ["SCRIPT_TEST_MODE"],
    })
    reader["source_capabilities"] = ["metadata"]
    reader["sink_capabilities"] = ["s3", "timescale"]
    manifest["utilities"] = [{
        "id": "tools.audit_probe@1", "accepted_context_schema": "utility-context-v1",
        "targets": ["s3"],
        "execution": {
            "path": "utility_script.py",
            "sha256": hashlib.sha256((FIXTURES / "utility_script.py").read_bytes()).hexdigest(),
            "timeout_seconds": 1, "max_output_bytes": 4096,
            "context_transport": "file", "output_contract": "utility-result-v1",
            "optional_env_names": ["SCRIPT_TEST_MODE"],
        },
    }]
    return resolve_config_documents(profile, binding, manifest, environment="local")


def _job(config):
    return plan_run(
        config, FeedKind.METADATA, datetime(2026, 9, 28, tzinfo=timezone.utc),
    )[1][0]


def test_v2_file_context_returns_pinned_artifact_manifest(monkeypatch):
    monkeypatch.delenv("SCRIPT_TEST_MODE", raising=False)
    config = _config()
    completion = RegisteredScriptHandler(config, FIXTURES, selected_feeds={FeedKind.METADATA}).run(_job(config))
    assert completion.completed_scope == (_job(config).site_ref,)


@pytest.mark.parametrize("mode", ["bad_json", "wrong_scope"])
def test_v2_invalid_output_is_rejected(monkeypatch, mode):
    monkeypatch.setenv("SCRIPT_TEST_MODE", mode)
    config = _config()
    with pytest.raises(NonRetryableJobError, match="invalid completion"):
        RegisteredScriptHandler(config, FIXTURES, selected_feeds={FeedKind.METADATA}).run(_job(config))


def test_v2_timeout_is_bounded(monkeypatch):
    monkeypatch.setenv("SCRIPT_TEST_MODE", "slow")
    config = _config()
    with pytest.raises(ScriptExecutionError, match="timed out"):
        RegisteredScriptHandler(config, FIXTURES, selected_feeds={FeedKind.METADATA}).run(_job(config))


class _Audit:
    def __init__(self):
        self.records = {}

    def start(self, context):
        self.records[context.run_id] = {"status": "running", "script_id": context.script_id}

    def finish(self, run_id, *, status, error_class=None, result=None):
        self.records[run_id].update(status=status, error_class=error_class, result=result)


def _context(config, *, script_id="tools.audit_probe@1", target="s3"):
    return UtilityContext(
        schema_version=1, run_id=str(uuid4()), script_id=script_id,
        config_hash=config.config_hash, target=target,
    )


def test_utility_completion_and_unknown_id_are_tracked(monkeypatch):
    monkeypatch.delenv("SCRIPT_TEST_MODE", raising=False)
    config = _config()
    audit = _Audit()
    runner = RegisteredUtilityRunner(config, FIXTURES, audit)
    accepted = _context(config)
    assert runner.run(accepted).run_id == accepted.run_id
    assert audit.records[accepted.run_id]["status"] == "completed"
    unknown = _context(config, script_id="tools.unknown@1")
    with pytest.raises(NonRetryableJobError, match="unknown"):
        runner.run(unknown)
    assert audit.records[unknown.run_id]["status"] == "rejected"


def test_utility_forbidden_target_invalid_output_and_timeout_are_tracked(monkeypatch):
    config = _config()
    audit = _Audit()
    runner = RegisteredUtilityRunner(config, FIXTURES, audit)
    forbidden = _context(config, target="timescale")
    with pytest.raises(NonRetryableJobError, match="target"):
        runner.run(forbidden)
    assert audit.records[forbidden.run_id]["status"] == "rejected"
    bad = _context(config)
    monkeypatch.setenv("SCRIPT_TEST_MODE", "bad_json")
    with pytest.raises(NonRetryableJobError, match="output contract"):
        runner.run(bad)
    assert audit.records[bad.run_id]["status"] == "rejected"
    slow = _context(config)
    monkeypatch.setenv("SCRIPT_TEST_MODE", "slow")
    with pytest.raises(ScriptExecutionError, match="timed out"):
        runner.run(slow)
    assert audit.records[slow.run_id]["status"] == "failed"
