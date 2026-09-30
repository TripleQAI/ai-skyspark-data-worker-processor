"""Opt-in durable Phase 8 utility audit; requires a disposable control DB."""

import hashlib
import os
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
import yaml

from ingestion.adapters.control.migrations import apply_migrations
from ingestion.adapters.control.standalone_runs import PostgresStandaloneRunRepository
from ingestion.adapters.standalone_scripts import RegisteredUtilityRunner
from ingestion.config.loader import resolve_config_documents
from ingestion.contracts.standalone import UtilityContext


ROOT = Path(__file__).resolve().parents[2]
DSN = os.environ.get("TEST_CONTROL_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="TEST_CONTROL_DSN is required")


def _config():
    profile = yaml.safe_load((ROOT / "config/profiles/default.yaml").read_text(encoding="utf-8"))
    binding = yaml.safe_load((ROOT / "config/bindings/example-local.yaml").read_text(encoding="utf-8"))
    manifest = yaml.safe_load((ROOT / "config/manifests/plugins.yaml").read_text(encoding="utf-8"))
    script = ROOT / "tests/fixtures/utility_script.py"
    manifest["utilities"] = [{
        "id": "tools.audit_probe@1", "accepted_context_schema": "utility-context-v1",
        "targets": ["s3"],
        "execution": {
            "path": script.name, "sha256": hashlib.sha256(script.read_bytes()).hexdigest(),
            "timeout_seconds": 1, "max_output_bytes": 4096,
            "context_transport": "file", "output_contract": "utility-result-v1",
            "optional_env_names": ["SCRIPT_TEST_MODE"],
        },
    }]
    return resolve_config_documents(profile, binding, manifest, environment="local")


def test_registered_and_rejected_utility_runs_have_durable_audit(monkeypatch):
    apply_migrations(DSN, ROOT / "migrations/control")
    config = _config()
    runner = RegisteredUtilityRunner(
        config, ROOT / "tests/fixtures", PostgresStandaloneRunRepository(DSN),
    )
    cases = [
        ("ok", "tools.audit_probe@1", "s3", "completed"),
        ("ok", "tools.unknown@1", "s3", "rejected"),
        ("ok", "tools.audit_probe@1", "timescale", "rejected"),
        ("bad_json", "tools.audit_probe@1", "s3", "rejected"),
        ("slow", "tools.audit_probe@1", "s3", "failed"),
    ]
    for mode, script_id, target, expected in cases:
        monkeypatch.setenv("SCRIPT_TEST_MODE", mode)
        context = UtilityContext(
            schema_version=1, run_id=str(uuid4()), script_id=script_id,
            config_hash=config.config_hash, target=target,
        )
        if expected == "completed":
            assert runner.run(context).run_id == context.run_id
        else:
            with pytest.raises(Exception):
                runner.run(context)
        with psycopg.connect(DSN) as conn:
            row = conn.execute(
                "SELECT status, error_class, result_json, finished_at "
                "FROM ingestion.standalone_script_runs WHERE run_id = %s",
                (context.run_id,),
            ).fetchone()
        assert row[0] == expected
        assert row[3] is not None
        assert (row[2] is not None) == (expected == "completed")
        assert (row[1] is None) == (expected == "completed")
