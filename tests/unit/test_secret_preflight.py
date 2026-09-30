from __future__ import annotations

import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts/secret_preflight.py"


def _module():
    spec = importlib.util.spec_from_file_location("secret_preflight", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_scan_finds_literal_credentials_without_returning_values(tmp_path):
    secret = "Q" * 16
    candidate = tmp_path / "candidate.py"
    candidate.write_text(
        "password = '" + secret + "'\n"
        "safe_ref = 'aws-secretsmanager://arn:aws:secretsmanager:us-east-1:123:secret:x'\n",
        encoding="utf-8",
    )
    findings = _module().scan_file(candidate)
    assert findings
    assert any(line == 1 for line, _ in findings)
    assert secret not in repr(findings)


def test_current_project_preflight_is_clean():
    module = _module()
    assert all(not module.scan_file(path) for path in module._included(ROOT))
