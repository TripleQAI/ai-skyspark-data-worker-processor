from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path

import pytest
from pydantic import ValidationError

from ingestion.contracts.config import SourceBinding
from ingestion.probe_cli import main as probe_main
from ingestion.source_probe import PhableProbeClient, PilotScope, approved_api_url, run_probe


def _binding(endpoint: str = "http://pilot.example/api/") -> SourceBinding:
    return SourceBinding(
        schema_version=1,
        profile_id="default",
        tenant_id="pilot-tenant",
        project_id="pilot-project",
        endpoint=endpoint,
        secret_ref="aws-secretsmanager://arn:aws:secretsmanager:us-east-1:123456789012:secret:pilot",
        approved_sites={"site-a": "pilotSite"},
    )


def _scope() -> PilotScope:
    return PilotScope(
        schema_version=1,
        site_ref="site-a",
        source_site_ref="source-site-1",
        equipment_ids=("equip-1", "equip-2"),
        history_point_ids=("point-1", "point-2"),
        history_batch_sizes=(1, 2),
        rules_batch_sizes=(1, 2),
        window_start_utc=datetime(2026, 9, 28, 10, tzinfo=timezone.utc),
        window_end_utc=datetime(2026, 9, 28, 10, 5, tzinfo=timezone.utc),
        rule_day=date(2026, 9, 27),
    )


class FakeClient:
    def __init__(self):
        self.expressions = []
        self.history_calls = []

    def eval(self, expression):
        self.expressions.append(expression)
        if "ruleSparks" in expression:
            return {"meta": {"ver": "3.0"}, "cols": [{"name": "targetRef"}], "rows": []}
        if "readAll(equip and siteRef" in expression and "id==@" in expression:
            ids = [value for value in ("equip-1", "equip-2") if f"id==@{value}" in expression]
            return {
                "meta": {"ver": "3.0"},
                "cols": [{"name": "id"}],
                "rows": [{"id": value} for value in ids],
            }
        return {
            "meta": {"ver": "3.0"},
            "cols": [{"name": "id"}, {"name": "siteRef"}],
            "rows": [{"id": "synthetic-1", "siteRef": "source-site-1"}],
        }

    def history(self, ids, start, end):
        self.history_calls.append((ids, start, end))
        return {
            "meta": {"ver": "3.0"},
            "cols": [{"name": "ts"}, *(
                {"name": f"v{index}", "meta": {"id": value}}
                for index, value in enumerate(ids)
            )],
            "rows": [{"ts": start.isoformat(), **{
                f"v{index}": 42 if index == 0 else None
                for index, _ in enumerate(ids)
            }}],
        }


def test_probe_is_bounded_and_report_contains_no_source_ids_or_url():
    client = FakeClient()
    report = run_probe(_binding(), _scope(), client)
    assert report["all_calls_succeeded"] is True
    assert len(report["calls"]) == 8
    assert all(call["elapsed_ms"] >= 0 for call in report["calls"])
    assert all(call["status"] == "succeeded" for call in report["calls"])
    assert any(call.get("equipment_scope_matches") for call in report["calls"])
    assert all(call.get("zero_rows_does_not_prove_equipment_coverage") is True
               for call in report["calls"] if call["operation"].startswith("rules_results"))
    assert client.history_calls[1][0] == ("point-1", "point-2")
    assert client.history_calls[1][1:] == (
        _scope().window_start_utc, _scope().window_end_utc,
    )
    history_calls = [call for call in report["calls"]
                     if call["operation"].startswith("history_")]
    assert [call["history_value_cells"] for call in history_calls] == [1, 1]
    assert [call["history_point_columns"] for call in history_calls] == [1, 2]
    assert [call["history_columns_with_id_metadata"] for call in history_calls] == [1, 2]
    assert all(call["requested_point_columns_matched"] == call["history_point_columns"]
               for call in history_calls)
    assert all(call["missing_requested_point_columns"] == 0 for call in history_calls)
    assert all("columns" not in call for call in history_calls)
    assert all("siteRef==@source-site-1" in expression for expression in client.expressions)
    output = json.dumps(report)
    for value in ("equip-1", "point-1", "point-2", "source-site-1", "pilot.example"):
        assert value not in output


def test_probe_accepts_configured_http_root_and_rejects_unsafe_scope():
    assert approved_api_url(_binding(), _scope()) == "http://pilot.example/api/pilotSite"
    with pytest.raises(ValueError, match="credential-free"):
        approved_api_url(_binding("http://pilot.example/api/?page=2"), _scope())
    with pytest.raises(ValidationError, match="plain Haystack Refs"):
        PilotScope.model_validate({
            **_scope().model_dump(mode="python"),
            "equipment_ids": ("equip-1).deleteAll(",),
        })


def test_probe_dry_run_never_reads_credentials_or_source(tmp_path, capsys, monkeypatch):
    binding_path = tmp_path / "binding.yaml"
    scope_path = tmp_path / "scope.json"
    binding_path.write_text(
        "schema_version: 1\nprofile_id: default\ntenant_id: pilot-tenant\n"
        "project_id: pilot-project\nendpoint: http://pilot.example/api/\n"
        "secret_ref: aws-secretsmanager://arn:aws:secretsmanager:us-east-1:123456789012:secret:pilot\n"
        "approved_sites:\n  site-a: pilotSite\n",
        encoding="utf-8",
    )
    scope_path.write_text(_scope().model_dump_json(), encoding="utf-8")
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    assert probe_main(["--binding", str(binding_path), "--scope", str(scope_path)]) == 0
    output = capsys.readouterr().out
    assert '"planned_calls": 8' in output
    assert "pilot.example" not in output
    assert "equip-1" not in output


def test_phable_adapter_sends_typed_read_only_request_if_optional_dependency_exists():
    pytest.importorskip("phable")

    class Client:
        def __init__(self):
            self.calls = []

        def call(self, path, request):
            self.calls.append((path, request))
            return {"meta": {}, "cols": [], "rows": []}

        def his_read_by_ids(self, ids, window):
            self.calls.append((ids, window))
            return {"meta": {}, "cols": [], "rows": []}

    client = Client()
    adapter = PhableProbeClient(client)
    adapter.eval("readAll(equip and siteRef==@source-site-1)")
    assert client.calls[0][0] == "eval"
    assert client.calls[0][1].rows[0]["expr"].startswith("readAll(")
    adapter.history(("point-1",), _scope().window_start_utc, _scope().window_end_utc)
    assert len(client.calls[1][0]) == 1
