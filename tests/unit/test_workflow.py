import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from ingestion import aws_handlers
from ingestion.config.loader import resolve_config
from ingestion.contracts.config import FeedKind
from ingestion.contracts.resources import load_resources
from ingestion.contracts.scheduled import ScheduledTrigger
from ingestion.core.scheduled_service import persist_scheduled_trigger
from ingestion.core.workflow import build_workflow_definition
from ingestion.workflow_cli import main as workflow_main


ROOT = Path(__file__).resolve().parents[2]
RESOURCES = ROOT / "local/resources.yaml"
PLANNER_ARN = "arn:aws:lambda:us-east-1:123456789012:function:skyspark-planner"
STATUS_ARN = "arn:aws:lambda:us-east-1:123456789012:function:skyspark-status"
INVENTORY_ARN = "arn:aws:lambda:us-east-1:123456789012:function:skyspark-inventory"


def _config():
    return resolve_config(
        ROOT / "config/profiles/default.yaml",
        ROOT / "config/bindings/example-local.yaml",
        ROOT / "config/manifests/plugins.yaml",
        environment="local",
    )


def _trigger(config):
    return ScheduledTrigger.model_validate({
        "schema_version": 1,
        "tenant_id": config.binding.tenant_id,
        "project_id": config.binding.project_id,
        "feed": "metadata",
        "profile_id": config.profile.profile_id,
        "config_hash": config.config_hash,
        "config_ref": "s3://reviewed-configs/demo/config.json?versionId=v1",
        "scheduled_at": "2026-09-27T03:00:00Z",
    })


def test_workflow_routes_scope_and_coverage_with_bounded_polling():
    resources = load_resources(RESOURCES)
    definition = build_workflow_definition(
        resources, planner_arn=PLANNER_ARN, status_arn=STATUS_ARN,
        inventory_arn=INVENTORY_ARN,
    )
    states = definition["States"]
    assert definition["TimeoutSeconds"] == 14400 + 600 + 900
    assert states["WaitForCoverage"]["Seconds"] == 30
    assert states["PlanRun"]["Parameters"] == {
        "FunctionName": PLANNER_ARN, "Payload.$": "$"
    }
    assert states["ReadRunStatus"]["Parameters"]["Payload"] == {
        f"{field}.$": f"$.plan.run.{field}"
        for field in ("run_id", "tenant_id", "project_id", "config_hash", "feed", "lookback_run_ids")
    }
    assert {item["StringEquals"]: item["Next"] for item in states["EvaluateCoverage"]["Choices"]} == {
        "certified": "ChooseCertifiedFeed", "partial": "Partial",
        "blocked": "Blocked", "pending": "WaitForCoverage",
    }
    assert all(item["Next"] in states for item in states["EvaluateCoverage"]["Choices"])
    assert states["Partial"]["Type"] == states["Blocked"]["Type"] == "Fail"
    assert states["Certified"]["Type"] == "Succeed"
    assert states["ChooseCertifiedFeed"]["Choices"][0]["Next"] == "PublishMetadataInventory"
    assert states["PublishMetadataInventory"]["Parameters"] == {
        "FunctionName": INVENTORY_ARN,
        "Payload": states["ReadRunStatus"]["Parameters"]["Payload"],
    }
    assert "password" not in json.dumps(definition)
    with pytest.raises(ValueError, match="Lambda function ARNs"):
        build_workflow_definition(resources, planner_arn="not-an-arn", status_arn=STATUS_ARN, inventory_arn=INVENTORY_ARN)


def test_workflow_cli_renders_without_aws_or_database(capsys):
    assert workflow_main([
        "--resources", str(RESOURCES),
        "--planner-arn", PLANNER_ARN, "--status-arn", STATUS_ARN,
        "--inventory-arn", INVENTORY_ARN,
    ]) == 0
    assert json.loads(capsys.readouterr().out)["StartAt"] == "PlanRun"


def test_shared_scheduled_service_persists_metadata_and_returns_scope():
    config = _config()
    trigger = _trigger(config)
    calls = []

    class ConfigStore:
        def load(self, ref, *, environment):
            assert (ref, environment) == (trigger.config_ref, "local")
            return config

    class Repository:
        def save_plan(self, received_config, run, jobs, *, queue_class):
            calls.append((received_config, run, jobs, queue_class))
            return SimpleNamespace(run_id=run.run_id, expected_jobs=len(jobs), new_jobs=len(jobs))

    result = persist_scheduled_trigger(
        trigger, resources=load_resources(RESOURCES), environment="local",
        dsn="unused-in-injected-test", config_store=ConfigStore(), repository=Repository(),
    )
    assert len(calls) == 1
    assert result["run_id"] == calls[0][1].run_id
    assert result["expected_jobs"] == len(config.binding.approved_sites)
    assert result["queue_class"] == "metadata_sweep"
    assert result["inventory_version"] is None
    assert result["tenant_id"] == config.binding.tenant_id
    assert result["config_hash"] == config.config_hash


def test_plan_handler_validates_scheduler_input_and_calls_service(monkeypatch):
    config = _config()
    trigger = _trigger(config)
    monkeypatch.setattr(aws_handlers, "_runtime", lambda: ("local", RESOURCES, "dsn", None))
    seen = []
    monkeypatch.setattr(aws_handlers, "persist_scheduled_trigger", lambda parsed, **kwargs: (
        seen.append((parsed, kwargs)) or {"run_id": "a" * 64}
    ))
    assert aws_handlers.plan_handler(trigger.model_dump(mode="json"), None) == {
        "run_id": "a" * 64
    }
    assert seen[0][0] == trigger
    with pytest.raises(ValueError):
        aws_handlers.plan_handler({**trigger.model_dump(mode="json"), "untrusted": 1}, None)


def test_status_handler_scopes_query_and_uses_feed_deadline(monkeypatch):
    config = _config()
    monkeypatch.setattr(aws_handlers, "_runtime", lambda: ("local", RESOURCES, "dsn", None))
    seen = []

    class Reader:
        def __init__(self, dsn):
            assert dsn == "dsn"

        def read(self, **kwargs):
            seen.append(kwargs)
            return SimpleNamespace(summary=lambda: {"state": "pending"})

    monkeypatch.setattr(aws_handlers, "PostgresRunStatusReader", Reader)
    event = {
        "run_id": "a" * 64, "tenant_id": config.binding.tenant_id,
        "project_id": config.binding.project_id, "config_hash": config.config_hash,
        "feed": "history",
    }
    assert aws_handlers.status_handler(event, None) == {"state": "pending"}
    assert seen[0]["max_run_seconds"] == 900
    with pytest.raises(ValueError):
        aws_handlers.status_handler({**event, "feed": "invalid"}, None)


@pytest.mark.parametrize("feed", ["history", "rules"])
def test_status_handler_waits_for_lookback_runs(monkeypatch, feed):
    from ingestion.adapters.control.run_status import RunProgress

    config = _config()
    monkeypatch.setattr(aws_handlers, "_runtime", lambda: ("local", RESOURCES, "dsn", None))
    state = {"a" * 64: "certified", "b" * 64: "pending"}

    class Reader:
        def __init__(self, dsn):
            assert dsn == "dsn"

        def read(self, **kwargs):
            return RunProgress(
                run_id=kwargs["run_id"], feed=FeedKind(feed),
                state=state[kwargs["run_id"]], reason="test",
                expected_root_jobs=1, actual_root_jobs=1, total_jobs=1,
                evidence_certified_jobs=int(state[kwargs["run_id"]] == "certified"),
                terminal_jobs=0, expected_sites=1, completed_sites=1,
                deadline_at=datetime(2026, 9, 28, tzinfo=timezone.utc),
            )

    monkeypatch.setattr(aws_handlers, "PostgresRunStatusReader", Reader)
    event = {
        "run_id": "a" * 64, "lookback_run_ids": ["b" * 64],
        "tenant_id": config.binding.tenant_id,
        "project_id": config.binding.project_id,
        "config_hash": config.config_hash, "feed": feed,
    }
    assert aws_handlers.status_handler(event, None)["state"] == "pending"
    state["b" * 64] = "certified"
    assert aws_handlers.status_handler(event, None)["state"] == "certified"


def test_inventory_handler_requires_metadata_and_pinned_binding(monkeypatch):
    config = _config()
    monkeypatch.setattr(aws_handlers, "_runtime", lambda: ("local", RESOURCES, "dsn", "http://localstack:4566"))
    seen = []

    class Registry:
        def __init__(self, dsn, *, environment):
            assert (dsn, environment) == ("dsn", "local")

        def load(self, **scope):
            seen.append(scope)
            return config

    class Publisher:
        def __init__(self, dsn, **kwargs):
            assert dsn == "dsn"

        def publish(self, *, run_id, config):
            seen.append(run_id)
            return SimpleNamespace(
                source_run_id=run_id, version="v1",
                object_ref="s3://example/inventory.json?versionId=1",
            )

    monkeypatch.setattr(aws_handlers, "PostgresConfigRegistry", Registry)
    monkeypatch.setattr(aws_handlers, "PostgresMetadataInventoryPublisher", Publisher)
    monkeypatch.setattr(aws_handlers, "S3ObjectStore", lambda **kwargs: object())
    monkeypatch.setattr(aws_handlers, "S3VersionedInventoryStore", lambda **kwargs: object())
    event = {
        "run_id": "a" * 64, "tenant_id": config.binding.tenant_id,
        "project_id": config.binding.project_id, "config_hash": config.config_hash,
        "feed": "metadata",
    }
    assert aws_handlers.inventory_handler(event, None)["inventory_version"] == "v1"
    assert seen[0] == {
        "config_hash": config.config_hash, "tenant_id": config.binding.tenant_id,
        "project_id": config.binding.project_id,
    }
    with pytest.raises(ValueError, match="only metadata"):
        aws_handlers.inventory_handler({**event, "feed": "history"}, None)


def test_aws_runtime_reads_dsn_from_secret_only(monkeypatch):
    monkeypatch.setenv("APP_ENVIRONMENT", "aws")
    monkeypatch.setenv("RESOURCE_CONFIG_PATH", str(RESOURCES))
    monkeypatch.setenv("CONTROL_DATABASE_SECRET_ARN", "arn:aws:secretsmanager:us-east-1:123456789012:secret:control")
    monkeypatch.setenv("CONTROL_DATABASE_URL", "ignored-local-dsn")
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://localhost:4566")
    seen = []

    class Client:
        def get_secret_value(self, **kwargs):
            seen.append(kwargs)
            return {"SecretString": json.dumps({"dsn": "postgresql://secret-dsn"})}

    monkeypatch.setattr(aws_handlers.boto3, "client", lambda name: Client() if name == "secretsmanager" else None)
    assert aws_handlers._runtime() == ("aws", RESOURCES, "postgresql://secret-dsn", None)
    assert seen == [{"SecretId": "arn:aws:secretsmanager:us-east-1:123456789012:secret:control"}]


def test_aws_inventory_uses_target_database_secret_for_timescale(monkeypatch):
    config = _config()
    monkeypatch.setattr(aws_handlers, "_runtime", lambda: ("aws", RESOURCES, "control-dsn", None))
    monkeypatch.setenv("TARGET_DATABASE_SECRET_ARN", "arn:aws:secretsmanager:us-east-1:123456789012:secret:target")
    monkeypatch.setenv("TARGET_DATABASE_URL", "must-not-use-local-value")
    requested = []
    monkeypatch.setattr(aws_handlers, "_secret_dsn", lambda arn: requested.append(arn) or "target-dsn")
    monkeypatch.setattr(aws_handlers, "PostgresConfigRegistry", lambda *args, **kwargs: SimpleNamespace(load=lambda **scope: config))
    monkeypatch.setattr(aws_handlers, "S3ObjectStore", lambda **kwargs: object())
    monkeypatch.setattr(aws_handlers, "S3VersionedInventoryStore", lambda **kwargs: object())

    class Publisher:
        def __init__(self, dsn, **kwargs):
            assert dsn == "control-dsn"
            assert kwargs["target_dsn"] == "target-dsn"

        def publish(self, *, run_id, config):
            return SimpleNamespace(source_run_id=run_id, version="v1", object_ref="s3://bucket/key?versionId=1")

    monkeypatch.setattr(aws_handlers, "PostgresMetadataInventoryPublisher", Publisher)
    event = {
        "run_id": "a" * 64, "tenant_id": config.binding.tenant_id,
        "project_id": config.binding.project_id, "config_hash": config.config_hash,
        "feed": "metadata",
    }
    assert aws_handlers.inventory_handler(event, None)["inventory_version"] == "v1"
    assert requested == ["arn:aws:secretsmanager:us-east-1:123456789012:secret:target"]
