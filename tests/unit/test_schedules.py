import json
from pathlib import Path

import pytest
from botocore.exceptions import ClientError

from ingestion.adapters.aws.scheduler import ScheduleDriftError, SchedulerReconciler
from ingestion.config.loader import resolve_config
from ingestion.core.schedules import build_schedule_specs
from ingestion.schedule_cli import main as schedule_main


ROOT = Path(__file__).resolve().parents[2]


def _specs(**overrides):
    config = resolve_config(
        ROOT / "config/profiles/default.yaml",
        ROOT / "config/bindings/example-local.yaml",
        ROOT / "config/manifests/plugins.yaml",
        environment="local",
    )
    args = {
        "config_ref": "s3://reviewed-configs/project/config.yaml?versionId=v1",
        "group_name": "ingestion",
        "state_machine_arn": "arn:aws:states:us-east-1:123456789012:stateMachine:planner",
        "role_arn": "arn:aws:iam::123456789012:role/scheduler-planner",
        "dead_letter_arn": "arn:aws:sqs:us-east-1:123456789012:scheduler_dlq",
    }
    args.update(overrides)
    return config, build_schedule_specs(config, **args)


def test_three_project_feed_schedules_are_utc_aligned_and_secret_free():
    config, specs = _specs()
    assert len(specs) == 3
    assert {s.feed.value: s.request["ScheduleExpression"] for s in specs} == {
        "history": "cron(0/5 * * * ? *)",
        "metadata": "cron(0 3 ? * SUN *)",
        "rules": "cron(0 2 * * ? *)",
    }
    assert all(s.request["State"] == "DISABLED" for s in specs)
    assert all(s.request["FlexibleTimeWindow"] == {"Mode": "OFF"} for s in specs)
    assert all(len(s.name) <= 64 for s in specs)
    assert len({s.name for s in specs}) == 3
    assert [s.name for s in specs] == [s.name for s in _specs()[1]]
    for spec in specs:
        payload = json.loads(spec.request["Target"]["Input"])
        assert payload["config_hash"] == config.config_hash
        assert payload["scheduled_at"] == "<aws.scheduler.scheduled-time>"
        assert payload["feed"] == spec.feed.value
        assert "secret_ref" not in payload
        assert "endpoint" not in payload
        assert "approved_sites" not in payload


def test_schedule_rejects_unpinned_config_and_unaligned_interval():
    config, _ = _specs()
    with pytest.raises(ValueError, match="version-pinned"):
        _specs(config_ref="s3://bucket/current.yaml")
    data = config.profile.model_dump(mode="json")
    data["feeds"]["history"]["schedule"]["every_minutes"] = 7
    from ingestion.contracts.config import PipelineProfile
    from ingestion.config.loader import EffectiveConfig

    changed = EffectiveConfig(
        PipelineProfile.model_validate(data), config.binding, config.manifest, config.config_hash
    )
    with pytest.raises(ValueError, match="divide 60"):
        build_schedule_specs(
            changed, config_ref="s3://bucket/key?versionId=v1", group_name="ingestion",
            state_machine_arn="arn:aws:states:us-east-1:123456789012:stateMachine:planner",
            role_arn="arn:aws:iam::123456789012:role/scheduler-planner",
            dead_letter_arn="arn:aws:sqs:us-east-1:123456789012:scheduler_dlq",
        )


class FakeScheduler:
    def __init__(self):
        self.schedules = {}
        self.created = []
        self.updated = []

    def get_schedule(self, **kwargs):
        key = (kwargs["GroupName"], kwargs["Name"])
        if key not in self.schedules:
            raise ClientError(
                {"Error": {"Code": "ResourceNotFoundException", "Message": "absent"}},
                "GetSchedule",
            )
        return self.schedules[key]

    def create_schedule(self, **kwargs):
        self.created.append(kwargs)
        self.schedules[(kwargs["GroupName"], kwargs["Name"])] = kwargs

    def update_schedule(self, **kwargs):
        self.updated.append(kwargs)
        self.schedules[(kwargs["GroupName"], kwargs["Name"])] = kwargs


def test_reconcile_is_read_only_by_default_then_creates_once():
    _, specs = _specs()
    client = FakeScheduler()
    reconciler = SchedulerReconciler(region_name="us-east-1", client=client)
    assert {r["state"] for r in reconciler.reconcile(specs, apply=False)} == {"missing"}
    assert not client.created
    assert {r["state"] for r in reconciler.reconcile(specs, apply=True)} == {"created"}
    assert len(client.created) == 3
    assert all(len(item["ClientToken"]) == 32 for item in client.created)
    assert {r["state"] for r in reconciler.reconcile(specs, apply=True)} == {"unchanged"}
    assert len(client.created) == 3


def test_existing_schedule_drift_fails_without_mutation():
    _, specs = _specs()
    client = FakeScheduler()
    client.create_schedule(**specs[0].request)
    client.schedules[(specs[0].group_name, specs[0].name)]["Description"] = "someone-else"
    with pytest.raises(ScheduleDriftError, match="not project-managed"):
        SchedulerReconciler(region_name="us-east-1", client=client).reconcile(
            specs, apply=True
        )
    assert len(client.created) == 1


def test_late_drift_prevents_earlier_missing_schedule_creation():
    _, specs = _specs()
    client = FakeScheduler()
    client.create_schedule(**specs[-1].request)
    client.schedules[(specs[-1].group_name, specs[-1].name)]["Description"] = "someone-else"
    with pytest.raises(ScheduleDriftError):
        SchedulerReconciler(region_name="us-east-1", client=client).reconcile(
            specs, apply=True
        )
    assert len(client.created) == 1


def test_managed_schedule_update_preserves_optional_aws_fields():
    _, specs = _specs()
    client = FakeScheduler()
    client.create_schedule(**specs[0].request)
    current = client.schedules[(specs[0].group_name, specs[0].name)]
    current["State"] = "ENABLED"
    current["KmsKeyArn"] = "arn:aws:kms:us-east-1:123456789012:key/example"
    current["Target"]["RetryPolicy"] = {"MaximumRetryAttempts": 2}
    reconciler = SchedulerReconciler(region_name="us-east-1", client=client)
    assert reconciler.reconcile((specs[0],), apply=False)[0]["state"] == "update_needed"
    assert not client.updated
    assert reconciler.reconcile((specs[0],), apply=True)[0]["state"] == "updated"
    update = client.updated[0]
    assert update["State"] == "DISABLED"
    assert update["KmsKeyArn"] == current["KmsKeyArn"]
    assert update["Target"]["RetryPolicy"] == {"MaximumRetryAttempts": 2}
    assert reconciler.reconcile((specs[0],), apply=True)[0]["state"] == "unchanged"


def test_preview_command_needs_neither_database_nor_aws(monkeypatch, capsys):
    monkeypatch.delenv("CONTROL_DATABASE_URL", raising=False)
    args = [
        "--profile", str(ROOT / "config/profiles/default.yaml"),
        "--binding", str(ROOT / "config/bindings/example-local.yaml"),
        "--manifest", str(ROOT / "config/manifests/plugins.yaml"),
        "--environment", "local",
        "--config-ref", "s3://reviewed-configs/project/config.yaml?versionId=v1",
        "--group-name", "ingestion",
        "--state-machine-arn", "arn:aws:states:us-east-1:123456789012:stateMachine:planner",
        "--role-arn", "arn:aws:iam::123456789012:role/scheduler-planner",
        "--dead-letter-arn", "arn:aws:sqs:us-east-1:123456789012:scheduler_dlq",
        "--region", "us-east-1", "preview",
    ]
    assert schedule_main(args) == 0
    assert len(json.loads(capsys.readouterr().out)) == 3
