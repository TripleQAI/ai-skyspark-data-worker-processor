import io
import json
from datetime import date, datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
from pydantic import ValidationError

from ingestion.config.loader import resolve_config
from ingestion.config.versioned_s3 import S3VersionedConfigStore, parse_versioned_s3_ref
from ingestion.contracts.jobs import CertifiedInventory
from ingestion.contracts.scheduled import ScheduledTrigger
from ingestion.core.scheduled_planner import _rules_day_window, plan_scheduled_run, plan_scheduled_runs


ROOT = Path(__file__).resolve().parents[2]
REF = "s3://reviewed-configs/demo/config.json?versionId=v1"


def _config():
    return resolve_config(
        ROOT / "config/profiles/default.yaml",
        ROOT / "config/bindings/example-local.yaml",
        ROOT / "config/manifests/plugins.yaml",
        environment="local",
    )


def _trigger(config, feed="history", due="2026-09-27T10:05:00Z"):
    return ScheduledTrigger.model_validate({
        "schema_version": 1,
        "tenant_id": config.binding.tenant_id,
        "project_id": config.binding.project_id,
        "feed": feed,
        "profile_id": config.profile.profile_id,
        "config_hash": config.config_hash,
        "config_ref": REF,
        "scheduled_at": due,
    })


def test_scheduled_windows_and_deterministic_jobs():
    config = _config()
    inventory = CertifiedInventory.model_validate_json(
        (ROOT / "local/fixtures/inventory.json").read_text(encoding="utf-8")
    )
    history = _trigger(config)
    run, jobs = plan_scheduled_run(history, config, inventory=inventory)
    assert run.window_start == datetime(2026, 9, 27, 10, 0, tzinfo=timezone.utc)
    assert run.window_end == history.scheduled_at
    assert len(jobs) >= 2
    assert plan_scheduled_run(history, config, inventory=inventory)[0].run_id == run.run_id

    rules, rule_jobs = plan_scheduled_run(
        _trigger(config, "rules", "2026-09-27T02:00:00Z"), config, inventory=inventory
    )
    assert rules.window_start == datetime(2026, 9, 26, 0, 0, tzinfo=timezone.utc)
    assert rules.window_end == datetime(2026, 9, 27, 0, 0, tzinfo=timezone.utc)
    assert len(rule_jobs) >= 2
    metadata, metadata_jobs = plan_scheduled_run(
        _trigger(config, "metadata", "2026-09-27T03:00:00Z"), config
    )
    assert metadata.window_start is None
    assert len(metadata_jobs) == len(config.binding.approved_sites)


def test_configured_source_lag_delays_exact_history_window():
    from dataclasses import replace

    config = _config()
    config = replace(config, binding=config.binding.model_copy(
        update={"history_source_lag_minutes": 5},
    ))
    inventory = CertifiedInventory.model_validate_json(
        (ROOT / "local/fixtures/inventory.json").read_text(encoding="utf-8")
    )
    run, _ = plan_scheduled_run(_trigger(config), config, inventory=inventory)
    assert run.window_start == datetime(2026, 9, 27, 9, 55, tzinfo=timezone.utc)
    assert run.window_end == datetime(2026, 9, 27, 10, 0, tzinfo=timezone.utc)


def test_lookback_plans_distinct_prior_windows_from_same_due_time():
    from dataclasses import replace

    config = _config()
    config = replace(config, binding=config.binding.model_copy(update={
        "history_source_lag_minutes": 5, "history_lookback_windows": 2,
    }))
    inventory = CertifiedInventory.model_validate_json(
        (ROOT / "local/fixtures/inventory.json").read_text(encoding="utf-8")
    )
    runs = plan_scheduled_runs(_trigger(config), config, inventory=inventory)
    assert len(runs) == 3
    assert [run.window_start.hour * 60 + run.window_start.minute for run, _ in runs] == [595, 590, 585]
    assert len({run.run_id for run, _ in runs}) == 3
    assert all(run.scheduled_at == runs[0][0].scheduled_at for run, _ in runs)


def test_rules_local_day_handles_dst_and_overlap_replays_prior_days():
    from dataclasses import replace

    chicago = ZoneInfo("America/Chicago")
    spring = _rules_day_window(date(2026, 3, 8), chicago)
    fall = _rules_day_window(date(2026, 11, 1), chicago)
    assert int((spring[1] - spring[0]).total_seconds() / 3600) == 23
    assert int((fall[1] - fall[0]).total_seconds() / 3600) == 25

    config = _config()
    config = replace(config, binding=config.binding.model_copy(update={
        "rules_lookback_days": 2,
    }))
    inventory = CertifiedInventory.model_validate_json(
        (ROOT / "local/fixtures/inventory.json").read_text(encoding="utf-8")
    )
    runs = plan_scheduled_runs(
        _trigger(config, "rules", "2026-09-28T02:00:00Z"),
        config, inventory=inventory,
    )
    assert [run.window_start.date().isoformat() for run, _ in runs] == [
        "2026-09-27", "2026-09-26", "2026-09-25",
    ]
    assert len({run.run_id for run, _ in runs}) == 3


def test_scheduled_ingress_fails_closed_on_scope_and_cadence():
    config = _config()
    with pytest.raises(ValueError, match="requires certified inventory"):
        plan_scheduled_run(_trigger(config), config)
    with pytest.raises(ValueError, match="interval grid"):
        plan_scheduled_run(_trigger(config, due="2026-09-27T10:06:00Z"), config)
    with pytest.raises(ValueError, match="weekday"):
        plan_scheduled_run(
            _trigger(config, "metadata", "2026-09-26T03:00:00Z"), config
        )
    mismatched = _trigger(config).model_copy(update={"project_id": "other-project"})
    with pytest.raises(ValueError, match="does not match"):
        plan_scheduled_run(mismatched, config)
    with pytest.raises(ValidationError, match="UTC minute"):
        _trigger(config, due="2026-09-27T10:05:01Z")


class FakeS3:
    def __init__(self, payload, *, version="v1", length=None):
        self.payload = payload
        self.version = version
        self.length = len(payload) if length is None else length
        self.calls = []

    def get_object(self, **kwargs):
        self.calls.append(kwargs)
        return {
            "VersionId": self.version,
            "ContentLength": self.length,
            "Body": io.BytesIO(self.payload),
        }


def _bundle(config):
    return json.dumps({
        "schema_version": 1,
        "profile": config.profile.model_dump(mode="json"),
        "binding": config.binding.model_dump(mode="json"),
        "manifest": config.manifest.model_dump(mode="json"),
    }).encode("utf-8")


def test_versioned_bundle_resolves_same_hash_and_exact_s3_version():
    config = _config()
    client = FakeS3(_bundle(config))
    loaded = S3VersionedConfigStore(
        region_name="us-east-1", max_bytes=100_000, client=client
    ).load(REF, environment="local")
    assert loaded.config_hash == config.config_hash
    assert client.calls == [{
        "Bucket": "reviewed-configs", "Key": "demo/config.json", "VersionId": "v1"
    }]


def test_bundle_rejects_unpinned_or_wrong_version_and_oversize():
    config = _config()
    with pytest.raises(ValueError, match="pin one non-null"):
        parse_versioned_s3_ref("s3://bucket/key")
    with pytest.raises(ValueError, match="pin one non-null"):
        parse_versioned_s3_ref("s3://bucket/key?versionId=null")
    with pytest.raises(ValueError, match="different configuration version"):
        S3VersionedConfigStore(
            region_name="us-east-1", max_bytes=100_000,
            client=FakeS3(_bundle(config), version="v2"),
        ).load(REF, environment="local")
    with pytest.raises(ValueError, match="byte limit"):
        S3VersionedConfigStore(
            region_name="us-east-1", max_bytes=10, client=FakeS3(_bundle(config)),
        ).load(REF, environment="local")


def test_bundle_rejects_duplicate_json_keys():
    payload = b'{"schema_version":1,"schema_version":1,"profile":{},"binding":{},"manifest":{}}'
    with pytest.raises(ValueError, match="duplicate bundle key"):
        S3VersionedConfigStore(
            region_name="us-east-1", max_bytes=1000, client=FakeS3(payload),
        ).load(REF, environment="local")
