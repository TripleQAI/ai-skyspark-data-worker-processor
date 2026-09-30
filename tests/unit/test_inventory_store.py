import hashlib
import io
import json
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import pytest

from ingestion.adapters.aws.inventory import S3VersionedInventoryStore
from ingestion.adapters.control.inventory import InventoryRecord
from ingestion.config.loader import resolve_config
from ingestion.contracts.resources import load_resources


ROOT = Path(__file__).resolve().parents[2]


def _fixtures():
    config = resolve_config(
        ROOT / "config/profiles/default.yaml",
        ROOT / "config/bindings/example-local.yaml",
        ROOT / "config/manifests/plugins.yaml",
        environment="local",
    )
    raw = (ROOT / "local/fixtures/inventory.json").read_bytes()
    data = json.loads(raw)
    record = InventoryRecord(
        tenant_id=config.binding.tenant_id,
        project_id=config.binding.project_id,
        version=data["version"], source_run_id="metadata-run",
        object_ref="s3://inventory-bucket/demo/inventory.json?versionId=object-v1",
        object_sha256=hashlib.sha256(raw).hexdigest(), byte_count=len(raw),
        certified_at=datetime(2026, 9, 27, tzinfo=timezone.utc),
    )
    return config, load_resources(ROOT / "local/resources.yaml").inventory_artifacts, record, raw


class FakeS3:
    def __init__(self, raw, version="object-v1"):
        self.raw = raw
        self.version = version
        self.calls = []

    def get_object(self, **kwargs):
        self.calls.append(kwargs)
        return {
            "VersionId": self.version, "ContentLength": len(self.raw),
            "Body": io.BytesIO(self.raw),
        }


def test_inventory_snapshot_requires_exact_version_hash_and_scope():
    config, policy, record, raw = _fixtures()
    client = FakeS3(raw)
    inventory = S3VersionedInventoryStore(
        region_name="us-east-1", policy=policy, client=client
    ).load(record, config=config)
    assert inventory.version == record.version
    assert set(inventory.sites) == set(config.binding.approved_sites)
    assert client.calls == [{
        "Bucket": "inventory-bucket", "Key": "demo/inventory.json",
        "VersionId": "object-v1",
    }]
    with pytest.raises(ValueError, match="different inventory version"):
        S3VersionedInventoryStore(
            region_name="us-east-1", policy=policy,
            client=FakeS3(raw, version="object-v2"),
        ).load(record, config=config)
    with pytest.raises(ValueError, match="checksum or length"):
        S3VersionedInventoryStore(
            region_name="us-east-1", policy=policy,
            client=FakeS3(raw.replace(b"site-a", b"site-x")),
        ).load(record, config=config)


def test_inventory_snapshot_rejects_unapproved_or_missing_sites():
    config, policy, record, raw = _fixtures()
    data = json.loads(raw)
    data["sites"].pop("site-b")
    changed = json.dumps(data).encode("utf-8")
    changed_record = replace(
        record, object_sha256=hashlib.sha256(changed).hexdigest(),
        byte_count=len(changed),
    )
    with pytest.raises(ValueError, match="site set"):
        S3VersionedInventoryStore(
            region_name="us-east-1", policy=policy, client=FakeS3(changed)
        ).load(changed_record, config=config)
