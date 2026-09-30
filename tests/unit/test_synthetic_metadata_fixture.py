"""The local reader must produce verifiable physical metadata evidence."""

from __future__ import annotations

import hashlib
import importlib.util
import io
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from botocore.exceptions import ClientError

from ingestion.adapters.aws.s3_evidence import S3EvidenceVerifier, S3ObjectStore
from ingestion.config.loader import resolve_config
from ingestion.contracts.config import FeedKind
from ingestion.contracts.jobs import JobCompletion
from ingestion.contracts.resources import load_resources
from ingestion.core.planner import plan_run


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "local/scripts/synthetic_metadata.py"


class FakeS3:
    def __init__(self):
        self.objects: dict[tuple[str, str], bytes] = {}

    def put_object(self, **kwargs):
        key = (kwargs["Bucket"], kwargs["Key"])
        if key in self.objects:
            raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
        self.objects[key] = kwargs["Body"].read()

    def get_object(self, **kwargs):
        return {"Body": io.BytesIO(self.objects[(kwargs["Bucket"], kwargs["Key"])])}


def test_local_metadata_fixture_writes_verifiable_s3_receipts(monkeypatch):
    config = resolve_config(
        ROOT / "config/profiles/default.yaml",
        ROOT / "config/bindings/example-local.yaml",
        ROOT / "local/manifests/synthetic-metadata.yaml",
        environment="local",
    )
    execution = next(
        reader.execution for reader in config.manifest.readers
        if reader.feed == FeedKind.METADATA
    )
    assert execution.sha256 == hashlib.sha256(SCRIPT.read_bytes()).hexdigest()
    _, jobs = plan_run(
        config, FeedKind.METADATA, datetime(2026, 9, 27, 3, tzinfo=timezone.utc),
    )
    assert len(jobs) == 2

    spec = importlib.util.spec_from_file_location("synthetic_metadata_fixture", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    fake = FakeS3()
    objects = S3ObjectStore(region_name="us-east-1", client=fake)
    monkeypatch.setattr(module, "S3ObjectStore", lambda **_: objects)
    monkeypatch.setenv("RESOURCE_CONFIG_PATH", str(ROOT / "local/resources.yaml"))
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://localhost:4566")
    resources = load_resources(ROOT / "local/resources.yaml")

    for job in jobs:
        request = {
            "job": job.model_dump(mode="json"),
            "target": "s3",
            "source": {
                "site_uri": config.binding.approved_sites[job.site_ref],
            },
        }
        output = io.StringIO()
        monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps(request)))
        monkeypatch.setattr(sys, "stdout", output)
        module.main()
        completion = JobCompletion.model_validate_json(output.getvalue())
        S3EvidenceVerifier(objects, resources.storage).verify(job, completion)
        data = fake.objects[(resources.storage.certified_bucket, completion.sink.batch_key)]
        rows = [json.loads(line) for line in data.splitlines()]
        assert len(rows) == completion.sink.row_count == 2
        assert {row["kind"] for row in rows} == {"equipment", "point"}
        assert {row["site_ref"] for row in rows} == {job.site_ref}
        assert all(row["source_kind"] == "synthetic-local-fixture" for row in rows)
