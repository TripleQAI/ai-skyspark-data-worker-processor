from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path

import pytest
from botocore.exceptions import ClientError

from ingestion.adapters.aws.s3_evidence import (
    EvidenceMismatch, S3EvidenceVerifier, S3JsonlSink, S3ObjectStore, S3RawStore,
)
from ingestion.config.loader import resolve_config
from ingestion.contracts.config import FeedKind
from ingestion.contracts.jobs import JobCompletion
from ingestion.contracts.resources import load_resources
from ingestion.core.planner import plan_run
from ingestion.core.failures import CertifiedBatchTooLarge


ROOT = Path(__file__).resolve().parents[2]


class FakeS3:
    def __init__(self):
        self.objects = {}

    def put_object(self, **kwargs):
        key = (kwargs["Bucket"], kwargs["Key"])
        if key in self.objects:
            raise ClientError({
                "Error": {"Code": "PreconditionFailed", "Message": "exists"}
            }, "PutObject")
        data = kwargs["Body"].read()
        assert len(data) == kwargs["ContentLength"]
        self.objects[key] = data

    def get_object(self, **kwargs):
        return {"Body": BytesIO(self.objects[(kwargs["Bucket"], kwargs["Key"])])}


def _job():
    config = resolve_config(
        ROOT / "config/profiles/default.yaml",
        ROOT / "config/bindings/example-local.yaml",
        ROOT / "config/manifests/plugins.yaml",
        environment="local",
    )
    _, jobs = plan_run(
        config, FeedKind.METADATA,
        datetime(2026, 9, 26, 15, tzinfo=timezone.utc),
    )
    return jobs[0]


def test_raw_and_s3_target_are_immutable_and_physically_verified():
    job = _job()
    policy = load_resources(ROOT / "local/resources.yaml").storage
    fake = FakeS3()
    objects = S3ObjectStore(region_name="us-east-1", client=fake)
    raw = S3RawStore(objects, policy)
    sink = S3JsonlSink(objects, policy)
    artifact = raw.put(
        job, [b"source-", b"response"], query_id="query-1",
        completed_scope=(job.site_ref,), row_count=1,
    )
    receipt = sink.put(job, [{"source_id": "equipment-1", "kind": "equipment"}])
    completion = JobCompletion(
        raw=artifact, sink=receipt, completed_scope=(job.site_ref,)
    )
    S3EvidenceVerifier(objects, policy).verify(job, completion)
    before = len(fake.objects)
    assert raw.put(
        job, [b"source-response"], query_id="query-1",
        completed_scope=(job.site_ref,), row_count=1,
    ) == artifact
    assert sink.put(job, [{"source_id": "equipment-1", "kind": "equipment"}]) == receipt
    assert len(fake.objects) == before == 4
    fake.objects[(policy.raw_bucket, artifact.object_key)] = b"tampered"
    with pytest.raises(EvidenceMismatch, match="checksum or length"):
        S3EvidenceVerifier(objects, policy).verify(job, completion)


def test_raw_scope_and_storage_caps_reject_incomplete_or_oversized_data():
    job = _job()
    policy = load_resources(ROOT / "local/resources.yaml").storage
    fake = FakeS3()
    objects = S3ObjectStore(region_name="us-east-1", client=fake)
    raw = S3RawStore(objects, policy)
    with pytest.raises(ValueError, match="cover requested"):
        raw.put(job, [b"x"], query_id="query-1", completed_scope=(), row_count=0)
    tiny_policy = policy.model_copy(update={"max_raw_bytes": 3})
    with pytest.raises(ValueError, match="byte cap"):
        S3RawStore(objects, tiny_policy).put(
            job, [b"four"], query_id="query-1",
            completed_scope=(job.site_ref,), row_count=1,
        )
    assert fake.objects == {}


def test_certified_batch_cap_is_terminal_before_any_object_is_written():
    job = _job()
    policy = load_resources(ROOT / "local/resources.yaml").storage.model_copy(
        update={"max_certified_rows": 1}
    )
    fake = FakeS3()
    sink = S3JsonlSink(S3ObjectStore(region_name="us-east-1", client=fake), policy)
    with pytest.raises(CertifiedBatchTooLarge, match="configured cap"):
        sink.put(job, [{"source_id": "one"}, {"source_id": "two"}])
    assert fake.objects == {}
