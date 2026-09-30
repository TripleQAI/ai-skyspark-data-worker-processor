"""Opt-in physical S3 check for the local-only metadata fixture."""

from __future__ import annotations

import importlib.util
import io
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import boto3
import pytest

from ingestion.adapters.aws.s3_evidence import S3EvidenceVerifier, S3ObjectStore
from ingestion.config.loader import resolve_config
from ingestion.contracts.config import FeedKind
from ingestion.contracts.jobs import JobCompletion
from ingestion.contracts.resources import load_resources
from ingestion.core.planner import plan_run


ROOT = Path(__file__).resolve().parents[2]
ENDPOINT = os.environ.get("TEST_LOCALSTACK_ENDPOINT_URL")
pytestmark = pytest.mark.skipif(not ENDPOINT, reason="TEST_LOCALSTACK_ENDPOINT_URL is unset")


def test_localstack_synthetic_metadata_has_physical_s3_evidence(monkeypatch):
    if not ENDPOINT.startswith((
        "http://localhost:", "http://127.0.0.1:", "http://localstack:",
    )):
        raise ValueError("test requires a local LocalStack endpoint")
    resources = load_resources(ROOT / "local/resources.yaml")
    config = resolve_config(
        ROOT / "config/profiles/default.yaml",
        ROOT / "config/bindings/example-local.yaml",
        ROOT / "local/manifests/synthetic-metadata.yaml",
        environment="local",
    )
    _, jobs = plan_run(config, FeedKind.METADATA, datetime.now(timezone.utc))
    job = jobs[0].model_copy(update={
        "run_id": uuid4().hex * 2,
        "job_id": uuid4().hex * 2,
    })
    spec = importlib.util.spec_from_file_location(
        "synthetic_metadata_localstack", ROOT / "local/scripts/synthetic_metadata.py",
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setenv("AWS_ENDPOINT_URL", ENDPOINT)
    monkeypatch.setenv("RESOURCE_CONFIG_PATH", str(ROOT / "local/resources.yaml"))
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    request = {
        "job": job.model_dump(mode="json"),
        "target": "s3",
        "source": {"site_uri": config.binding.approved_sites[job.site_ref]},
    }
    output = io.StringIO()
    monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps(request)))
    monkeypatch.setattr(sys, "stdout", output)
    client = boto3.client(
        "s3", region_name=resources.region, endpoint_url=ENDPOINT,
        aws_access_key_id="test", aws_secret_access_key="test",
    )
    try:
        module.main()
        completion = JobCompletion.model_validate_json(output.getvalue())
        S3EvidenceVerifier(
            S3ObjectStore(region_name=resources.region, client=client),
            resources.storage,
        ).verify(job, completion)
        assert completion.sink.row_count == 2
    finally:
        job_prefix = (
            f"{job.tenant_id}/{job.project_id}/{job.feed.value}/"
            f"{job.run_id}/{job.job_id}/"
        )
        for bucket, prefix in (
            (resources.storage.raw_bucket, f"raw/{job_prefix}"),
            (resources.storage.certified_bucket, f"certified/{job_prefix}"),
        ):
            for page in client.get_paginator("list_objects_v2").paginate(
                Bucket=bucket, Prefix=prefix,
            ):
                for item in page.get("Contents", []):
                    client.delete_object(Bucket=bucket, Key=item["Key"])
