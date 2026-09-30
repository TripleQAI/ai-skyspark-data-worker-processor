"""Disposable LocalStack check of versioned config and AWS schedule primitives."""

import json
import os
from pathlib import Path
import time
from uuid import uuid4

import boto3
import pytest

from ingestion.config.loader import resolve_config
from ingestion.adapters.aws.scheduler import SchedulerReconciler
from ingestion.core.schedules import build_schedule_specs


ROOT = Path(__file__).resolve().parents[2]
ENDPOINT = os.environ.get("TEST_LOCALSTACK_ENDPOINT_URL")
pytestmark = pytest.mark.skipif(not ENDPOINT, reason="TEST_LOCALSTACK_ENDPOINT_URL is required")


def test_versioned_config_and_disabled_project_schedules_reach_localstack():
    if not ENDPOINT.startswith(("http://localhost:", "http://127.0.0.1:")):
        raise ValueError("Phase 9 integration requires a host-local LocalStack endpoint")
    session = boto3.Session(
        aws_access_key_id="test", aws_secret_access_key="test", region_name="us-east-1",
    )
    s3 = session.client("s3", endpoint_url=ENDPOINT)
    sqs = session.client("sqs", endpoint_url=ENDPOINT)
    states = session.client("stepfunctions", endpoint_url=ENDPOINT)
    scheduler = session.client("scheduler", endpoint_url=ENDPOINT)
    suffix = uuid4().hex[:16]
    bucket = f"phase9-certified-{suffix}"
    queue_name = f"phase9-scheduler-dlq-{suffix}"
    group_name = f"phase9-{suffix}"
    machine_name = f"phase9-{suffix}"
    queue_url = machine_arn = None
    created_schedules = []
    bucket_created = group_created = False
    try:
        s3.create_bucket(Bucket=bucket)
        bucket_created = True
        s3.put_bucket_versioning(
            Bucket=bucket, VersioningConfiguration={"Status": "Enabled"},
        )
        key = f"config/tenant-a/project-a/{suffix}.json"
        object_version = s3.put_object(Bucket=bucket, Key=key, Body=b"{}")['VersionId']
        assert object_version and object_version != "null"
        assert s3.get_object(Bucket=bucket, Key=key, VersionId=object_version)["Body"].read() == b"{}"

        queue_url = sqs.create_queue(QueueName=queue_name)["QueueUrl"]
        queue_arn = sqs.get_queue_attributes(
            QueueUrl=queue_url, AttributeNames=["QueueArn"],
        )["Attributes"]["QueueArn"]
        role_arn = "arn:aws:iam::000000000000:role/phase9-local-test"
        machine_arn = states.create_state_machine(
            name=machine_name, type="STANDARD", roleArn=role_arn,
            definition=json.dumps({"StartAt": "Done", "States": {"Done": {"Type": "Pass", "End": True}}}),
        )["stateMachineArn"]
        execution = states.start_execution(
            stateMachineArn=machine_arn, input=json.dumps({"pilot": suffix}),
        )
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            record = states.describe_execution(executionArn=execution["executionArn"])
            if record["status"] != "RUNNING":
                break
            time.sleep(0.2)
        assert record["status"] == "SUCCEEDED"
        assert json.loads(record["output"])["pilot"] == suffix

        scheduler.create_schedule_group(Name=group_name)
        group_created = True
        config = resolve_config(
            ROOT / "config/profiles/default.yaml",
            ROOT / "config/bindings/example-local.yaml",
            ROOT / "config/manifests/plugins.yaml",
            environment="local",
        )
        specs = build_schedule_specs(
            config, config_ref=f"s3://{bucket}/{key}?versionId={object_version}",
            group_name=group_name, state_machine_arn=machine_arn,
            role_arn=role_arn, dead_letter_arn=queue_arn,
        )
        assert len(specs) == 3
        reconciler = SchedulerReconciler(region_name="us-east-1", client=scheduler)
        for spec in specs:
            assert reconciler.reconcile((spec,), apply=True)[0]["state"] == "created"
            created_schedules.append(spec.name)
            recorded = scheduler.get_schedule(Name=spec.name, GroupName=group_name)
            assert recorded["State"] == "DISABLED"
            assert recorded["ScheduleExpression"] == spec.request["ScheduleExpression"]
            assert json.loads(recorded["Target"]["Input"])["config_hash"] == config.config_hash
        assert all(item["state"] == "unchanged" for item in reconciler.reconcile(specs, apply=False))
        assert {spec.feed.value for spec in specs} == {"history", "rules", "metadata"}
    finally:
        for name in created_schedules:
            scheduler.delete_schedule(Name=name, GroupName=group_name)
        if group_created:
            scheduler.delete_schedule_group(Name=group_name)
        if machine_arn:
            states.delete_state_machine(stateMachineArn=machine_arn)
        if queue_url:
            sqs.delete_queue(QueueUrl=queue_url)
        if bucket_created:
            versions = s3.list_object_versions(Bucket=bucket)
            for item in versions.get("Versions", []) + versions.get("DeleteMarkers", []):
                s3.delete_object(Bucket=bucket, Key=item["Key"], VersionId=item["VersionId"])
            s3.delete_bucket(Bucket=bucket)
