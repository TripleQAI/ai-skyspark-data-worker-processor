"""Small Step Functions Lambda entrypoints; workers remain long-running ECS tasks."""

from __future__ import annotations

import json
import os
from dataclasses import replace
from pathlib import Path
from typing import Any

import boto3
from pydantic import Field

from ingestion.adapters.control.run_status import PostgresRunStatusReader
from ingestion.adapters.control.config_registry import PostgresConfigRegistry
from ingestion.adapters.control.metadata_inventory import PostgresMetadataInventoryPublisher
from ingestion.adapters.aws.inventory import S3VersionedInventoryStore
from ingestion.adapters.aws.s3_evidence import S3ObjectStore
from ingestion.contracts.config import FeedKind, StrictModel
from ingestion.contracts.resources import load_resources
from ingestion.contracts.scheduled import ScheduledTrigger
from ingestion.core.scheduled_service import persist_scheduled_trigger


class RunStatusRequest(StrictModel):
    run_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    tenant_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]*$")
    project_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]*$")
    config_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    feed: FeedKind
    lookback_run_ids: tuple[str, ...] = ()


def _runtime() -> tuple[str, Path, str, str | None]:
    environment = os.environ.get("APP_ENVIRONMENT")
    if environment not in {"local", "aws"}:
        raise ValueError("APP_ENVIRONMENT must be local or aws")
    path = os.environ.get("RESOURCE_CONFIG_PATH")
    if not path:
        raise ValueError("RESOURCE_CONFIG_PATH is required")
    if environment == "local":
        dsn = os.environ.get("CONTROL_DATABASE_URL")
        if not dsn:
            raise ValueError("CONTROL_DATABASE_URL is required for local runs")
        return environment, Path(path), dsn, os.environ.get("AWS_ENDPOINT_URL")
    secret_arn = os.environ.get("CONTROL_DATABASE_SECRET_ARN")
    if not secret_arn:
        raise ValueError("CONTROL_DATABASE_SECRET_ARN is required for AWS runs")
    return environment, Path(path), _secret_dsn(secret_arn), None


def _secret_dsn(secret_arn: str) -> str:
    response = boto3.client("secretsmanager").get_secret_value(SecretId=secret_arn)
    secret = json.loads(response["SecretString"])
    dsn = secret.get("dsn") if isinstance(secret, dict) else None
    if not isinstance(dsn, str) or not dsn:
        raise ValueError("database secret must contain a nonempty dsn")
    return dsn


def plan_handler(event: dict[str, Any], context: Any) -> dict[str, object]:
    environment, path, dsn, endpoint = _runtime()
    resources = load_resources(path)
    if len(json.dumps(event, separators=(",", ":")).encode("utf-8")) > (
        resources.config_artifacts.max_bundle_bytes
    ):
        raise ValueError("scheduled trigger exceeds configured byte limit")
    trigger = ScheduledTrigger.model_validate(event)
    return persist_scheduled_trigger(
        trigger, resources=resources, environment=environment,
        dsn=dsn, endpoint_url=endpoint,
    )


def status_handler(event: dict[str, Any], context: Any) -> dict[str, object]:
    _, path, dsn, _ = _runtime()
    resources = load_resources(path)
    request = RunStatusRequest.model_validate(event)
    progress = PostgresRunStatusReader(dsn).read(
        run_id=request.run_id,
        tenant_id=request.tenant_id,
        project_id=request.project_id,
        config_hash=request.config_hash,
        feed=request.feed,
        max_run_seconds=resources.workflow.max_run_seconds[request.feed],
    )
    if request.lookback_run_ids:
        if request.feed not in (FeedKind.HISTORY, FeedKind.RULES) or len(request.lookback_run_ids) > 12:
            raise ValueError("lookback status requires a bounded windowed feed")
        states = [progress]
        for extra_id in request.lookback_run_ids:
            if extra_id == request.run_id or len(extra_id) != 64 or any(
                char not in "0123456789abcdef" for char in extra_id
            ):
                raise ValueError("lookback run ID is invalid")
            states.append(PostgresRunStatusReader(dsn).read(
                run_id=extra_id, tenant_id=request.tenant_id,
                project_id=request.project_id, config_hash=request.config_hash,
                feed=request.feed,
                max_run_seconds=resources.workflow.max_run_seconds[request.feed],
            ))
        if len(set(request.lookback_run_ids)) != len(request.lookback_run_ids):
            raise ValueError("lookback run IDs must be unique")
        progress = replace(
            progress,
            expected_root_jobs=sum(item.expected_root_jobs for item in states),
            actual_root_jobs=sum(item.actual_root_jobs for item in states),
            total_jobs=sum(item.total_jobs for item in states),
            evidence_certified_jobs=sum(item.evidence_certified_jobs for item in states),
            terminal_jobs=sum(item.terminal_jobs for item in states),
            expected_sites=sum(item.expected_sites for item in states),
            completed_sites=sum(item.completed_sites for item in states),
        )
        if any(item.state == "blocked" for item in states):
            progress = replace(progress, state="blocked", reason="lookback_blocked")
        elif any(item.state == "partial" for item in states):
            progress = replace(progress, state="partial", reason="lookback_partial")
        elif any(item.state != "certified" for item in states):
            progress = replace(progress, state="pending", reason="lookback_pending")
    return progress.summary()


def inventory_handler(event: dict[str, Any], context: Any) -> dict[str, object]:
    """Publish a metadata inventory after every site job certifies."""
    environment, path, dsn, endpoint = _runtime()
    resources = load_resources(path)
    request = RunStatusRequest.model_validate(event)
    if request.feed != FeedKind.METADATA:
        raise ValueError("inventory publication accepts only metadata runs")
    config = PostgresConfigRegistry(dsn, environment=environment).load(
        config_hash=request.config_hash, tenant_id=request.tenant_id,
        project_id=request.project_id,
    )
    target_dsn = os.environ.get("TARGET_DATABASE_URL") if environment == "local" else None
    if environment == "aws" and os.environ.get("TARGET_DATABASE_SECRET_ARN"):
        target_dsn = _secret_dsn(os.environ["TARGET_DATABASE_SECRET_ARN"])
    record = PostgresMetadataInventoryPublisher(
        dsn,
        objects=S3ObjectStore(region_name=resources.region, endpoint_url=endpoint),
        inventory_store=S3VersionedInventoryStore(
            region_name=resources.region, policy=resources.inventory_artifacts,
            endpoint_url=endpoint,
        ),
        resources=resources,
        target_dsn=target_dsn,
    ).publish(run_id=request.run_id, config=config)
    return {
        "run_id": record.source_run_id,
        "inventory_version": record.version,
        "object_ref": record.object_ref,
    }
