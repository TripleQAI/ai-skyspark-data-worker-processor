"""Two-stage persistence and dispatch gate for a disposable local control DB.

Run ``prepare``, restart the Docker services without removing volumes, then
run ``verify``. The state file contains IDs only; credentials stay in env vars.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path

import boto3
import psycopg

from ingestion.adapters.aws.sqs import SQSMessageSender
from ingestion.adapters.control.migrations import apply_migrations
from ingestion.adapters.control.postgres import PostgresControlRepository
from ingestion.config.loader import resolve_config
from ingestion.contracts.config import FeedKind
from ingestion.contracts.resources import load_resources
from ingestion.core.dispatch import dispatch_once
from ingestion.core.planner import plan_run


ROOT = Path(__file__).resolve().parents[1]
STATE = ROOT / "local/state/phase2-gate.json"


def _clients(endpoint: str, region: str):
    if not endpoint.startswith(("http://localhost:", "http://127.0.0.1:")):
        raise ValueError("the Phase 2 gate requires a host-local LocalStack endpoint")
    session = boto3.Session(
        aws_access_key_id="test", aws_secret_access_key="test", region_name=region,
    )
    return (
        session.client("s3", endpoint_url=endpoint),
        session.client("sqs", endpoint_url=endpoint),
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("stage", choices=("prepare", "verify"))
    parser.add_argument("--db-only", action="store_true")
    parser.add_argument("--state", type=Path, default=STATE)
    args = parser.parse_args(argv)
    dsn = os.environ["CONTROL_DATABASE_URL"]
    resources = load_resources(ROOT / "local/resources.yaml")
    config = resolve_config(
        ROOT / "config/profiles/default.yaml",
        ROOT / "config/bindings/example-local.yaml",
        ROOT / "local/manifests/synthetic-metadata.yaml",
        environment="local",
    )
    repository = PostgresControlRepository(dsn)
    s3 = sqs = None
    if not args.db_only:
        s3, sqs = _clients(os.environ["LOCALSTACK_ENDPOINT_URL"], resources.region)

    if args.stage == "prepare":
        if args.state.exists():
            raise ValueError(f"state already exists: {args.state}; use a fresh state path")
        apply_migrations(dsn, ROOT / "migrations/control")
        scheduled_at = datetime.now(timezone.utc)
        run, jobs = plan_run(config, FeedKind.METADATA, scheduled_at)
        saved = repository.save_plan(config, run, jobs, queue_class="metadata_sweep")
        if saved.new_jobs != len(jobs):
            raise AssertionError("fresh gate plan did not create all jobs")
        marker_key = f"phase2-gate/{run.run_id}.json"
        if s3:
            s3.put_object(
                Bucket=resources.storage.raw_bucket,
                Key=marker_key,
                Body=json.dumps({"run_id": run.run_id}).encode("utf-8"),
            )
        args.state.parent.mkdir(parents=True, exist_ok=True)
        args.state.write_text(json.dumps({
            "run_id": run.run_id,
            "job_ids": [job.job_id for job in jobs],
            "scheduled_at": scheduled_at.isoformat(),
            "marker_key": marker_key if s3 else None,
        }, indent=2), encoding="utf-8")
        print(json.dumps({"stage": "prepared", "run_id": run.run_id, "jobs": len(jobs)}))
        return 0

    state = json.loads(args.state.read_text(encoding="utf-8"))
    run, jobs = plan_run(config, FeedKind.METADATA, datetime.fromisoformat(state["scheduled_at"]))
    if run.run_id != state["run_id"] or {job.job_id for job in jobs} != set(state["job_ids"]):
        raise AssertionError("saved gate state does not match the pinned plan")
    if repository.save_plan(config, run, jobs, queue_class="metadata_sweep").new_jobs != 0:
        raise AssertionError("replanning created duplicate jobs")
    if any(repository.get_job(job.job_id) != job for job in jobs):
        raise AssertionError("committed jobs did not survive restart")
    with psycopg.connect(dsn) as conn:
        rows = conn.execute(
            "SELECT job_id, state FROM ingestion.dispatch_outbox WHERE job_id = ANY(%s)",
            (state["job_ids"],),
        ).fetchall()
    if {row[0] for row in rows} != set(state["job_ids"]) or any(row[1] != "pending" for row in rows):
        raise AssertionError("committed pending dispatch intents did not survive restart")
    if s3:
        if not state["marker_key"]:
            raise AssertionError("LocalStack marker was not prepared")
        marker = json.loads(s3.get_object(
            Bucket=resources.storage.raw_bucket, Key=state["marker_key"],
        )["Body"].read())
        if marker["run_id"] != run.run_id:
            raise AssertionError("LocalStack S3 state did not survive restart")
        sender = SQSMessageSender(
            queue_names=set(resources.work_queues), region_name=resources.region,
            client=sqs,
        )
        result = dispatch_once(
            repository, sender, owner="phase2-gate", limit=len(jobs),
            lease_seconds=resources.dispatch.lease_seconds,
            base_backoff_seconds=resources.dispatch.base_backoff_seconds,
            max_backoff_seconds=resources.dispatch.max_backoff_seconds,
        )
        if (result.claimed, result.sent, result.failed) != (len(jobs), len(jobs), 0):
            raise AssertionError(f"dispatch did not recover all jobs: {result}")
        url = sqs.get_queue_url(QueueName="metadata_sweep")["QueueUrl"]
        messages = sqs.receive_message(
            QueueUrl=url, MaxNumberOfMessages=len(jobs), WaitTimeSeconds=2,
        ).get("Messages", [])
        if {json.loads(message["Body"])["job_id"] for message in messages} != set(state["job_ids"]):
            raise AssertionError("SQS did not receive the expected job references")
        for message in messages:
            sqs.delete_message(QueueUrl=url, ReceiptHandle=message["ReceiptHandle"])
        s3.delete_object(Bucket=resources.storage.raw_bucket, Key=state["marker_key"])
    print(json.dumps({
        "stage": "verified", "run_id": run.run_id,
        "jobs": len(jobs), "localstack": bool(s3),
    }))
    args.state.unlink()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
