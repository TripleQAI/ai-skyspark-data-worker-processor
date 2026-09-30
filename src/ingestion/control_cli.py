"""Explicit migration, planning, and dispatch commands for local or AWS runs."""

from __future__ import annotations

import argparse
import json
import os
import signal
import threading
import boto3
from uuid import uuid4
from datetime import datetime
from pathlib import Path

from ingestion.adapters.aws.sqs import SQSMessageSender
from ingestion.adapters.aws.eventbridge import EventBridgePublicationSender
from ingestion.adapters.aws.sqs_consumer import SQSQueueTransport
from ingestion.adapters.aws.ecs_task_protection import ECSAgentTaskProtection
from ingestion.adapters.aws.s3_evidence import S3EvidenceVerifier, S3ObjectStore
from ingestion.adapters.aws.inventory import S3VersionedInventoryStore
from ingestion.adapters.scripts import ScopedRegisteredScriptHandler
from ingestion.adapters.skyspark.metadata_exports import assess_metadata_exports
from ingestion.adapters.control.migrations import apply_migrations
from ingestion.adapters.control.postgres import PostgresControlRepository
from ingestion.adapters.control.checkpoints import PostgresCheckpointReconciler
from ingestion.adapters.control.source_permits import PostgresSourcePermitPool
from ingestion.adapters.control.publications import PostgresPublicationRepository
from ingestion.adapters.control.run_status import PostgresRunStatusReader
from ingestion.adapters.control.config_registry import PostgresConfigRegistry
from ingestion.adapters.control.inventory import PostgresInventoryRegistry
from ingestion.adapters.control.metadata_inventory import PostgresMetadataInventoryPublisher
from ingestion.adapters.db.timescale import TimescaleEvidenceVerifier
from ingestion.config.loader import resolve_config
from ingestion.config.versioned_s3 import _unique_object
from ingestion.contracts.config import FeedKind, TargetKind
from ingestion.contracts.jobs import CertifiedInventory
from ingestion.contracts.resources import load_resources
from ingestion.contracts.replay import ReplayRequest
from ingestion.contracts.scheduled import ScheduledTrigger
from ingestion.core.dispatch import dispatch_once
from ingestion.core.control_loop import RecoveryBatch, serve_control_loop
from ingestion.core.evidence import ScopedFeedRoutedEvidenceVerifier
from ingestion.core.job_config import ScopedJobConfigResolver
from ingestion.core.planner import plan_run
from ingestion.core.scheduled_service import persist_scheduled_trigger
from ingestion.core.publication import publish_once
from ingestion.core.replay import persist_replay_request
from ingestion.core.storage_operations import measure_storage, retention_preview
from ingestion.core.source_gate import PermitGuardedHandler
from ingestion.core.task_protection import TaskProtectionManager
from ingestion.core.worker import QueueWorker


def _timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="skyspark-control")
    subcommands = parser.add_subparsers(dest="command", required=True)

    migration = subcommands.add_parser("migrate")
    migration.add_argument("--migrations", type=Path, required=True)
    target_migration = subcommands.add_parser("migrate-target")
    target_migration.add_argument("--migrations", type=Path, required=True)

    persist = subcommands.add_parser("persist-plan")
    persist.add_argument("--profile", type=Path, required=True)
    persist.add_argument("--binding", type=Path, required=True)
    persist.add_argument("--manifest", type=Path, required=True)
    persist.add_argument("--environment", choices=("local", "aws"), required=True)
    persist.add_argument("--resources", type=Path, required=True)
    persist.add_argument("--feed", choices=[kind.value for kind in FeedKind], required=True)
    persist.add_argument("--scheduled-at", type=_timestamp, required=True)
    persist.add_argument("--inventory", type=Path)
    persist.add_argument("--window-start", type=_timestamp)
    persist.add_argument("--window-end", type=_timestamp)

    scheduled = subcommands.add_parser("persist-scheduled")
    scheduled.add_argument("--environment", choices=("local", "aws"), required=True)
    scheduled.add_argument("--resources", type=Path, required=True)
    scheduled.add_argument("--trigger-file", type=Path)

    replay = subcommands.add_parser("request-replay")
    replay.add_argument("--environment", choices=("local", "aws"), required=True)
    replay.add_argument("--resources", type=Path, required=True)
    replay.add_argument("--request-file", type=Path, required=True)

    retention = subcommands.add_parser("retention-preview")
    retention.add_argument("--resources", type=Path, required=True)

    storage = subcommands.add_parser("measure-storage")
    storage.add_argument("--environment", choices=("local", "aws"), required=True)
    storage.add_argument("--resources", type=Path, required=True)

    dispatch = subcommands.add_parser("dispatch-once")
    dispatch.add_argument("--resources", type=Path, required=True)
    dispatch.add_argument("--owner", required=True)

    for command in ("dispatch-service", "publication-service", "recovery-service"):
        service = subcommands.add_parser(command)
        service.add_argument("--environment", choices=("local", "aws"), required=True)
        service.add_argument("--resources", type=Path, required=True)
        if command != "recovery-service":
            service.add_argument("--owner")

    recover = subcommands.add_parser("recover-stale")
    recover.add_argument("--resources", type=Path, required=True)
    recover.add_argument("--run-id")

    publish = subcommands.add_parser("publish-once")
    publish.add_argument("--resources", type=Path, required=True)
    publish.add_argument("--owner", required=True)

    inspect = subcommands.add_parser("inspect-metadata-export")
    inspect.add_argument("--equipment", type=Path, required=True)
    inspect.add_argument("--points", type=Path, required=True)
    inspect.add_argument("--resources", type=Path, required=True)
    inspect.add_argument("--project-key", required=True)
    inspect.add_argument("--site-ref", action="append", required=True)

    inventory_publish = subcommands.add_parser("publish-metadata-inventory")
    inventory_publish.add_argument("--run-id", required=True)
    inventory_publish.add_argument("--tenant-id", required=True)
    inventory_publish.add_argument("--project-id", required=True)
    inventory_publish.add_argument("--config-hash", required=True)
    inventory_publish.add_argument("--environment", choices=("local", "aws"), required=True)
    inventory_publish.add_argument("--resources", type=Path, required=True)

    seed = subcommands.add_parser("seed-checkpoint")
    seed.add_argument("--profile", type=Path, required=True)
    seed.add_argument("--binding", type=Path, required=True)
    seed.add_argument("--manifest", type=Path, required=True)
    seed.add_argument("--environment", choices=("local", "aws"), required=True)
    seed.add_argument("--feed", choices=("history", "rules"), required=True)
    seed.add_argument("--site-ref", required=True)
    seed.add_argument("--start-at", type=_timestamp, required=True)

    reconcile = subcommands.add_parser("reconcile-run")
    reconcile.add_argument("--run-id", required=True)

    status = subcommands.add_parser("run-status")
    status.add_argument("--run-id", required=True)
    status.add_argument("--tenant-id", required=True)
    status.add_argument("--project-id", required=True)
    status.add_argument("--config-hash", required=True)
    status.add_argument("--feed", choices=[kind.value for kind in FeedKind], required=True)
    status.add_argument("--resources", type=Path, required=True)

    worker = subcommands.add_parser("worker")
    worker.add_argument("--environment", choices=("local", "aws"), required=True)
    worker.add_argument("--resources", type=Path, required=True)
    worker.add_argument("--script-root", type=Path, required=True)
    worker.add_argument("--queue-class", required=True)
    worker.add_argument("--worker-id")
    worker.add_argument("--once", action="store_true")

    args = parser.parse_args(argv)
    if args.command == "inspect-metadata-export":
        resources = load_resources(args.resources)
        assessment = assess_metadata_exports(
            args.equipment, args.points,
            project_key=args.project_key,
            expected_site_refs=set(args.site_ref),
            policy=resources.metadata_inspection,
        )
        print(json.dumps(assessment.summary(), sort_keys=True))
        return 2 if assessment.issue_counts else 0
    if args.command == "migrate-target":
        target_dsn = os.environ.get("TARGET_DATABASE_URL")
        if not target_dsn:
            parser.error("TARGET_DATABASE_URL must be set")
        applied = apply_migrations(
            target_dsn, args.migrations, registry="target_schema_migrations"
        )
        print(json.dumps({"applied_migrations": applied}))
        return 0
    if args.command == "retention-preview":
        print(json.dumps(retention_preview(load_resources(args.resources)), sort_keys=True))
        return 0
    dsn = os.environ.get("CONTROL_DATABASE_URL")
    if not dsn:
        parser.error("CONTROL_DATABASE_URL must be set")

    if args.command == "migrate":
        applied = apply_migrations(dsn, args.migrations)
        print(json.dumps({"applied_migrations": applied}))
        return 0

    if args.command == "seed-checkpoint":
        config = resolve_config(
            args.profile, args.binding, args.manifest, environment=args.environment
        )
        PostgresCheckpointReconciler(dsn).seed_checkpoint(
            config, FeedKind(args.feed), args.site_ref, args.start_at
        )
        print(json.dumps({"site_ref": args.site_ref, "feed": args.feed,
                          "start_at": args.start_at.isoformat()}))
        return 0
    if args.command == "reconcile-run":
        reconciler = PostgresCheckpointReconciler(dsn)
        results = reconciler.reconcile_run(args.run_id)
        if not results:
            parser.error("run has no jobs or does not exist")
        print(json.dumps([{
            "run_id": item.run_id, "site_ref": item.site_ref,
            "state": item.state, "job_count": item.job_count,
        } for item in results]))
        return 0 if all(item.state in {"advanced", "already_advanced"} for item in results) else 2

    resources = load_resources(args.resources)
    if args.command == "measure-storage":
        target_dsn = os.environ.get("TARGET_DATABASE_URL")
        if not target_dsn:
            parser.error("TARGET_DATABASE_URL must be set for storage measurement")
        endpoint = os.environ.get("AWS_ENDPOINT_URL") if args.environment == "local" else None
        result = measure_storage(
            resources,
            s3_client=boto3.client("s3", region_name=resources.region,
                                   endpoint_url=endpoint),
            control_dsn=dsn, target_dsn=target_dsn,
        )
        print(json.dumps(result, sort_keys=True))
        return 0
    if args.command == "request-replay":
        raw = args.request_file.read_bytes()
        if len(raw) > 32_768:
            parser.error("replay request exceeds the reviewed size cap")
        request = ReplayRequest.model_validate(
            json.loads(raw, object_pairs_hook=_unique_object)
        )
        config = PostgresConfigRegistry(dsn, environment=args.environment).load(
            config_hash=request.config_hash, tenant_id=request.tenant_id,
            project_id=request.project_id,
        )
        record = PostgresInventoryRegistry(dsn).load_exact(
            tenant_id=request.tenant_id, project_id=request.project_id,
            version=request.inventory_version,
        )
        endpoint = os.environ.get("AWS_ENDPOINT_URL") if args.environment == "local" else None
        inventory = S3VersionedInventoryStore(
            region_name=resources.region, policy=resources.inventory_artifacts,
            endpoint_url=endpoint,
        ).load(record, config=config)
        saved = persist_replay_request(
            request, config=config, inventory=inventory,
            resources=resources, repository=PostgresControlRepository(dsn),
        )
        print(json.dumps({
            "request_id": str(request.request_id), "run_id": saved.run_id,
            "expected_jobs": saved.expected_jobs, "new_jobs": saved.new_jobs,
            "queue_class": resources.backfill_queue,
        }, sort_keys=True))
        return 0
    if args.command == "publish-metadata-inventory":
        config = PostgresConfigRegistry(dsn, environment=args.environment).load(
            config_hash=args.config_hash, tenant_id=args.tenant_id,
            project_id=args.project_id,
        )
        endpoint = os.environ.get("AWS_ENDPOINT_URL") if args.environment == "local" else None
        record = PostgresMetadataInventoryPublisher(
            dsn,
            objects=S3ObjectStore(region_name=resources.region, endpoint_url=endpoint),
            inventory_store=S3VersionedInventoryStore(
                region_name=resources.region, policy=resources.inventory_artifacts,
                endpoint_url=endpoint,
            ),
            resources=resources,
            target_dsn=os.environ.get("TARGET_DATABASE_URL"),
        ).publish(run_id=args.run_id, config=config)
        print(json.dumps({
            "inventory_version": record.version, "source_run_id": record.source_run_id,
            "object_ref": record.object_ref, "certified_at": record.certified_at.isoformat(),
        }, sort_keys=True))
        return 0
    repository = PostgresControlRepository(dsn)
    if args.command in {"dispatch-service", "publication-service", "recovery-service"}:
        stop = threading.Event()
        signal.signal(signal.SIGTERM, lambda *_: stop.set())
        signal.signal(signal.SIGINT, lambda *_: stop.set())
        endpoint = os.environ.get("AWS_ENDPOINT_URL") if args.environment == "local" else None
        if args.command == "dispatch-service":
            policy = resources.dispatch
            owner = args.owner or f"dispatcher-{uuid4().hex}"
            sender = SQSMessageSender(
                queue_names=set(resources.work_queues), region_name=resources.region,
                endpoint_url=endpoint,
            )

            def run_batch():
                return dispatch_once(
                    repository, sender, owner=owner, limit=policy.batch_limit,
                    lease_seconds=policy.lease_seconds,
                    base_backoff_seconds=policy.base_backoff_seconds,
                    max_backoff_seconds=policy.max_backoff_seconds,
                )

            idle_seconds = resources.control_loop.dispatch_idle_seconds
        elif args.command == "publication-service":
            policy = resources.publication
            owner = args.owner or f"publisher-{uuid4().hex}"
            sender = EventBridgePublicationSender(
                event_bus=policy.event_bus, source=policy.source,
                detail_type=policy.detail_type, region_name=resources.region,
                endpoint_url=endpoint,
            )
            publication_repository = PostgresPublicationRepository(dsn)

            def run_batch():
                return publish_once(
                    publication_repository, sender, owner=owner,
                    limit=policy.batch_limit, lease_seconds=policy.lease_seconds,
                    base_backoff_seconds=policy.base_backoff_seconds,
                    max_backoff_seconds=policy.max_backoff_seconds,
                )

            idle_seconds = resources.control_loop.publication_idle_seconds
        else:
            policy = resources.recovery

            def run_batch():
                recovered = repository.requeue_stale(
                    limit=policy.batch_limit,
                    expired_running_after_seconds=policy.expired_running_after_seconds,
                    never_started_after_seconds=policy.never_started_after_seconds,
                    max_redrives=policy.max_redrives,
                )
                return RecoveryBatch(claimed=len(recovered), recovered=len(recovered))

            idle_seconds = resources.control_loop.recovery_idle_seconds

        serve_control_loop(
            role=args.command, run_batch=run_batch, stop=stop,
            batch_limit=policy.batch_limit, idle_seconds=idle_seconds,
        )
        return 0
    if args.command == "run-status":
        feed = FeedKind(args.feed)
        progress = PostgresRunStatusReader(dsn).read(
            run_id=args.run_id, tenant_id=args.tenant_id,
            project_id=args.project_id, config_hash=args.config_hash,
            feed=feed, max_run_seconds=resources.workflow.max_run_seconds[feed],
        )
        print(json.dumps(progress.summary(), sort_keys=True))
        return 0
    if args.command == "persist-scheduled":
        if args.trigger_file:
            if args.trigger_file.stat().st_size > resources.config_artifacts.max_bundle_bytes:
                parser.error("scheduled trigger exceeds configured byte limit")
            trigger_bytes = args.trigger_file.read_bytes()
        else:
            trigger_json = os.environ.get("SCHEDULED_TRIGGER_JSON")
            if not trigger_json:
                parser.error("--trigger-file or SCHEDULED_TRIGGER_JSON is required")
            trigger_bytes = trigger_json.encode("utf-8")
            if len(trigger_bytes) > resources.config_artifacts.max_bundle_bytes:
                parser.error("scheduled trigger exceeds configured byte limit")
        trigger = ScheduledTrigger.model_validate_json(trigger_bytes)
        endpoint = os.environ.get("AWS_ENDPOINT_URL") if args.environment == "local" else None
        result = persist_scheduled_trigger(
            trigger, resources=resources, environment=args.environment,
            dsn=dsn, endpoint_url=endpoint, repository=repository,
        )
        print(json.dumps(result, sort_keys=True))
        return 0
    if args.command == "recover-stale":
        policy = resources.recovery
        recovered = repository.requeue_stale(
            limit=policy.batch_limit,
            expired_running_after_seconds=policy.expired_running_after_seconds,
            never_started_after_seconds=policy.never_started_after_seconds,
            max_redrives=policy.max_redrives,
            run_id=args.run_id,
        )
        print(json.dumps([{
            "job_id": item.job_id, "run_id": item.run_id,
            "queue_class": item.queue_class,
            "reason": item.reason, "redrive_count": item.redrive_count,
        } for item in recovered]))
        return 0
    if args.command == "persist-plan":
        config = resolve_config(
            args.profile, args.binding, args.manifest, environment=args.environment
        )
        inventory = None
        if args.inventory:
            inventory = CertifiedInventory.model_validate_json(
                args.inventory.read_text(encoding="utf-8")
            )
        feed = FeedKind(args.feed)
        run, jobs = plan_run(
            config, feed, args.scheduled_at, inventory=inventory,
            window_start=args.window_start, window_end=args.window_end,
        )
        saved = repository.save_plan(
            config, run, jobs, queue_class=resources.feed_routes[feed]
        )
        print(json.dumps({
            "run_id": saved.run_id,
            "config_hash": config.config_hash,
            "expected_jobs": saved.expected_jobs,
            "new_jobs": saved.new_jobs,
            "queue_class": resources.feed_routes[feed],
        }, sort_keys=True))
        return 0

    if args.command == "worker":
        if args.queue_class not in resources.work_queues:
            parser.error("worker queue class is not approved in resources")
        selected_feeds = {
            feed for feed, route in resources.feed_routes.items()
            if route == args.queue_class
        }
        if args.queue_class == resources.backfill_queue:
            selected_feeds = set(FeedKind)
            if resources.backfill is None:
                parser.error("backfill queue needs reviewed source and worker limits")
        if not selected_feeds:
            parser.error("worker queue has no configured feed route")
        policy = resources.worker
        worker_id = args.worker_id or f"worker-{uuid4().hex}"
        resolver = ScopedJobConfigResolver(
            PostgresConfigRegistry(dsn, environment=args.environment),
            cache_limit=policy.config_cache_entries,
        )
        script_runner = ScopedRegisteredScriptHandler(
            resolver, args.script_root, cache_limit=policy.config_cache_entries,
        )
        handler = PermitGuardedHandler(
            script_runner, PostgresSourcePermitPool(
                dsn, max_slot_no=(resources.backfill.max_source_calls_per_project
                                  if args.queue_class == resources.backfill_queue else None)),
            worker_id=worker_id,
            lease_seconds=policy.source_permit_lease_seconds,
            heartbeat_seconds=policy.source_permit_heartbeat_seconds,
            retry_seconds=policy.source_permit_retry_seconds,
        )
        s3_verifier = S3EvidenceVerifier(
            S3ObjectStore(
                region_name=resources.region,
                endpoint_url=os.environ.get("AWS_ENDPOINT_URL") if args.environment == "local" else None,
            ),
            resources.storage,
        )
        target_dsn = os.environ.get("TARGET_DATABASE_URL")
        verifiers = {TargetKind.S3: s3_verifier}
        if target_dsn:
            verifiers[TargetKind.TIMESCALE] = TimescaleEvidenceVerifier(
                target_dsn, s3_verifier, resources.storage)
        verifier = ScopedFeedRoutedEvidenceVerifier(resolver, verifiers)
        queue = SQSQueueTransport(
            queue_names=set(resources.work_queues),
            region_name=resources.region,
            endpoint_url=os.environ.get("AWS_ENDPOINT_URL") if args.environment == "local" else None,
        )
        protection = None
        if policy.scale_in_protection.enabled:
            agent_uri = os.environ.get("ECS_AGENT_URI")
            if not agent_uri:
                parser.error("ECS_AGENT_URI is required when task protection is enabled")
            protection = TaskProtectionManager(
                ECSAgentTaskProtection(
                    agent_uri,
                    timeout_seconds=policy.scale_in_protection.http_timeout_seconds,
                ),
                expires_minutes=policy.scale_in_protection.expires_minutes,
                refresh_seconds=policy.scale_in_protection.refresh_seconds,
            )
        try:
            processor = QueueWorker(
                repository, queue, handler, verifier, queue_class=args.queue_class,
                worker_id=worker_id, allowed_feeds=selected_feeds,
                slots=(resources.backfill.job_slots_per_task
                       if args.queue_class == resources.backfill_queue else policy.job_slots),
                batch_size=policy.sqs_batch_size,
                wait_seconds=policy.long_poll_seconds,
                visibility_seconds=policy.visibility_seconds,
                lease_seconds=policy.lease_seconds,
                heartbeat_seconds=policy.heartbeat_seconds,
                task_protection=protection,
                history_split_depth=(resources.history_read.max_split_depth
                                     if resources.history_read else 0),
                history_min_split_window_seconds=(resources.history_read.min_split_window_seconds
                                                  if resources.history_read else 30),
                history_max_descendant_jobs=(resources.history_read.max_descendant_jobs
                                             if resources.history_read else 8192),
            )
            if args.once:
                outcomes = processor.run_once()
                print(json.dumps([{
                    "job_id": outcome.job_id, "state": outcome.state
                } for outcome in outcomes]))
                return 0
            stop = threading.Event()
            signal.signal(signal.SIGTERM, lambda *_: stop.set())
            signal.signal(signal.SIGINT, lambda *_: stop.set())
            processor.serve_forever(stop)
            return 0
        finally:
            if protection is not None:
                protection.close()

    if args.command == "publish-once":
        policy = resources.publication
        sender = EventBridgePublicationSender(
            event_bus=policy.event_bus, source=policy.source,
            detail_type=policy.detail_type, region_name=resources.region,
            endpoint_url=os.environ.get("AWS_ENDPOINT_URL"),
        )
        result = publish_once(
            PostgresPublicationRepository(dsn), sender,
            owner=args.owner, limit=policy.batch_limit,
            lease_seconds=policy.lease_seconds,
            base_backoff_seconds=policy.base_backoff_seconds,
            max_backoff_seconds=policy.max_backoff_seconds,
        )
        print(json.dumps({"claimed": result.claimed,
                          "delivered": result.delivered, "failed": result.failed}))
        return 0 if result.failed == 0 else 1

    sender = SQSMessageSender(
        queue_names=set(resources.work_queues),
        region_name=resources.region,
        endpoint_url=os.environ.get("AWS_ENDPOINT_URL"),
    )
    policy = resources.dispatch
    result = dispatch_once(
        repository, sender, owner=args.owner, limit=policy.batch_limit,
        lease_seconds=policy.lease_seconds,
        base_backoff_seconds=policy.base_backoff_seconds,
        max_backoff_seconds=policy.max_backoff_seconds,
    )
    print(json.dumps({"claimed": result.claimed, "sent": result.sent, "failed": result.failed}))
    return 0 if result.failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
