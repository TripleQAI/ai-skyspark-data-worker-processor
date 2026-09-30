"""Read-only storage measures and reviewed retention policy preview."""

from __future__ import annotations

from typing import Any

import psycopg

from ingestion.contracts.resources import ResourceConfig


def retention_preview(resources: ResourceConfig) -> dict[str, object]:
    """Render a policy for review; this function never changes AWS or SQL state."""
    policy = resources.retention
    if policy is None:
        raise ValueError("reviewed retention values are not configured")
    if (resources.backfill is not None and
            min(policy.raw_object_days, policy.certified_object_days,
                policy.inventory_object_days) < resources.backfill.max_window_age_days):
        raise ValueError("object retention is shorter than approved replay age")
    bucket_rules: dict[str, list[dict[str, object]]] = {}
    for bucket, prefix, days in (
        (resources.storage.raw_bucket, "raw/", policy.raw_object_days),
        (resources.storage.certified_bucket, "certified/", policy.certified_object_days),
        (resources.storage.certified_bucket, "inventory/", policy.inventory_object_days),
    ):
        bucket_rules.setdefault(bucket, []).append({
            "ID": f"expire-{prefix.rstrip('/')}", "Status": "Enabled",
            "Filter": {"Prefix": prefix}, "Expiration": {"Days": days},
            "NoncurrentVersionExpiration": {"NoncurrentDays": days},
        })
    return {
        "bucket_lifecycle_configurations": {
            bucket: {"Rules": rules} for bucket, rules in bucket_rules.items()
        },
        "history_hypertable_retention_days": policy.history_observation_days,
        "table_retention_requires_review": [
            "ingestion_target.history_observation_revisions",
            "ingestion_target.metadata_entity_revisions",
            "ingestion_target.metadata_batch_entities",
            "ingestion_target.rule_detection_revisions",
            "ingestion_target.rule_batch_detections",
            "ingestion control and publication tables",
        ],
        "applied": False,
    }


def _s3_prefix(client: Any, bucket: str, prefix: str, cap: int) -> dict[str, object]:
    count = size = 0
    token = None
    while count < cap:
        args = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": min(1000, cap - count)}
        if token is not None:
            args["ContinuationToken"] = token
        page = client.list_objects_v2(**args)
        objects = page.get("Contents", ())
        count += len(objects)
        size += sum(item["Size"] for item in objects)
        if not page.get("IsTruncated"):
            return {"objects": count, "bytes": size, "complete": True}
        token = page.get("NextContinuationToken")
        if not token:
            raise ValueError("S3 listing is truncated without a continuation token")
    return {"objects": count, "bytes": size, "complete": False}


def _relations(dsn: str, schema: str) -> list[dict[str, object]]:
    with psycopg.connect(dsn) as conn:
        rows = conn.execute(
            """SELECT relname, n_live_tup::bigint,
                      pg_total_relation_size(relid)::bigint
               FROM pg_stat_user_tables WHERE schemaname = %s ORDER BY relname""",
            (schema,),
        ).fetchall()
    return [{"table": name, "estimated_live_rows": count,
             "relation_bytes": size} for name, count, size in rows]


def measure_storage(
    resources: ResourceConfig, *, s3_client: Any,
    control_dsn: str, target_dsn: str,
) -> dict[str, object]:
    """Report capped object counts and DB relation estimates, without prices."""
    if resources.measurement is None:
        raise ValueError("reviewed measurement cap is required")
    cap = resources.measurement.max_s3_objects_per_prefix
    with psycopg.connect(target_dsn) as conn:
        history_bytes = conn.execute(
            "SELECT hypertable_size('ingestion_target.history_observations'::regclass)"
        ).fetchone()[0]
    return {
        "s3": {
            "raw": _s3_prefix(s3_client, resources.storage.raw_bucket, "raw/", cap),
            "certified": _s3_prefix(s3_client, resources.storage.certified_bucket,
                                     "certified/", cap),
            "inventory": _s3_prefix(s3_client, resources.storage.certified_bucket,
                                     "inventory/", cap),
        },
        "control_relations": _relations(control_dsn, "ingestion"),
        "target_relations": _relations(target_dsn, "ingestion_target"),
        "history_hypertable_bytes": history_bytes,
        "database_row_counts_are_estimates": True,
        "history_hypertable_chunk_bytes_included": True,
    }
