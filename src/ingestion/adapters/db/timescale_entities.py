"""Atomic, bounded metadata and equipment-rule batches in TimescaleDB."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable

import psycopg
from psycopg.types.json import Jsonb

from ingestion.adapters.skyspark.metadata import _reference
from ingestion.adapters.skyspark.rules import _digest, _source_day
from ingestion.contracts.config import FeedKind
from ingestion.contracts.jobs import Job, SinkReceipt
from ingestion.contracts.resources import StoragePolicy
from ingestion.contracts.rules import RuleDetection
from ingestion.core.failures import CertifiedBatchTooLarge, NonRetryableSourceError


def _encoded(value: dict[str, object]) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _bounded(prepared: list[tuple], policy: StoragePolicy) -> str:
    if len(prepared) > policy.max_certified_rows:
        raise CertifiedBatchTooLarge("target batch exceeds configured row cap")
    digest = hashlib.sha256()
    byte_count = 0
    for row in prepared:
        encoded = row[-1]
        byte_count += len(encoded) + 1
        if byte_count > policy.max_certified_bytes:
            raise CertifiedBatchTooLarge("target batch exceeds configured byte cap")
        digest.update(encoded)
        digest.update(b"\n")
    return digest.hexdigest()


def _receipt(cursor: psycopg.Cursor, job: Job, *, batch_key: str,
             row_count: int, checksum: str) -> SinkReceipt:
    cursor.execute(
        """INSERT INTO ingestion_target.batch_receipts
           (batch_key, job_id, tenant_id, project_id, feed, row_count, checksum)
           VALUES (%s, %s, %s, %s, %s, %s, %s)
           ON CONFLICT (batch_key) DO NOTHING""",
        (batch_key, job.job_id, job.tenant_id, job.project_id, job.feed.value,
         row_count, checksum),
    )
    stored = cursor.execute(
        """SELECT job_id, tenant_id, project_id, feed, row_count, checksum
           FROM ingestion_target.batch_receipts WHERE batch_key = %s""",
        (batch_key,),
    ).fetchone()
    if stored != (job.job_id, job.tenant_id, job.project_id, job.feed.value,
                  row_count, checksum):
        raise ValueError("target batch receipt conflicts with existing data")
    return SinkReceipt(job_id=job.job_id, sink_kind="timescale",
                       batch_key=batch_key, row_count=row_count,
                       checksum=checksum)


class TimescaleMetadataSink:
    def __init__(self, dsn: str, policy: StoragePolicy):
        if not dsn:
            raise ValueError("target database DSN is required")
        self._dsn = dsn
        self._policy = policy

    def put(self, job: Job, rows: Iterable[dict[str, object]]) -> SinkReceipt:
        if job.feed != FeedKind.METADATA or job.scope_ids:
            raise ValueError("metadata sink requires one site job")
        prepared: list[tuple[str, str, str, dict[str, object], bytes]] = []
        seen: set[tuple[str, str]] = set()
        for item in rows:
            if not isinstance(item, dict) or item.get("kind") not in ("equipment", "point"):
                raise NonRetryableSourceError("metadata target requires normalized entities")
            kind = item["kind"]
            source_id = item.get("source_id")
            tags = item.get("tags")
            if (not isinstance(source_id, str) or not source_id
                    or item.get("site_ref") != job.site_ref
                    or not isinstance(tags, dict)
                    or _reference(tags.get("id")) != source_id
                    or type(item.get("historized")) is not bool):
                raise NonRetryableSourceError("metadata target entity is outside site scope")
            if kind == "equipment" and (item.get("equipment_ref") is not None
                                        or item["historized"] or "site_level" in item):
                raise NonRetryableSourceError("equipment target has point-only fields")
            if kind == "point" and (item.get("site_level") is not (item.get("equipment_ref") is None)
                                    or (item.get("equipment_ref") is not None
                                        and not isinstance(item["equipment_ref"], str))):
                raise NonRetryableSourceError("point target has invalid equipment link")
            key = (kind, source_id)
            if key in seen:
                raise NonRetryableSourceError("duplicate metadata entity in one batch")
            seen.add(key)
            encoded = _encoded(item)
            prepared.append((kind, source_id, hashlib.sha256(encoded).hexdigest(), item, encoded))
        prepared.sort(key=lambda row: (row[0], row[1]))
        checksum = _bounded(prepared, self._policy)
        batch_key = f"metadata/{job.tenant_id}/{job.project_id}/{job.run_id}/{job.job_id}/{checksum}"
        with psycopg.connect(self._dsn) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""CREATE TEMP TABLE stage_metadata (
                    position integer, kind text, source_id text, row_hash text,
                    payload jsonb) ON COMMIT DROP""")
                with cursor.copy("COPY stage_metadata FROM STDIN") as copy:
                    for position, (kind, source_id, row_hash, payload, _) in enumerate(prepared):
                        copy.write_row((position, kind, source_id, row_hash, Jsonb(payload)))
                cursor.execute(
                    """INSERT INTO ingestion_target.metadata_entity_revisions
                       (tenant_id, project_id, site_ref, kind, source_id, row_hash,
                        equipment_ref, historized, payload, first_job_id)
                       SELECT %s, %s, %s, kind, source_id, row_hash,
                              payload->>'equipment_ref', (payload->>'historized')::boolean,
                              payload, %s FROM stage_metadata
                       ON CONFLICT DO NOTHING""",
                    (job.tenant_id, job.project_id, job.site_ref, job.job_id),
                )
                cursor.execute(
                    """INSERT INTO ingestion_target.metadata_batch_entities
                       (batch_key, position, kind, source_id, row_hash, payload)
                       SELECT %s, position, kind, source_id, row_hash, payload
                       FROM stage_metadata ON CONFLICT DO NOTHING""", (batch_key,),
                )
                receipt = _receipt(cursor, job, batch_key=batch_key,
                                   row_count=len(prepared), checksum=checksum)
        return receipt


class TimescaleRulesSink:
    def __init__(self, dsn: str, policy: StoragePolicy):
        if not dsn:
            raise ValueError("target database DSN is required")
        self._dsn = dsn
        self._policy = policy

    def put(self, job: Job, detections: Iterable[RuleDetection]) -> SinkReceipt:
        if job.feed != FeedKind.RULES or job.window_start is None or job.window_end is None:
            raise ValueError("rules sink requires a bounded equipment job")
        prepared: list[tuple[RuleDetection, dict[str, object], bytes]] = []
        seen: set[str] = set()
        for detection in detections:
            if not isinstance(detection, RuleDetection):
                raise TypeError("rules sink requires typed detections")
            identity = {
                "tenant_id": job.tenant_id, "project_id": job.project_id,
                "site_ref": job.site_ref, "equipment_id": detection.equipment_id,
                "rule_id": detection.rule_id,
                "source_date": detection.source_date.isoformat(),
                "source_timezone": detection.source_timezone,
            }
            if ((detection.tenant_id, detection.project_id, detection.site_ref)
                    != (job.tenant_id, job.project_id, job.site_ref)
                    or detection.equipment_id not in job.scope_ids
                    or detection.window_start != job.window_start
                    or detection.window_end != job.window_end
                    or _source_day(job, detection.source_timezone) != detection.source_date
                    or _digest(identity) != detection.detection_key
                    or _digest(detection.tags) != detection.revision_hash
                    or detection.detection_key in seen):
                raise NonRetryableSourceError("rule target has invalid scope or revision identity")
            seen.add(detection.detection_key)
            payload = detection.model_dump(mode="json")
            prepared.append((detection, payload, _encoded(payload)))
        prepared.sort(key=lambda row: row[0].detection_key)
        checksum = _bounded(prepared, self._policy)
        batch_key = f"rules/{job.tenant_id}/{job.project_id}/{job.run_id}/{job.job_id}/{checksum}"
        with psycopg.connect(self._dsn) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""CREATE TEMP TABLE stage_rules (
                    position integer, source_date date, detection_key text,
                    revision_hash text, equipment_id text, rule_id text,
                    payload jsonb) ON COMMIT DROP""")
                with cursor.copy("COPY stage_rules FROM STDIN") as copy:
                    for position, (row, payload, _) in enumerate(prepared):
                        copy.write_row((position, row.source_date, row.detection_key,
                                        row.revision_hash, row.equipment_id,
                                        row.rule_id, Jsonb(payload)))
                cursor.execute(
                    """INSERT INTO ingestion_target.rule_detection_revisions
                       (tenant_id, project_id, site_ref, source_date, detection_key,
                        revision_hash, equipment_id, rule_id, payload, first_job_id)
                       SELECT %s, %s, %s, source_date, detection_key, revision_hash,
                              equipment_id, rule_id, payload, %s FROM stage_rules
                       ON CONFLICT DO NOTHING""",
                    (job.tenant_id, job.project_id, job.site_ref, job.job_id),
                )
                cursor.execute(
                    """INSERT INTO ingestion_target.rule_batch_detections
                       (batch_key, position, source_date, detection_key, revision_hash, payload)
                       SELECT %s, position, source_date, detection_key, revision_hash, payload
                       FROM stage_rules ON CONFLICT DO NOTHING""", (batch_key,),
                )
                receipt = _receipt(cursor, job, batch_key=batch_key,
                                   row_count=len(prepared), checksum=checksum)
        return receipt
