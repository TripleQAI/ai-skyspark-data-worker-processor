"""Bounded, idempotent TimescaleDB history batches and durable receipts."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from datetime import timezone

import psycopg

from ingestion.adapters.aws.s3_evidence import EvidenceMismatch, S3EvidenceVerifier
from ingestion.contracts.config import FeedKind
from ingestion.contracts.history import HistoryObservation
from ingestion.contracts.jobs import Job, JobCompletion, SinkReceipt
from ingestion.contracts.resources import StoragePolicy
from ingestion.core.failures import CertifiedBatchTooLarge


def _row(job: Job, observation: HistoryObservation) -> tuple[tuple[object, ...], bytes, str]:
    observed_at = observation.observed_at.astimezone(timezone.utc)
    payload = {
        "point_id": observation.point_id,
        "observed_at": observed_at.isoformat(),
        "value_kind": observation.value_kind,
        "val_bool": observation.val_bool,
        "val_str": observation.val_str,
        "val_num": str(observation.val_num) if observation.val_num is not None else None,
        "val_na": observation.val_na,
    }
    for field in ("source_timestamp", "source_timezone", "source_status"):
        value = getattr(observation, field)
        if value is not None:
            payload[field] = value
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    row_hash = hashlib.sha256(encoded).hexdigest()
    return (
        (
            job.tenant_id, job.project_id, observation.point_id,
            observed_at, observation.value_kind, observation.val_bool,
            observation.val_str, observation.val_num, row_hash, job.job_id,
            observation.source_timestamp, observation.source_timezone,
            observation.source_status,
        ),
        encoded,
        row_hash,
    )


class TimescaleHistorySink:
    def __init__(self, dsn: str, policy: StoragePolicy):
        if not dsn:
            raise ValueError("target database DSN is required")
        self._dsn = dsn
        self._policy = policy

    def put(self, job: Job, observations: Iterable[HistoryObservation]) -> SinkReceipt:
        if job.feed != FeedKind.HISTORY or job.window_start is None or job.window_end is None:
            raise ValueError("Timescale history sink requires a bounded history job")
        approved = set(job.scope_ids)
        prepared: list[tuple[tuple[object, ...], bytes, str]] = []
        seen: set[tuple[str, object]] = set()
        byte_count = 0
        for observation in observations:
            if not isinstance(observation, HistoryObservation):
                raise TypeError("history sink requires typed observations")
            if observation.point_id not in approved or not (
                job.window_start <= observation.observed_at < job.window_end
            ):
                raise ValueError("history observation is outside requested scope/window")
            key = (observation.point_id, observation.observed_at.astimezone(timezone.utc))
            if key in seen:
                raise ValueError("duplicate point/timestamp in history batch")
            seen.add(key)
            prepared_row = _row(job, observation)
            byte_count += len(prepared_row[1]) + 1
            if (
                len(prepared) + 1 > self._policy.max_certified_rows
                or byte_count > self._policy.max_certified_bytes
            ):
                raise CertifiedBatchTooLarge("history batch exceeds configured cap")
            prepared.append(prepared_row)
        prepared.sort(key=lambda item: (item[0][2], item[0][3]))
        digest = hashlib.sha256()
        for _, encoded, _ in prepared:
            digest.update(encoded)
            digest.update(b"\n")
        checksum = digest.hexdigest()
        batch_key = f"history/{job.tenant_id}/{job.project_id}/{job.run_id}/{job.job_id}/{checksum}"
        receipt = SinkReceipt(
            job_id=job.job_id, sink_kind="timescale", batch_key=batch_key,
            row_count=len(prepared), checksum=checksum,
        )

        with psycopg.connect(self._dsn) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "CREATE TEMP TABLE stage_history "
                    "(LIKE ingestion_target.history_observations INCLUDING DEFAULTS) "
                    "ON COMMIT DROP"
                )
                with cursor.copy(
                    "COPY stage_history "
                    "(tenant_id, project_id, point_id, observed_at, value_kind, "
                    "value_bool, value_str, value_num, row_hash, first_job_id, "
                    "source_timestamp, source_timezone, source_status) FROM STDIN"
                ) as copy:
                    for row, _, _ in prepared:
                        copy.write_row(row)
                cursor.execute(
                    """
                    INSERT INTO ingestion_target.history_observations
                        (tenant_id, project_id, point_id, observed_at, value_kind,
                         value_bool, value_str, value_num, row_hash, first_job_id,
                         source_timestamp, source_timezone, source_status)
                    SELECT tenant_id, project_id, point_id, observed_at, value_kind,
                           value_bool, value_str, value_num, row_hash, first_job_id,
                           source_timestamp, source_timezone, source_status
                    FROM stage_history WHERE true
                    ON CONFLICT (tenant_id, project_id, point_id, observed_at) DO NOTHING
                    """
                )
                conflicts = cursor.execute(
                    """
                    SELECT count(*) FROM stage_history AS s
                    JOIN ingestion_target.history_observations AS t
                      ON t.tenant_id = s.tenant_id AND t.project_id = s.project_id
                     AND t.point_id = s.point_id AND t.observed_at = s.observed_at
                    WHERE t.row_hash <> s.row_hash
                    """
                ).fetchone()[0]
                if conflicts:
                    if self._policy.history_correction_policy != "append_revision":
                        raise ValueError("history conflict requires an explicit correction policy")
                    cursor.execute(
                        """INSERT INTO ingestion_target.history_observation_revisions
                           (tenant_id, project_id, point_id, observed_at, row_hash,
                            value_kind, value_bool, value_str, value_num, source_job_id,
                            source_timestamp, source_timezone, source_status)
                           SELECT s.tenant_id, s.project_id, s.point_id, s.observed_at,
                                  s.row_hash, s.value_kind, s.value_bool, s.value_str,
                                  s.value_num, s.first_job_id, s.source_timestamp,
                                  s.source_timezone, s.source_status
                           FROM stage_history AS s
                           JOIN ingestion_target.history_observations AS t
                             ON t.tenant_id = s.tenant_id AND t.project_id = s.project_id
                            AND t.point_id = s.point_id AND t.observed_at = s.observed_at
                           WHERE t.row_hash <> s.row_hash
                           ON CONFLICT DO NOTHING"""
                    )
                cursor.execute(
                    """
                    INSERT INTO ingestion_target.batch_receipts
                        (batch_key, job_id, tenant_id, project_id, feed, row_count, checksum)
                    VALUES (%s, %s, %s, %s, 'history', %s, %s)
                    ON CONFLICT (batch_key) DO NOTHING
                    """,
                    (
                        batch_key, job.job_id, job.tenant_id, job.project_id,
                        len(prepared), checksum,
                    ),
                )
                stored = cursor.execute(
                    """
                    SELECT job_id, tenant_id, project_id, feed, row_count, checksum
                    FROM ingestion_target.batch_receipts WHERE batch_key = %s
                    """,
                    (batch_key,),
                ).fetchone()
                if stored != (
                    job.job_id, job.tenant_id, job.project_id,
                    "history", len(prepared), checksum,
                ):
                    raise ValueError("target batch receipt conflicts with existing data")
        return receipt


class TimescaleEvidenceVerifier:
    def __init__(self, dsn: str, raw_verifier: S3EvidenceVerifier,
                 policy: StoragePolicy | None = None):
        if not dsn:
            raise ValueError("target database DSN is required")
        self._dsn = dsn
        self._raw_verifier = raw_verifier
        self._policy = policy

    def verify(self, job: Job, completion: JobCompletion) -> None:
        if completion.sink.sink_kind != "timescale":
            raise EvidenceMismatch("Timescale verifier requires a Timescale target")
        prefix = (f"{job.feed.value}/{job.tenant_id}/{job.project_id}/"
                  f"{job.run_id}/{job.job_id}/")
        if not completion.sink.batch_key.startswith(prefix):
            raise EvidenceMismatch("Timescale receipt is outside the job prefix")
        self._raw_verifier.verify_raw(job, completion)
        with psycopg.connect(self._dsn) as conn:
            row = conn.execute(
                """
                SELECT job_id, tenant_id, project_id, feed, row_count, checksum
                FROM ingestion_target.batch_receipts WHERE batch_key = %s
                """,
                (completion.sink.batch_key,),
            ).fetchone()
        if row != (
            job.job_id, job.tenant_id, job.project_id,
                job.feed.value, completion.sink.row_count, completion.sink.checksum,
        ):
            raise EvidenceMismatch("Timescale target receipt is missing or mismatched")
        if job.feed in (FeedKind.METADATA, FeedKind.RULES):
            table = ("metadata_batch_entities" if job.feed == FeedKind.METADATA
                     else "rule_batch_detections")
            with psycopg.connect(self._dsn) as conn:
                rows = conn.execute(
                    f"SELECT payload FROM ingestion_target.{table} "
                    "WHERE batch_key = %s ORDER BY position",
                    (completion.sink.batch_key,),
                ).fetchall()
            if len(rows) != completion.sink.row_count or (
                self._policy is not None and len(rows) > self._policy.max_certified_rows
            ):
                raise EvidenceMismatch("Timescale batch rows differ from receipt")
            digest = hashlib.sha256()
            byte_count = 0
            for (payload,) in rows:
                encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"),
                                     allow_nan=False).encode() + b"\n"
                byte_count += len(encoded)
                if self._policy is not None and byte_count > self._policy.max_certified_bytes:
                    raise EvidenceMismatch("Timescale batch exceeds verification limit")
                digest.update(encoded)
            if digest.hexdigest() != completion.sink.checksum:
                raise EvidenceMismatch("Timescale batch checksum differs from receipt")
