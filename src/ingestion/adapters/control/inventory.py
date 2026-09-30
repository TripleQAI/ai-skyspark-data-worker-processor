"""Durable, scope-bound inventory selection for scheduled history/rules runs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

import psycopg
from psycopg.rows import dict_row

from ingestion.contracts.config import FeedKind


@dataclass(frozen=True, slots=True)
class InventoryRecord:
    tenant_id: str
    project_id: str
    version: str
    source_run_id: str
    object_ref: str
    object_sha256: str
    byte_count: int
    certified_at: datetime


class PostgresInventoryRegistry:
    def __init__(self, dsn: str):
        if not dsn:
            raise ValueError("control database DSN is required")
        self._dsn = dsn

    def load_exact(self, *, tenant_id: str, project_id: str,
                   version: str) -> InventoryRecord:
        """Resolve one still-certified historical snapshot for explicit replay."""
        if not tenant_id or not project_id or not version:
            raise ValueError("replay inventory scope and version are required")
        with psycopg.connect(self._dsn, row_factory=dict_row) as conn:
            conn.execute("SET TRANSACTION READ ONLY")
            row = conn.execute(
                """SELECT versions.* FROM ingestion.inventory_versions AS versions
                   JOIN ingestion.runs AS source ON source.run_id = versions.source_run_id
                   WHERE versions.tenant_id = %s AND versions.project_id = %s
                     AND versions.inventory_version = %s
                     AND source.feed = 'metadata' AND source.status = 'certified'
                     AND source.tenant_id = versions.tenant_id
                     AND source.project_id = versions.project_id
                     AND NOT EXISTS (
                       SELECT 1 FROM ingestion.jobs AS jobs
                       LEFT JOIN ingestion.certifications AS cert ON cert.job_id = jobs.job_id
                       LEFT JOIN ingestion.site_run_completions AS done
                         ON done.run_id = jobs.run_id AND done.site_ref = jobs.site_ref
                       WHERE jobs.run_id = source.run_id
                         AND (jobs.status <> 'certified' OR cert.job_id IS NULL
                              OR done.run_id IS NULL))""",
                (tenant_id, project_id, version),
            ).fetchone()
        if row is None:
            raise ValueError("replay inventory is missing or no longer certified")
        return InventoryRecord(
            tenant_id=row["tenant_id"], project_id=row["project_id"],
            version=row["inventory_version"], source_run_id=row["source_run_id"],
            object_ref=row["object_ref"], object_sha256=row["object_sha256"],
            byte_count=row["byte_count"], certified_at=row["certified_at"],
        )

    def pin_for_due(
        self, *, tenant_id: str, project_id: str, feed: FeedKind,
        scheduled_at: datetime, config_hash: str, max_age_hours: int,
    ) -> InventoryRecord:
        """Atomically reuse or select one eligible metadata snapshot for a due time."""
        if feed not in (FeedKind.HISTORY, FeedKind.RULES):
            raise ValueError("only history/rules use certified inventory")
        if scheduled_at.tzinfo is None or scheduled_at.utcoffset() is None:
            raise ValueError("scheduled_at must have a timezone")
        if max_age_hours < 1:
            raise ValueError("max_age_hours must be positive")
        due = scheduled_at.astimezone(timezone.utc)
        identity = (tenant_id, project_id, feed.value, due, config_hash)
        with psycopg.connect(self._dsn, row_factory=dict_row) as conn:
            pinned = conn.execute(
                """
                SELECT inventory_version FROM ingestion.scheduled_inventory_pins
                WHERE tenant_id = %s AND project_id = %s AND feed = %s
                  AND scheduled_at = %s AND config_hash = %s
                FOR UPDATE
                """,
                identity,
            ).fetchone()
            if pinned is None:
                candidate = conn.execute(
                    """
                    SELECT versions.inventory_version
                    FROM ingestion.inventory_versions AS versions
                    JOIN ingestion.runs AS source ON source.run_id = versions.source_run_id
                    WHERE versions.tenant_id = %s AND versions.project_id = %s
                      AND source.tenant_id = versions.tenant_id
                      AND source.project_id = versions.project_id
                      AND source.feed = 'metadata' AND source.status = 'certified'
                      AND source.scheduled_at <= %s
                      AND EXISTS (
                          SELECT 1 FROM ingestion.site_run_completions AS done
                          WHERE done.run_id = source.run_id
                      )
                      AND NOT EXISTS (
                          SELECT 1 FROM ingestion.jobs AS jobs
                          LEFT JOIN ingestion.certifications AS cert
                            ON cert.job_id = jobs.job_id
                          LEFT JOIN ingestion.site_run_completions AS done
                            ON done.run_id = jobs.run_id AND done.site_ref = jobs.site_ref
                          WHERE jobs.run_id = source.run_id
                            AND (jobs.status <> 'certified'
                                 OR cert.job_id IS NULL OR done.run_id IS NULL)
                      )
                      AND versions.certified_at <= %s
                      AND versions.certified_at <= now()
                      AND versions.certified_at >= %s - (%s * interval '1 hour')
                      AND versions.certified_at >= (
                          SELECT max(done.completed_at)
                          FROM ingestion.site_run_completions AS done
                          WHERE done.run_id = source.run_id
                      )
                    ORDER BY versions.certified_at DESC, versions.inventory_version DESC
                    LIMIT 1
                    """,
                    (tenant_id, project_id, due, due, due, max_age_hours),
                ).fetchone()
                if candidate is None:
                    raise ValueError("no eligible certified inventory for scheduled run")
                conn.execute(
                    """
                    INSERT INTO ingestion.scheduled_inventory_pins
                        (tenant_id, project_id, feed, scheduled_at, config_hash, inventory_version)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (*identity, candidate["inventory_version"]),
                )
                pinned = conn.execute(
                    """
                    SELECT inventory_version FROM ingestion.scheduled_inventory_pins
                    WHERE tenant_id = %s AND project_id = %s AND feed = %s
                      AND scheduled_at = %s AND config_hash = %s
                    """,
                    identity,
                ).fetchone()
            row = conn.execute(
                """
                SELECT versions.* FROM ingestion.inventory_versions AS versions
                JOIN ingestion.runs AS source ON source.run_id = versions.source_run_id
                WHERE versions.tenant_id = %s AND versions.project_id = %s
                  AND versions.inventory_version = %s
                  AND source.feed = 'metadata' AND source.status = 'certified'
                  AND source.tenant_id = versions.tenant_id
                  AND source.project_id = versions.project_id
                  AND source.scheduled_at <= %s
                  AND EXISTS (
                      SELECT 1 FROM ingestion.site_run_completions AS done
                      WHERE done.run_id = source.run_id
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM ingestion.jobs AS jobs
                      LEFT JOIN ingestion.certifications AS cert
                        ON cert.job_id = jobs.job_id
                      LEFT JOIN ingestion.site_run_completions AS done
                        ON done.run_id = jobs.run_id AND done.site_ref = jobs.site_ref
                      WHERE jobs.run_id = source.run_id
                        AND (jobs.status <> 'certified'
                             OR cert.job_id IS NULL OR done.run_id IS NULL)
                  )
                  AND versions.certified_at <= %s
                  AND versions.certified_at <= now()
                  AND versions.certified_at >= %s - (%s * interval '1 hour')
                  AND versions.certified_at >= (
                      SELECT max(done.completed_at)
                      FROM ingestion.site_run_completions AS done
                      WHERE done.run_id = source.run_id
                  )
                """,
                (tenant_id, project_id, pinned["inventory_version"],
                 due, due, due, max_age_hours),
            ).fetchone()
            if row is None:
                raise ValueError("pinned inventory lost certification or freshness")
        return InventoryRecord(
            tenant_id=row["tenant_id"], project_id=row["project_id"],
            version=row["inventory_version"], source_run_id=row["source_run_id"],
            object_ref=row["object_ref"], object_sha256=row["object_sha256"],
            byte_count=row["byte_count"], certified_at=row["certified_at"],
        )
