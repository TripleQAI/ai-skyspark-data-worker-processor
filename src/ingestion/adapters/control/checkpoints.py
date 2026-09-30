"""Contiguous per-site window checkpoints and durable publication intents."""

from __future__ import annotations

from dataclasses import dataclass
from contextlib import nullcontext
from datetime import datetime, timezone
import hashlib

import psycopg
from psycopg.rows import dict_row

from ingestion.config.loader import EffectiveConfig
from ingestion.contracts.config import FeedKind


@dataclass(frozen=True, slots=True)
class ReconcileResult:
    run_id: str
    site_ref: str
    state: str
    job_count: int


class PostgresCheckpointReconciler:
    def __init__(self, dsn: str):
        if not dsn:
            raise ValueError("control database DSN is required")
        self._dsn = dsn

    def _connect(self) -> psycopg.Connection:
        return psycopg.connect(self._dsn, row_factory=dict_row)

    def seed_checkpoint(
        self, config: EffectiveConfig, feed: FeedKind, site_ref: str,
        start_at: datetime,
    ) -> None:
        """Set the approved first window start once; never reset a live cursor."""
        if feed not in (FeedKind.HISTORY, FeedKind.RULES):
            raise ValueError("only windowed feeds use this checkpoint")
        if feed not in config.profile.feeds or site_ref not in config.binding.approved_sites:
            raise ValueError("feed/site is outside the approved binding")
        if start_at.tzinfo is None:
            raise ValueError("start_at must be timezone-aware")
        start_at = start_at.astimezone(timezone.utc)
        tenant_id, project_id = config.binding.tenant_id, config.binding.project_id
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO ingestion.checkpoints
                    (tenant_id, project_id, site_ref, feed, baseline_at, certified_through)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                """,
                (tenant_id, project_id, site_ref, feed.value, start_at, start_at),
            )
            row = conn.execute(
                """
                SELECT baseline_at, certified_through FROM ingestion.checkpoints
                WHERE tenant_id = %s AND project_id = %s AND site_ref = %s AND feed = %s
                FOR UPDATE
                """,
                (tenant_id, project_id, site_ref, feed.value),
            ).fetchone()
            if (row["baseline_at"] != start_at or row["certified_through"] is None
                    or row["certified_through"] < start_at):
                raise ValueError("checkpoint already exists with a different baseline")

    def run_sites(self, run_id: str) -> tuple[str, ...]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT DISTINCT site_ref FROM ingestion.jobs WHERE run_id = %s ORDER BY site_ref",
                (run_id,),
            ).fetchall()
        return tuple(row["site_ref"] for row in rows)

    def reconcile_run(self, run_id: str) -> tuple[ReconcileResult, ...]:
        return tuple(self.reconcile_site(run_id, site) for site in self.run_sites(run_id))

    def reconcile_site(
        self, run_id: str, site_ref: str,
        *, connection: psycopg.Connection | None = None,
    ) -> ReconcileResult:
        """Complete a site after every root and child job is certified."""
        # Reuse the certification transaction when a worker finishes the final
        # partition. Standalone reconciliation owns its transaction on recovery.
        with (nullcontext(connection) if connection is not None else self._connect()) as conn:
            run = conn.execute(
                "SELECT * FROM ingestion.runs WHERE run_id = %s FOR UPDATE",
                (run_id,),
            ).fetchone()
            if run is None:
                raise ValueError(f"unknown run: {run_id}")
            if run["feed"] not in ("history", "rules", "metadata"):
                raise ValueError("unsupported feed")
            if run["feed"] != "metadata" and (run["window_start"] is None or run["window_end"] is None):
                raise ValueError("windowed run has no window")

            roots = conn.execute(
                """
                SELECT jobs.job_id FROM ingestion.jobs AS jobs
                WHERE jobs.run_id = %s AND jobs.site_ref = %s
                  AND NOT EXISTS (
                    SELECT 1 FROM ingestion.job_children AS links
                    WHERE links.child_job_id = jobs.job_id
                  )
                ORDER BY jobs.job_id
                """,
                (run_id, site_ref),
            ).fetchall()
            job_ids = tuple(row["job_id"] for row in roots)
            if not job_ids:
                raise ValueError(f"run has no jobs for site: {site_ref}")
            result = ReconcileResult(run_id, site_ref, "waiting_for_certification", len(job_ids))
            certified = conn.execute(
                """
                WITH RECURSIVE related(job_id) AS (
                    SELECT job_id FROM ingestion.jobs WHERE run_id = %s AND site_ref = %s
                    UNION
                    SELECT child_job_id FROM ingestion.job_children AS children
                    JOIN related ON children.parent_job_id = related.job_id
                )
                SELECT count(*) FILTER (
                           WHERE NOT EXISTS (
                             SELECT 1 FROM ingestion.job_children AS links
                             WHERE links.parent_job_id = jobs.job_id
                           )
                       ) AS total,
                       count(*) FILTER (
                           WHERE jobs.status = 'certified'
                             AND cert.job_id IS NOT NULL
                             AND jobs.run_id = %s AND jobs.site_ref = %s
                             AND NOT EXISTS (
                               SELECT 1 FROM ingestion.job_children AS links
                               WHERE links.parent_job_id = jobs.job_id
                             )
                       ) AS complete
                FROM related JOIN ingestion.jobs AS jobs USING (job_id)
                LEFT JOIN ingestion.certifications AS cert USING (job_id)
                """,
                (run_id, site_ref, run_id, site_ref),
            ).fetchone()
            if certified["total"] != certified["complete"]:
                return result

            prior = conn.execute(
                """
                SELECT job_count FROM ingestion.site_run_completions
                WHERE run_id = %s AND site_ref = %s
                """,
                (run_id, site_ref),
            ).fetchone()
            if prior is not None:
                if prior["job_count"] != len(job_ids):
                    raise ValueError("site completion conflicts with current plan")
                self._finalize_run(conn, run_id)
                return ReconcileResult(run_id, site_ref, "already_advanced", len(job_ids))

            if run["feed"] == "metadata":
                self._record_completion(conn, run_id, site_ref, job_ids)
                self._finalize_run(conn, run_id)
                return ReconcileResult(run_id, site_ref, "advanced", len(job_ids))

            cursor = conn.execute(
                """
                SELECT baseline_at, certified_through FROM ingestion.checkpoints
                WHERE tenant_id = %s AND project_id = %s AND site_ref = %s AND feed = %s
                FOR UPDATE
                """,
                (run["tenant_id"], run["project_id"], site_ref, run["feed"]),
            ).fetchone()
            if cursor is None or cursor["baseline_at"] is None or cursor["certified_through"] is None:
                return ReconcileResult(run_id, site_ref, "unseeded", len(job_ids))
            if cursor["certified_through"] < run["window_start"]:
                return ReconcileResult(run_id, site_ref, "waiting_for_prior_window", len(job_ids))
            if cursor["certified_through"] >= run["window_end"]:
                # A scheduled lookback is fully covered by the existing cursor.
                # Its fresh source/target evidence can certify without moving it.
                self._record_completion(conn, run_id, site_ref, job_ids)
                self._finalize_run(conn, run_id)
                return ReconcileResult(run_id, site_ref, "replay_certified", len(job_ids))
            if cursor["certified_through"] > run["window_start"]:
                return ReconcileResult(run_id, site_ref, "superseded_or_overlapping", len(job_ids))

            conn.execute(
                """
                UPDATE ingestion.checkpoints SET certified_through = %s,
                    inventory_version = %s, last_run_id = %s, updated_at = now()
                WHERE tenant_id = %s AND project_id = %s AND site_ref = %s AND feed = %s
                """,
                (run["window_end"], run["inventory_version"], run_id,
                 run["tenant_id"], run["project_id"], site_ref, run["feed"]),
            )
            self._record_completion(conn, run_id, site_ref, job_ids)
            self._finalize_run(conn, run_id)
            return ReconcileResult(run_id, site_ref, "advanced", len(job_ids))

    @staticmethod
    def _record_completion(
        conn: psycopg.Connection, run_id: str, site_ref: str,
        job_ids: tuple[str, ...],
    ) -> None:
        conn.execute(
            """
            INSERT INTO ingestion.site_run_completions(run_id, site_ref, job_count)
            VALUES (%s, %s, %s)
            """,
            (run_id, site_ref, len(job_ids)),
        )
        leaves = conn.execute(
            """SELECT jobs.job_id FROM ingestion.jobs AS jobs
               WHERE jobs.run_id = %s AND jobs.site_ref = %s
                 AND NOT EXISTS (
                   SELECT 1 FROM ingestion.job_children AS links
                   WHERE links.parent_job_id = jobs.job_id
                 ) ORDER BY jobs.job_id""", (run_id, site_ref),
        ).fetchall()
        for leaf in leaves:
            job_id = leaf["job_id"]
            publication_id = hashlib.sha256(
                f"certified-job:{job_id}".encode("utf-8")
            ).hexdigest()
            conn.execute(
                """
                INSERT INTO ingestion.publication_outbox(publication_id, job_id)
                VALUES (%s, %s) ON CONFLICT DO NOTHING
                """,
                (publication_id, job_id),
            )

    @staticmethod
    def _finalize_run(conn: psycopg.Connection, run_id: str) -> None:
        conn.execute(
            """
            UPDATE ingestion.runs AS runs SET status = 'certified'
            WHERE runs.run_id = %s
              AND NOT EXISTS (
                  SELECT 1 FROM ingestion.jobs AS jobs
                  WHERE jobs.run_id = runs.run_id
                    AND jobs.status NOT IN ('certified', 'split')
              )
              AND NOT EXISTS (
                  SELECT 1 FROM ingestion.jobs AS jobs
                  LEFT JOIN ingestion.site_run_completions AS done
                    ON done.run_id = jobs.run_id AND done.site_ref = jobs.site_ref
                  WHERE jobs.run_id = runs.run_id AND done.run_id IS NULL
              )
            """,
            (run_id,),
        )
