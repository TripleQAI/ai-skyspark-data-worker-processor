"""Read-only certified-coverage status for one scoped scheduled run."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone

import psycopg
from psycopg.rows import dict_row

from ingestion.contracts.config import FeedKind


@dataclass(frozen=True, slots=True)
class RunProgress:
    run_id: str
    feed: FeedKind
    state: str
    reason: str
    expected_root_jobs: int
    actual_root_jobs: int
    total_jobs: int
    evidence_certified_jobs: int
    terminal_jobs: int
    expected_sites: int
    completed_sites: int
    deadline_at: datetime

    def summary(self) -> dict[str, object]:
        data = asdict(self)
        data["feed"] = self.feed.value
        data["deadline_at"] = self.deadline_at.isoformat()
        return data


class PostgresRunStatusReader:
    def __init__(self, dsn: str):
        if not dsn:
            raise ValueError("control database DSN is required")
        self._dsn = dsn

    def read(
        self, *, run_id: str, tenant_id: str, project_id: str,
        config_hash: str, feed: FeedKind, max_run_seconds: int,
        now: datetime | None = None,
    ) -> RunProgress:
        if max_run_seconds < 1:
            raise ValueError("max_run_seconds must be positive")
        observed_at = now or datetime.now(timezone.utc)
        if observed_at.tzinfo is None or observed_at.utcoffset() is None:
            raise ValueError("now must have a timezone")
        observed_at = observed_at.astimezone(timezone.utc)
        with psycopg.connect(self._dsn, row_factory=dict_row) as conn:
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            run = conn.execute(
                """
                SELECT run_id, feed, scheduled_at, expected_job_count, status
                FROM ingestion.runs
                WHERE run_id = %s AND tenant_id = %s AND project_id = %s
                  AND config_hash = %s AND feed = %s
                """,
                (run_id, tenant_id, project_id, config_hash, feed.value),
            ).fetchone()
            if run is None:
                raise ValueError("run not found in approved tenant/project/config scope")
            jobs = conn.execute(
                """
                SELECT count(*) AS total_jobs,
                       count(*) FILTER (
                           WHERE jobs.status = 'certified' AND cert.job_id IS NOT NULL
                       ) AS evidence_certified_jobs,
                       count(*) FILTER (
                           WHERE jobs.status IN ('failed', 'quarantined')
                       ) AS terminal_jobs,
                       count(*) FILTER (WHERE jobs.status = 'split') AS split_jobs
                FROM ingestion.jobs AS jobs
                LEFT JOIN ingestion.certifications AS cert ON cert.job_id = jobs.job_id
                WHERE jobs.run_id = %s
                """,
                (run_id,),
            ).fetchone()
            roots = conn.execute(
                """
                SELECT count(*) AS root_jobs, count(DISTINCT jobs.site_ref) AS sites
                FROM ingestion.jobs AS jobs
                WHERE jobs.run_id = %s
                  AND NOT EXISTS (
                      SELECT 1 FROM ingestion.job_children AS children
                      WHERE children.child_job_id = jobs.job_id
                  )
                """,
                (run_id,),
            ).fetchone()
            completed = conn.execute(
                """
                SELECT count(*) AS sites
                FROM ingestion.site_run_completions WHERE run_id = %s
                """,
                (run_id,),
            ).fetchone()["sites"]

        deadline = run["scheduled_at"] + timedelta(seconds=max_run_seconds)
        total = jobs["total_jobs"]
        certified = jobs["evidence_certified_jobs"]
        terminal = jobs["terminal_jobs"]
        split = jobs["split_jobs"]
        expected_sites = roots["sites"]
        inconsistent = (
            total == 0 or roots["root_jobs"] != run["expected_job_count"]
            or expected_sites == 0 or completed > expected_sites
            or (run["status"] == "certified" and (
                certified + split != total or completed != expected_sites
            ))
        )
        if inconsistent:
            state, reason = "blocked", "inconsistent_control_state"
        elif run["status"] == "certified" and certified + split == total and completed == expected_sites:
            state, reason = "certified", "all_sites_certified"
        elif terminal:
            state, reason = ("partial" if completed else "blocked"), "terminal_job"
        elif observed_at >= deadline:
            state, reason = ("partial" if completed else "blocked"), "deadline_elapsed"
        else:
            state, reason = "pending", "awaiting_certification"
        return RunProgress(
            run_id=run_id, feed=FeedKind(run["feed"]), state=state, reason=reason,
            expected_root_jobs=run["expected_job_count"],
            actual_root_jobs=roots["root_jobs"], total_jobs=total,
            evidence_certified_jobs=certified, terminal_jobs=terminal,
            expected_sites=expected_sites, completed_sites=completed,
            deadline_at=deadline,
        )
