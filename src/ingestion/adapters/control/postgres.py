"""PostgreSQL authority for planned jobs and queue-dispatch intents."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
import hashlib
import json

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from ingestion.config.loader import EffectiveConfig
from ingestion.adapters.control.checkpoints import PostgresCheckpointReconciler
from ingestion.contracts.jobs import DispatchIntent, Job, JobCompletion, QueueEnvelope, Run
from ingestion.contracts.replay import ReplayRequest
from ingestion.core.failures import NonRetryableJobError


@dataclass(frozen=True, slots=True)
class PlanSaveResult:
    run_id: str
    expected_jobs: int
    new_jobs: int


@dataclass(frozen=True, slots=True)
class StaleRecovery:
    job_id: str
    run_id: str
    queue_class: str
    reason: str
    redrive_count: int


class PostgresControlRepository:
    def __init__(self, dsn: str):
        if not dsn:
            raise ValueError("control database DSN is required")
        self._dsn = dsn

    def _connect(self) -> psycopg.Connection:
        return psycopg.connect(self._dsn, row_factory=dict_row)

    def save_plan(
        self,
        config: EffectiveConfig,
        run: Run,
        jobs: tuple[Job, ...],
        *,
        queue_class: str,
        replay_request: ReplayRequest | None = None,
    ) -> PlanSaveResult:
        """Commit a run, every job, and its dispatch intent together."""

        if not queue_class or any(char.isspace() for char in queue_class):
            raise ValueError("queue_class must be a logical nonempty name")
        if (run.tenant_id, run.project_id, run.config_hash) != (
            config.binding.tenant_id,
            config.binding.project_id,
            config.config_hash,
        ):
            raise ValueError("run scope or configuration does not match binding")
        for job in jobs:
            if (job.run_id, job.tenant_id, job.project_id, job.config_hash) != (
                run.run_id, run.tenant_id, run.project_id, run.config_hash
            ) or job.feed != run.feed:
                raise ValueError("job scope or configuration does not match run")
            if job.site_ref not in config.binding.approved_sites:
                raise ValueError(f"job site is not approved: {job.site_ref}")
        if replay_request is not None and (
            replay_request.tenant_id != run.tenant_id
            or replay_request.project_id != run.project_id
            or replay_request.config_hash != run.config_hash
            or replay_request.feed != run.feed
            or replay_request.inventory_version != run.inventory_version
            or replay_request.window_start != run.window_start
            or replay_request.window_end != run.window_end
            or replay_request.requested_at != run.scheduled_at
        ):
            raise ValueError("replay audit scope differs from planned run")

        new_jobs = 0
        with self._connect() as conn:
            if replay_request is not None:
                grant = conn.execute(
                    """SELECT * FROM ingestion.replay_approval_grants
                       WHERE approval_ref = %s AND expires_at > now()
                       FOR SHARE""",
                    (replay_request.approval_ref,),
                ).fetchone()
                if (grant is None or
                    (grant["tenant_id"], grant["project_id"], grant["config_hash"],
                     grant["feed"], grant["inventory_version"]) !=
                    (run.tenant_id, run.project_id, run.config_hash,
                     run.feed.value, run.inventory_version) or
                    grant["window_start"] > run.window_start or
                    grant["window_end"] < run.window_end or
                    grant["max_jobs"] < len(jobs) or
                    grant["expires_at"] < run.scheduled_at):
                    raise ValueError("replay lacks an active approval grant for this scope")
            source_capacity = config.profile.source_policy.max_concurrent_calls
            conn.execute(
                """
                INSERT INTO ingestion.source_budgets
                    (tenant_id, project_id, max_concurrent_calls)
                VALUES (%s, %s, %s) ON CONFLICT DO NOTHING
                """,
                (run.tenant_id, run.project_id, source_capacity),
            )
            budget = conn.execute(
                """
                SELECT max_concurrent_calls FROM ingestion.source_budgets
                WHERE tenant_id = %s AND project_id = %s FOR UPDATE
                """,
                (run.tenant_id, run.project_id),
            ).fetchone()
            if budget["max_concurrent_calls"] != source_capacity:
                raise ValueError("source budget differs from pinned project policy")
            conn.execute(
                """
                INSERT INTO ingestion.source_permits(tenant_id, project_id, slot_no)
                SELECT %s, %s, slot_no FROM generate_series(1, %s) AS slot_no
                ON CONFLICT DO NOTHING
                """,
                (run.tenant_id, run.project_id, source_capacity),
            )
            conn.execute(
                """
                INSERT INTO ingestion.pipeline_versions
                    (config_hash, tenant_id, project_id, profile_json, binding_json, manifest_json)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (config_hash) DO NOTHING
                """,
                (
                    config.config_hash,
                    run.tenant_id,
                    run.project_id,
                    Jsonb(config.profile.model_dump(mode="json")),
                    Jsonb(config.binding.model_dump(mode="json")),
                    Jsonb(config.manifest.model_dump(mode="json")),
                ),
            )
            conn.execute(
                """
                INSERT INTO ingestion.runs
                    (run_id, tenant_id, project_id, feed, scheduled_at, window_start,
                     window_end, config_hash, inventory_version, expected_job_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id) DO NOTHING
                """,
                (
                    run.run_id, run.tenant_id, run.project_id, run.feed.value,
                    run.scheduled_at, run.window_start, run.window_end,
                    run.config_hash, run.inventory_version, len(jobs),
                ),
            )
            stored_run = conn.execute(
                "SELECT tenant_id, project_id, config_hash, expected_job_count FROM ingestion.runs WHERE run_id = %s",
                (run.run_id,),
            ).fetchone()
            if stored_run is None or (
                stored_run["tenant_id"], stored_run["project_id"],
                stored_run["config_hash"], stored_run["expected_job_count"]
            ) != (run.tenant_id, run.project_id, run.config_hash, len(jobs)):
                raise ValueError("stored run conflicts with deterministic plan")

            for job in jobs:
                inserted = conn.execute(
                    """
                    INSERT INTO ingestion.jobs
                        (job_id, run_id, tenant_id, project_id, site_ref, feed,
                         scope_ids, window_start, window_end, config_hash, inventory_version)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (job_id) DO NOTHING
                    RETURNING job_id
                    """,
                    (
                        job.job_id, job.run_id, job.tenant_id, job.project_id,
                        job.site_ref, job.feed.value, Jsonb(list(job.scope_ids)),
                        job.window_start, job.window_end, job.config_hash,
                        job.inventory_version,
                    ),
                ).fetchone()
                if inserted:
                    new_jobs += 1
                else:
                    stored_job = conn.execute(
                        "SELECT run_id, site_ref, scope_ids, config_hash FROM ingestion.jobs WHERE job_id = %s",
                        (job.job_id,),
                    ).fetchone()
                    if stored_job is None or (
                        stored_job["run_id"], stored_job["site_ref"],
                        tuple(stored_job["scope_ids"]), stored_job["config_hash"]
                    ) != (job.run_id, job.site_ref, job.scope_ids, job.config_hash):
                        raise ValueError("stored job conflicts with deterministic plan")
                conn.execute(
                    """
                    INSERT INTO ingestion.dispatch_outbox(job_id, queue_class)
                    VALUES (%s, %s) ON CONFLICT (job_id) DO NOTHING
                    """,
                    (job.job_id, queue_class),
                )
                existing_route = conn.execute(
                    "SELECT queue_class FROM ingestion.dispatch_outbox WHERE job_id = %s",
                    (job.job_id,),
                ).fetchone()
                if existing_route is None or existing_route["queue_class"] != queue_class:
                    raise ValueError("job has a conflicting queue route")
            if replay_request is not None:
                audit = (
                    str(replay_request.request_id), run.tenant_id, run.project_id,
                    run.config_hash, run.feed.value, run.inventory_version,
                    run.window_start, run.window_end, run.scheduled_at,
                    replay_request.requested_by, replay_request.approval_ref,
                    replay_request.reason, queue_class, run.run_id,
                )
                conn.execute(
                    """INSERT INTO ingestion.approved_replay_requests
                       (request_id, tenant_id, project_id, config_hash, feed,
                        inventory_version, window_start, window_end, requested_at,
                        requested_by, approval_ref, reason, queue_class, run_id)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT (request_id) DO NOTHING""", audit,
                )
                stored = conn.execute(
                    """SELECT request_id::text, tenant_id, project_id, config_hash,
                              feed, inventory_version, window_start, window_end,
                              requested_at, requested_by, approval_ref, reason,
                              queue_class, run_id
                       FROM ingestion.approved_replay_requests WHERE request_id = %s""",
                    (str(replay_request.request_id),),
                ).fetchone()
                if stored is None or tuple(stored.values()) != audit:
                    raise ValueError("replay request ID conflicts with existing audit")
        return PlanSaveResult(run.run_id, len(jobs), new_jobs)

    def claim_dispatch(
        self, *, owner: str, limit: int, lease_seconds: int
    ) -> tuple[DispatchIntent, ...]:
        if not owner or limit < 1 or lease_seconds < 1:
            raise ValueError("owner, limit, and lease_seconds must be positive")
        with self._connect() as conn:
            rows = conn.execute(
                """
                WITH picked AS (
                    SELECT job_id FROM ingestion.dispatch_outbox
                    WHERE (state = 'pending' AND next_attempt_at <= now())
                       OR (state = 'sending' AND claim_until <= now())
                    ORDER BY created_at, job_id
                    LIMIT %s FOR UPDATE SKIP LOCKED
                )
                UPDATE ingestion.dispatch_outbox AS o
                SET state = 'sending', claim_owner = %s,
                    claim_until = now() + (%s * interval '1 second'),
                    delivery_attempts = delivery_attempts + 1
                FROM picked WHERE o.job_id = picked.job_id
                RETURNING o.job_id, o.queue_class, o.delivery_attempts
                """,
                (limit, owner, lease_seconds),
            ).fetchall()
            intents: list[DispatchIntent] = []
            for row in rows:
                job = conn.execute(
                    "SELECT job_id, run_id, config_hash FROM ingestion.jobs WHERE job_id = %s",
                    (row["job_id"],),
                ).fetchone()
                intents.append(
                    DispatchIntent(
                        job_id=row["job_id"],
                        queue_class=row["queue_class"],
                        envelope=QueueEnvelope(**job),
                        delivery_attempts=row["delivery_attempts"],
                    )
                )
        return tuple(intents)

    def mark_dispatched(
        self, *, job_id: str, owner: str, delivery_attempts: int, message_id: str
    ) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                """
                UPDATE ingestion.dispatch_outbox
                SET state = 'sent', claim_owner = NULL, claim_until = NULL,
                    sqs_message_id = %s, sent_at = now(), last_error = NULL
                WHERE job_id = %s AND state = 'sending' AND claim_owner = %s
                  AND delivery_attempts = %s
                  AND claim_until > now()
                RETURNING job_id
                """,
                (message_id, job_id, owner, delivery_attempts),
            ).fetchone()
        return row is not None

    def release_dispatch(
        self, *, job_id: str, owner: str, delivery_attempts: int,
        delay_seconds: int, error_class: str
    ) -> bool:
        if delay_seconds < 0:
            raise ValueError("delay_seconds must be nonnegative")
        with self._connect() as conn:
            row = conn.execute(
                """
                UPDATE ingestion.dispatch_outbox
                SET state = 'pending', claim_owner = NULL, claim_until = NULL,
                    next_attempt_at = now() + (%s * interval '1 second'),
                    last_error = %s
                WHERE job_id = %s AND state = 'sending' AND claim_owner = %s
                  AND delivery_attempts = %s
                RETURNING job_id
                """,
                (delay_seconds, error_class[:120], job_id, owner, delivery_attempts),
            ).fetchone()
        return row is not None

    def requeue_stale(
        self, *, limit: int, expired_running_after_seconds: int,
        never_started_after_seconds: int, max_redrives: int,
        run_id: str | None = None,
    ) -> tuple[StaleRecovery, ...]:
        """Restore dispatch intents for crashed or never-started jobs only."""
        if min(limit, expired_running_after_seconds, never_started_after_seconds,
               max_redrives) < 1:
            raise ValueError("recovery limits and thresholds must be positive")
        if never_started_after_seconds < expired_running_after_seconds:
            raise ValueError("never-started threshold is too short")
        if run_id is not None and not run_id:
            raise ValueError("run_id must be nonempty when supplied")
        with self._connect() as conn:
            rows = conn.execute(
                """
                WITH picked AS (
                    SELECT outbox.job_id, jobs.run_id,
                           CASE WHEN jobs.status = 'running'
                                THEN 'expired_running'
                                ELSE 'never_started' END AS reason
                    FROM ingestion.dispatch_outbox AS outbox
                    JOIN ingestion.jobs AS jobs ON jobs.job_id = outbox.job_id
                    WHERE outbox.state = 'sent'
                      AND outbox.redrive_count < %s
                      AND (%s::text IS NULL OR jobs.run_id = %s)
                      AND (
                        (jobs.status = 'running'
                         AND jobs.lease_expires_at <= now() - (%s * interval '1 second')
                         AND jobs.updated_at <= now() - (%s * interval '1 second'))
                        OR
                        (jobs.status = 'planned'
                         AND NOT EXISTS (
                             SELECT 1 FROM ingestion.attempts AS attempts
                             WHERE attempts.job_id = jobs.job_id
                         )
                         AND outbox.sent_at <= now() - (%s * interval '1 second')
                         AND jobs.updated_at <= now() - (%s * interval '1 second'))
                      )
                    ORDER BY outbox.sent_at, outbox.job_id
                    LIMIT %s FOR UPDATE OF outbox, jobs SKIP LOCKED
                )
                UPDATE ingestion.dispatch_outbox AS outbox
                SET state = 'pending', next_attempt_at = now(),
                    sqs_message_id = NULL, redrive_count = redrive_count + 1,
                    last_requeued_at = now(),
                    last_error = 'StaleRecovery:' || picked.reason
                FROM picked WHERE outbox.job_id = picked.job_id
                RETURNING outbox.job_id, picked.run_id, outbox.queue_class,
                          picked.reason, outbox.redrive_count
                """,
                (
                    max_redrives, run_id, run_id,
                    expired_running_after_seconds, expired_running_after_seconds,
                    never_started_after_seconds, never_started_after_seconds, limit,
                ),
            ).fetchall()
        return tuple(StaleRecovery(**row) for row in rows)

    def get_job(self, job_id: str) -> Job | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT job_id, run_id, tenant_id, project_id, site_ref, feed,
                       scope_ids, window_start, window_end, config_hash,
                       inventory_version
                FROM ingestion.jobs WHERE job_id = %s
                """,
                (job_id,),
            ).fetchone()
        return Job.model_validate(row) if row else None

    def job_status(self, job_id: str) -> str | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT status FROM ingestion.jobs WHERE job_id = %s", (job_id,)
            ).fetchone()
        return row["status"] if row else None

    def split_history_job(
        self, *, job_id: str, worker_id: str, fence_token: int,
        max_depth: int, min_window_seconds: int,
        max_descendant_jobs: int = 8192,
    ) -> bool:
        """Replace one live history leaf with two deterministic durable children."""
        if max_depth < 0 or min_window_seconds < 1 or max_descendant_jobs < 2:
            raise ValueError("invalid history split policy")
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM ingestion.jobs WHERE job_id = %s FOR UPDATE", (job_id,),
            ).fetchone()
            if row is None:
                raise ValueError("unknown history job")
            if (row["status"] != "running" or row["lease_owner"] != worker_id
                    or row["fence_token"] != fence_token
                    or row["lease_expires_at"] is None
                    or conn.execute("SELECT %s > now() AS live", (row["lease_expires_at"],)).fetchone()["live"] is not True):
                return False
            if row["feed"] != "history" or row["window_start"] is None or row["window_end"] is None:
                raise ValueError("only bounded history leaves can split")
            lineage = conn.execute(
                """WITH RECURSIVE ancestors(job_id, depth) AS (
                     SELECT %s::text, 0
                     UNION ALL
                     SELECT links.parent_job_id, ancestors.depth + 1
                     FROM ingestion.job_children AS links
                     JOIN ancestors ON links.child_job_id = ancestors.job_id
                   ) SELECT job_id AS root_id, depth FROM ancestors
                     ORDER BY depth DESC LIMIT 1""", (job_id,),
            ).fetchone()
            depth = lineage["depth"]
            if depth >= max_depth:
                raise NonRetryableJobError("history split depth exhausted")
            root_id = lineage["root_id"]
            conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (root_id,))
            descendants = conn.execute(
                """WITH RECURSIVE related(job_id) AS (
                     SELECT child_job_id FROM ingestion.job_children WHERE parent_job_id = %s
                     UNION
                     SELECT links.child_job_id FROM ingestion.job_children AS links
                     JOIN related ON links.parent_job_id = related.job_id
                   ) SELECT count(*) AS count FROM related""", (root_id,),
            ).fetchone()["count"]
            if descendants + 2 > max_descendant_jobs:
                raise NonRetryableJobError("history descendant job cap exhausted")
            parent = Job.model_validate({key: row[key] for key in (
                "job_id", "run_id", "tenant_id", "project_id", "site_ref", "feed",
                "scope_ids", "window_start", "window_end", "config_hash", "inventory_version",
            )})
            ids = parent.scope_ids
            if len(ids) > 1:
                half = len(ids) // 2
                parts = ((ids[:half], parent.window_start, parent.window_end),
                         (ids[half:], parent.window_start, parent.window_end))
            else:
                duration = (parent.window_end - parent.window_start).total_seconds()
                if duration < 2 * min_window_seconds:
                    raise NonRetryableJobError("history cannot split below minimum window")
                midpoint = parent.window_start + timedelta(seconds=duration / 2)
                parts = ((ids, parent.window_start, midpoint),
                         (ids, midpoint, parent.window_end))
            route = conn.execute(
                "SELECT queue_class FROM ingestion.dispatch_outbox WHERE job_id = %s",
                (job_id,),
            ).fetchone()
            if route is None:
                raise ValueError("history parent has no dispatch route")
            for child_ids, start, end in parts:
                identity = {"parent_job_id": job_id, "scope_ids": child_ids,
                            "window_start": start.isoformat(), "window_end": end.isoformat()}
                child_id = hashlib.sha256(json.dumps(
                    identity, sort_keys=True, separators=(",", ":"),
                ).encode()).hexdigest()
                child = parent.model_copy(update={
                    "job_id": child_id, "scope_ids": child_ids,
                    "window_start": start, "window_end": end,
                })
                conn.execute(
                    """INSERT INTO ingestion.jobs
                       (job_id, run_id, tenant_id, project_id, site_ref, feed,
                        scope_ids, window_start, window_end, config_hash, inventory_version)
                       VALUES (%s, %s, %s, %s, %s, 'history', %s, %s, %s, %s, %s)
                       ON CONFLICT (job_id) DO NOTHING""",
                    (child.job_id, child.run_id, child.tenant_id, child.project_id,
                     child.site_ref, Jsonb(list(child.scope_ids)), child.window_start,
                     child.window_end, child.config_hash, child.inventory_version),
                )
                conn.execute(
                    "INSERT INTO ingestion.job_children(parent_job_id, child_job_id) "
                    "VALUES (%s, %s) ON CONFLICT DO NOTHING", (job_id, child.job_id),
                )
                conn.execute(
                    "INSERT INTO ingestion.dispatch_outbox(job_id, queue_class) "
                    "VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    (child.job_id, route["queue_class"]),
                )
            conn.execute(
                """UPDATE ingestion.jobs SET status = 'split', lease_owner = NULL,
                   lease_expires_at = NULL, updated_at = now() WHERE job_id = %s""",
                (job_id,),
            )
            conn.execute(
                """UPDATE ingestion.attempts SET finished_at = now(), outcome = 'split',
                   error_class = 'HistorySplitRequired'
                   WHERE job_id = %s AND fence_token = %s""",
                (job_id, fence_token),
            )
        return True

    def acquire_job_lease(
        self, *, job_id: str, worker_id: str, lease_seconds: int
    ) -> int | None:
        if not worker_id or lease_seconds < 1:
            raise ValueError("worker_id and lease_seconds are required")
        with self._connect() as conn:
            row = conn.execute(
                """
                UPDATE ingestion.jobs
                SET status = 'running', lease_owner = %s,
                    lease_expires_at = now() + (%s * interval '1 second'),
                    fence_token = fence_token + 1, updated_at = now()
                WHERE job_id = %s AND status IN ('planned', 'running')
                  AND (lease_expires_at IS NULL OR lease_expires_at <= now())
                RETURNING fence_token
                """,
                (worker_id, lease_seconds, job_id),
            ).fetchone()
            if row:
                conn.execute(
                    """
                    UPDATE ingestion.attempts
                    SET finished_at = now(), outcome = 'expired'
                    WHERE job_id = %s AND finished_at IS NULL AND fence_token < %s
                    """,
                    (job_id, row["fence_token"]),
                )
                conn.execute(
                    """
                    INSERT INTO ingestion.attempts
                        (job_id, attempt_no, worker_id, fence_token)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (job_id, row["fence_token"], worker_id, row["fence_token"]),
                )
        return row["fence_token"] if row else None

    def renew_job_lease(
        self, *, job_id: str, worker_id: str, fence_token: int, lease_seconds: int
    ) -> bool:
        if lease_seconds < 1:
            raise ValueError("lease_seconds must be positive")
        with self._connect() as conn:
            row = conn.execute(
                """
                UPDATE ingestion.jobs
                SET lease_expires_at = now() + (%s * interval '1 second'), updated_at = now()
                WHERE job_id = %s AND status = 'running' AND lease_owner = %s
                  AND fence_token = %s AND lease_expires_at > now()
                RETURNING job_id
                """,
                (lease_seconds, job_id, worker_id, fence_token),
            ).fetchone()
        return row is not None

    def release_job_lease(
        self, *, job_id: str, worker_id: str, fence_token: int,
        error_class: str,
    ) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                """
                UPDATE ingestion.jobs
                SET status = 'planned', lease_owner = NULL,
                    lease_expires_at = NULL, updated_at = now()
                WHERE job_id = %s AND status = 'running'
                  AND lease_owner = %s AND fence_token = %s
                RETURNING job_id
                """,
                (job_id, worker_id, fence_token),
            ).fetchone()
            if row:
                conn.execute(
                    """
                    UPDATE ingestion.attempts
                    SET finished_at = now(), outcome = 'retry', error_class = %s
                    WHERE job_id = %s AND fence_token = %s
                    """,
                    (error_class[:120], job_id, fence_token),
                )
        return row is not None

    def quarantine_job(
        self, *, job_id: str, worker_id: str, fence_token: int,
        error_class: str,
    ) -> bool:
        """Terminalize a bad source partition only under its live fence."""
        with self._connect() as conn:
            row = conn.execute(
                """
                UPDATE ingestion.jobs
                SET status = 'quarantined', lease_owner = NULL,
                    lease_expires_at = NULL, updated_at = now()
                WHERE job_id = %s AND status = 'running'
                  AND lease_owner = %s AND fence_token = %s
                  AND lease_expires_at > now()
                RETURNING job_id
                """,
                (job_id, worker_id, fence_token),
            ).fetchone()
            if row:
                conn.execute(
                    """
                    UPDATE ingestion.attempts
                    SET finished_at = now(), outcome = 'quarantined', error_class = %s
                    WHERE job_id = %s AND fence_token = %s
                    """,
                    (error_class[:120], job_id, fence_token),
                )
        return row is not None

    def certify_job(
        self, *, job_id: str, worker_id: str, fence_token: int,
        completion: JobCompletion,
    ) -> bool:
        """Record evidence and finalize only while holding the current lease."""

        if completion.raw.job_id != job_id or completion.sink.job_id != job_id:
            raise ValueError("completion evidence belongs to another job")
        if not completion.raw.object_key or not completion.raw.checksum:
            raise ValueError("raw artifact key and checksum are required")
        if not completion.sink.batch_key or not completion.sink.checksum:
            raise ValueError("sink batch key and checksum are required")
        artifact_id = hashlib.sha256(
            f"{job_id}:{completion.raw.object_key}".encode("utf-8")
        ).hexdigest()
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT j.run_id, j.site_ref, j.feed, j.scope_ids, j.status, j.lease_owner,
                       j.lease_expires_at > now() AS lease_live, j.fence_token,
                       p.profile_json
                FROM ingestion.jobs AS j
                JOIN ingestion.pipeline_versions AS p ON p.config_hash = j.config_hash
                WHERE j.job_id = %s FOR UPDATE OF j
                """,
                (job_id,),
            ).fetchone()
            if row is None:
                raise ValueError(f"unknown job: {job_id}")
            if (
                row["status"] != "running" or row["lease_owner"] != worker_id
                or row["fence_token"] != fence_token or not row["lease_live"]
            ):
                return False
            expected = (
                (row["site_ref"],) if row["feed"] == "metadata"
                else tuple(row["scope_ids"])
            )
            completed = completion.completed_scope
            if len(completed) != len(set(completed)) or set(completed) != set(expected):
                raise ValueError("completion does not cover the full requested scope")
            target = row["profile_json"]["feeds"][row["feed"]]["target"]
            if completion.sink.sink_kind != target:
                raise ValueError("sink kind does not match pinned configuration")

            conn.execute(
                """
                INSERT INTO ingestion.raw_artifacts
                    (artifact_id, job_id, object_key, checksum, byte_count)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (artifact_id) DO NOTHING
                """,
                (
                    artifact_id, job_id, completion.raw.object_key,
                    completion.raw.checksum, completion.raw.byte_count,
                ),
            )
            stored_raw = conn.execute(
                "SELECT job_id, object_key, checksum, byte_count FROM ingestion.raw_artifacts "
                "WHERE artifact_id = %s", (artifact_id,),
            ).fetchone()
            if stored_raw != {
                "job_id": job_id, "object_key": completion.raw.object_key,
                "checksum": completion.raw.checksum,
                "byte_count": completion.raw.byte_count,
            }:
                raise ValueError("raw artifact conflicts with existing evidence")

            conn.execute(
                """
                INSERT INTO ingestion.sink_receipts
                    (batch_key, job_id, sink_kind, row_count, checksum)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (batch_key) DO NOTHING
                """,
                (
                    completion.sink.batch_key, job_id, completion.sink.sink_kind,
                    completion.sink.row_count, completion.sink.checksum,
                ),
            )
            stored_sink = conn.execute(
                "SELECT job_id, sink_kind, row_count, checksum FROM ingestion.sink_receipts "
                "WHERE batch_key = %s", (completion.sink.batch_key,),
            ).fetchone()
            if stored_sink != {
                "job_id": job_id, "sink_kind": completion.sink.sink_kind,
                "row_count": completion.sink.row_count, "checksum": completion.sink.checksum,
            }:
                raise ValueError("sink receipt conflicts with existing evidence")

            conn.execute(
                """
                INSERT INTO ingestion.certifications
                    (job_id, artifact_id, batch_key, completed_scope)
                VALUES (%s, %s, %s, %s)
                """,
                (job_id, artifact_id, completion.sink.batch_key, Jsonb(list(completed))),
            )
            conn.execute(
                """
                UPDATE ingestion.jobs
                SET status = 'certified', lease_owner = NULL, lease_expires_at = NULL,
                    updated_at = now()
                WHERE job_id = %s
                """,
                (job_id,),
            )
            conn.execute(
                """
                UPDATE ingestion.attempts
                SET finished_at = now(), outcome = 'certified'
                WHERE job_id = %s AND fence_token = %s
                """,
                (job_id, fence_token),
            )
            PostgresCheckpointReconciler(self._dsn).reconcile_site(
                row["run_id"], row["site_ref"], connection=conn
            )
        return True
