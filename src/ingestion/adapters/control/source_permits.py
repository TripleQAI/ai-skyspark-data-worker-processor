"""PostgreSQL fenced source-call capacity shared by every worker task."""

from __future__ import annotations

import psycopg
from psycopg.rows import dict_row

from ingestion.contracts.permits import SourcePermitLease


class PostgresSourcePermitPool:
    def __init__(self, dsn: str, *, max_slot_no: int | None = None):
        if not dsn or (max_slot_no is not None and max_slot_no < 1):
            raise ValueError("control database DSN is required")
        self._dsn = dsn
        self._max_slot_no = max_slot_no

    def _connect(self) -> psycopg.Connection:
        return psycopg.connect(self._dsn, row_factory=dict_row)

    def ensure_budget(
        self, *, tenant_id: str, project_id: str, max_concurrent_calls: int
    ) -> None:
        if not tenant_id or not project_id or max_concurrent_calls < 1:
            raise ValueError("source budget scope and capacity are required")
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO ingestion.source_budgets
                    (tenant_id, project_id, max_concurrent_calls)
                VALUES (%s, %s, %s) ON CONFLICT DO NOTHING
                """,
                (tenant_id, project_id, max_concurrent_calls),
            )
            row = conn.execute(
                """
                SELECT max_concurrent_calls FROM ingestion.source_budgets
                WHERE tenant_id = %s AND project_id = %s FOR UPDATE
                """,
                (tenant_id, project_id),
            ).fetchone()
            if row["max_concurrent_calls"] != max_concurrent_calls:
                raise ValueError("source budget differs from pinned project policy")
            conn.execute(
                """
                INSERT INTO ingestion.source_permits(tenant_id, project_id, slot_no)
                SELECT %s, %s, slot_no FROM generate_series(1, %s) AS slot_no
                ON CONFLICT DO NOTHING
                """,
                (tenant_id, project_id, max_concurrent_calls),
            )

    def try_acquire(
        self, *, tenant_id: str, project_id: str, job_id: str,
        owner: str, slots: int, lease_seconds: int,
    ) -> SourcePermitLease | None:
        if not all((tenant_id, project_id, job_id, owner)) or slots < 1 or lease_seconds < 1:
            raise ValueError("source permit scope, slots, and lease are required")
        with self._connect() as conn:
            budget = conn.execute(
                """
                SELECT max_concurrent_calls FROM ingestion.source_budgets
                WHERE tenant_id = %s AND project_id = %s
                """,
                (tenant_id, project_id),
            ).fetchone()
            cap = min(budget["max_concurrent_calls"], self._max_slot_no or budget["max_concurrent_calls"]) if budget else 0
            if budget is None or slots > cap:
                raise ValueError("source budget is missing or requested slots exceed capacity")
            rows = conn.execute(
                """
                WITH picked AS (
                    SELECT slot_no FROM ingestion.source_permits
                    WHERE tenant_id = %s AND project_id = %s
                      AND slot_no <= %s
                      AND (lease_until IS NULL OR lease_until <= now())
                    ORDER BY slot_no LIMIT %s FOR UPDATE SKIP LOCKED
                )
                UPDATE ingestion.source_permits AS p
                SET lease_owner = %s, job_id = %s,
                    lease_until = now() + (%s * interval '1 second'),
                    fence_token = fence_token + 1
                FROM picked
                WHERE p.tenant_id = %s AND p.project_id = %s
                  AND p.slot_no = picked.slot_no
                RETURNING p.slot_no, p.fence_token
                """,
                (
                    tenant_id, project_id, cap, slots, owner, job_id,
                    lease_seconds, tenant_id, project_id,
                ),
            ).fetchall()
            if len(rows) != slots:
                conn.rollback()
                return None
        return SourcePermitLease(
            tenant_id, project_id, job_id, owner,
            tuple(sorted((row["slot_no"], row["fence_token"]) for row in rows)),
        )

    def renew(self, lease: SourcePermitLease, *, lease_seconds: int) -> bool:
        if lease_seconds < 1:
            raise ValueError("lease_seconds must be positive")
        with self._connect() as conn:
            renewed = 0
            for slot_no, token in lease.slots:
                row = conn.execute(
                    """
                    UPDATE ingestion.source_permits
                    SET lease_until = now() + (%s * interval '1 second')
                    WHERE tenant_id = %s AND project_id = %s AND slot_no = %s
                      AND lease_owner = %s AND job_id = %s AND fence_token = %s
                      AND lease_until > now()
                    RETURNING slot_no
                    """,
                    (
                        lease_seconds, lease.tenant_id, lease.project_id,
                        slot_no, lease.owner, lease.job_id, token,
                    ),
                ).fetchone()
                renewed += row is not None
            if renewed != len(lease.slots):
                conn.rollback()
                return False
        return True

    def release(self, lease: SourcePermitLease) -> int:
        released = 0
        with self._connect() as conn:
            for slot_no, token in lease.slots:
                row = conn.execute(
                    """
                    UPDATE ingestion.source_permits
                    SET lease_owner = NULL, job_id = NULL, lease_until = NULL
                    WHERE tenant_id = %s AND project_id = %s AND slot_no = %s
                      AND lease_owner = %s AND job_id = %s AND fence_token = %s
                    RETURNING slot_no
                    """,
                    (
                        lease.tenant_id, lease.project_id, slot_no,
                        lease.owner, lease.job_id, token,
                    ),
                ).fetchone()
                released += row is not None
        return released
