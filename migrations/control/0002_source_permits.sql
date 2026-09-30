CREATE TABLE IF NOT EXISTS ingestion.source_budgets (
    tenant_id text NOT NULL,
    project_id text NOT NULL,
    max_concurrent_calls integer NOT NULL CHECK (max_concurrent_calls >= 1),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, project_id)
);

CREATE TABLE IF NOT EXISTS ingestion.source_permits (
    tenant_id text NOT NULL,
    project_id text NOT NULL,
    slot_no integer NOT NULL CHECK (slot_no >= 1),
    lease_owner text,
    job_id text,
    fence_token bigint NOT NULL DEFAULT 0,
    lease_until timestamptz,
    PRIMARY KEY (tenant_id, project_id, slot_no),
    FOREIGN KEY (tenant_id, project_id)
        REFERENCES ingestion.source_budgets(tenant_id, project_id),
    CHECK ((lease_owner IS NULL AND job_id IS NULL AND lease_until IS NULL)
        OR (lease_owner IS NOT NULL AND job_id IS NOT NULL AND lease_until IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS source_permits_available_idx
    ON ingestion.source_permits(tenant_id, project_id, lease_until, slot_no);
