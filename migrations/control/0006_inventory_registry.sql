CREATE TABLE IF NOT EXISTS ingestion.inventory_versions (
    tenant_id text NOT NULL,
    project_id text NOT NULL,
    inventory_version text NOT NULL,
    source_run_id text NOT NULL REFERENCES ingestion.runs(run_id),
    object_ref text NOT NULL,
    object_sha256 text NOT NULL CHECK (object_sha256 ~ '^[0-9a-f]{64}$'),
    byte_count bigint NOT NULL CHECK (byte_count > 0),
    certified_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, project_id, inventory_version),
    UNIQUE (source_run_id)
);
CREATE INDEX IF NOT EXISTS inventory_versions_due_idx
    ON ingestion.inventory_versions(tenant_id, project_id, certified_at DESC);

CREATE TABLE IF NOT EXISTS ingestion.scheduled_inventory_pins (
    tenant_id text NOT NULL,
    project_id text NOT NULL,
    feed text NOT NULL CHECK (feed IN ('history', 'rules')),
    scheduled_at timestamptz NOT NULL,
    config_hash text NOT NULL,
    inventory_version text NOT NULL,
    pinned_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, project_id, feed, scheduled_at, config_hash),
    FOREIGN KEY (tenant_id, project_id, inventory_version)
        REFERENCES ingestion.inventory_versions(tenant_id, project_id, inventory_version)
);
