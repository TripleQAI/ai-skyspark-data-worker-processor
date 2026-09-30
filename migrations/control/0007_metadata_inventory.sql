ALTER TABLE ingestion.inventory_versions
    ADD COLUMN IF NOT EXISTS source_snapshot_token text;

CREATE TABLE IF NOT EXISTS ingestion.inventory_entities (
    tenant_id text NOT NULL,
    project_id text NOT NULL,
    kind text NOT NULL CHECK (kind IN ('equipment', 'point')),
    source_id text NOT NULL,
    site_ref text NOT NULL,
    equipment_ref text,
    historized boolean NOT NULL,
    tags jsonb NOT NULL CHECK (jsonb_typeof(tags) = 'object'),
    row_sha256 text NOT NULL CHECK (row_sha256 ~ '^[0-9a-f]{64}$'),
    last_seen_run_id text NOT NULL REFERENCES ingestion.runs(run_id),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, project_id, kind, source_id)
);
CREATE INDEX IF NOT EXISTS inventory_entities_site_idx
    ON ingestion.inventory_entities(tenant_id, project_id, site_ref, kind);

CREATE TABLE IF NOT EXISTS ingestion.inventory_entity_changes (
    source_run_id text NOT NULL REFERENCES ingestion.runs(run_id),
    tenant_id text NOT NULL,
    project_id text NOT NULL,
    kind text NOT NULL CHECK (kind IN ('equipment', 'point')),
    source_id text NOT NULL,
    change_kind text NOT NULL CHECK (change_kind IN ('added', 'changed')),
    previous_sha256 text,
    current_sha256 text NOT NULL CHECK (current_sha256 ~ '^[0-9a-f]{64}$'),
    recorded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source_run_id, kind, source_id)
);
