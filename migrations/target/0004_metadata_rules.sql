ALTER TABLE ingestion_target.batch_receipts
    DROP CONSTRAINT IF EXISTS batch_receipts_feed_check;
ALTER TABLE ingestion_target.batch_receipts
    ADD CONSTRAINT batch_receipts_feed_check
    CHECK (feed IN ('metadata', 'history', 'rules'));

CREATE TABLE IF NOT EXISTS ingestion_target.metadata_entity_revisions (
    tenant_id text NOT NULL,
    project_id text NOT NULL,
    site_ref text NOT NULL,
    kind text NOT NULL CHECK (kind IN ('equipment', 'point')),
    source_id text NOT NULL,
    row_hash text NOT NULL CHECK (length(row_hash) = 64),
    equipment_ref text,
    historized boolean NOT NULL,
    payload jsonb NOT NULL,
    first_job_id text NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, project_id, site_ref, kind, source_id, row_hash)
);
CREATE INDEX IF NOT EXISTS metadata_entity_latest_idx
    ON ingestion_target.metadata_entity_revisions
    (tenant_id, project_id, site_ref, kind, source_id, recorded_at DESC);

CREATE TABLE IF NOT EXISTS ingestion_target.metadata_batch_entities (
    batch_key text NOT NULL,
    position integer NOT NULL CHECK (position >= 0),
    kind text NOT NULL CHECK (kind IN ('equipment', 'point')),
    source_id text NOT NULL,
    row_hash text NOT NULL CHECK (length(row_hash) = 64),
    payload jsonb NOT NULL,
    PRIMARY KEY (batch_key, position),
    UNIQUE (batch_key, kind, source_id)
);

CREATE TABLE IF NOT EXISTS ingestion_target.rule_detection_revisions (
    tenant_id text NOT NULL,
    project_id text NOT NULL,
    site_ref text NOT NULL,
    source_date date NOT NULL,
    detection_key text NOT NULL CHECK (length(detection_key) = 64),
    revision_hash text NOT NULL CHECK (length(revision_hash) = 64),
    equipment_id text NOT NULL,
    rule_id text NOT NULL,
    payload jsonb NOT NULL,
    first_job_id text NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, project_id, source_date, detection_key, revision_hash)
);
CREATE INDEX IF NOT EXISTS rule_detection_by_equipment_day_idx
    ON ingestion_target.rule_detection_revisions
    (tenant_id, project_id, site_ref, equipment_id, source_date DESC);

CREATE TABLE IF NOT EXISTS ingestion_target.rule_batch_detections (
    batch_key text NOT NULL,
    position integer NOT NULL CHECK (position >= 0),
    source_date date NOT NULL,
    detection_key text NOT NULL CHECK (length(detection_key) = 64),
    revision_hash text NOT NULL CHECK (length(revision_hash) = 64),
    payload jsonb NOT NULL,
    PRIMARY KEY (batch_key, position),
    UNIQUE (batch_key, detection_key)
);
