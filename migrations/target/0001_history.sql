CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE SCHEMA IF NOT EXISTS ingestion_target;

CREATE TABLE IF NOT EXISTS ingestion_target.history_observations (
    tenant_id text NOT NULL,
    project_id text NOT NULL,
    point_id text NOT NULL,
    observed_at timestamptz NOT NULL,
    value_kind text NOT NULL CHECK (value_kind IN ('bool', 'str', 'num', 'na')),
    value_bool boolean,
    value_str text,
    value_num numeric,
    row_hash text NOT NULL CHECK (length(row_hash) = 64),
    first_job_id text NOT NULL,
    PRIMARY KEY (tenant_id, project_id, point_id, observed_at)
);

SELECT create_hypertable(
    'ingestion_target.history_observations',
    by_range('observed_at'),
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS history_by_point_time_idx
    ON ingestion_target.history_observations
    (tenant_id, project_id, point_id, observed_at DESC);

CREATE TABLE IF NOT EXISTS ingestion_target.batch_receipts (
    batch_key text PRIMARY KEY,
    job_id text NOT NULL,
    tenant_id text NOT NULL,
    project_id text NOT NULL,
    feed text NOT NULL CHECK (feed = 'history'),
    row_count integer NOT NULL CHECK (row_count >= 0),
    checksum text NOT NULL CHECK (length(checksum) = 64),
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS target_receipts_job_idx
    ON ingestion_target.batch_receipts(job_id);
