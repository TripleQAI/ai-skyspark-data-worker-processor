CREATE TABLE IF NOT EXISTS ingestion_target.history_observation_revisions (
    tenant_id text NOT NULL,
    project_id text NOT NULL,
    point_id text NOT NULL,
    observed_at timestamptz NOT NULL,
    row_hash text NOT NULL CHECK (length(row_hash) = 64),
    value_kind text NOT NULL CHECK (value_kind IN ('bool', 'str', 'num', 'na')),
    value_bool boolean,
    value_str text,
    value_num numeric,
    source_job_id text NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, project_id, point_id, observed_at, row_hash)
);
CREATE INDEX IF NOT EXISTS history_revisions_by_point_time_idx
    ON ingestion_target.history_observation_revisions
    (tenant_id, project_id, point_id, observed_at DESC);
