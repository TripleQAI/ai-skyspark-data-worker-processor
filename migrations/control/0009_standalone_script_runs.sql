CREATE TABLE IF NOT EXISTS ingestion.standalone_script_runs (
    run_id uuid PRIMARY KEY,
    script_id text NOT NULL,
    config_hash text NOT NULL,
    target text,
    status text NOT NULL CHECK (status IN ('running', 'completed', 'rejected', 'failed')),
    error_class text,
    result_json jsonb,
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz
);
CREATE INDEX IF NOT EXISTS standalone_script_runs_script_started_idx
    ON ingestion.standalone_script_runs (script_id, started_at DESC);
