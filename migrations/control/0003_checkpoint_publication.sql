ALTER TABLE ingestion.checkpoints
    ADD COLUMN IF NOT EXISTS baseline_at timestamptz,
    ADD COLUMN IF NOT EXISTS last_run_id text;

CREATE TABLE IF NOT EXISTS ingestion.site_run_completions (
    run_id text NOT NULL REFERENCES ingestion.runs(run_id),
    site_ref text NOT NULL,
    job_count integer NOT NULL CHECK (job_count > 0),
    completed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, site_ref)
);

CREATE INDEX IF NOT EXISTS site_run_completions_site_idx
    ON ingestion.site_run_completions(site_ref, completed_at);
