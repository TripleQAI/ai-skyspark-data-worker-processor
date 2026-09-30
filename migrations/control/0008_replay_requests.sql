CREATE TABLE IF NOT EXISTS ingestion.replay_approval_grants (
    approval_ref text PRIMARY KEY,
    tenant_id text NOT NULL,
    project_id text NOT NULL,
    config_hash text NOT NULL,
    feed text NOT NULL CHECK (feed IN ('history', 'rules')),
    inventory_version text NOT NULL,
    window_start timestamptz NOT NULL,
    window_end timestamptz NOT NULL,
    max_jobs integer NOT NULL CHECK (max_jobs > 0),
    approved_by text NOT NULL,
    expires_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK (window_start < window_end)
);

CREATE TABLE IF NOT EXISTS ingestion.approved_replay_requests (
    request_id uuid PRIMARY KEY,
    tenant_id text NOT NULL,
    project_id text NOT NULL,
    config_hash text NOT NULL,
    feed text NOT NULL CHECK (feed IN ('history', 'rules')),
    inventory_version text NOT NULL,
    window_start timestamptz NOT NULL,
    window_end timestamptz NOT NULL,
    requested_at timestamptz NOT NULL,
    requested_by text NOT NULL,
    approval_ref text NOT NULL,
    reason text NOT NULL,
    queue_class text NOT NULL,
    run_id text NOT NULL UNIQUE REFERENCES ingestion.runs(run_id),
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK (window_start < window_end),
    CHECK (window_end <= requested_at)
);
CREATE INDEX IF NOT EXISTS replay_requests_scope_idx
    ON ingestion.approved_replay_requests
    (tenant_id, project_id, created_at DESC);
