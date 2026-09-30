CREATE SCHEMA IF NOT EXISTS ingestion;

CREATE TABLE IF NOT EXISTS ingestion.pipeline_versions (
    config_hash text PRIMARY KEY,
    tenant_id text NOT NULL,
    project_id text NOT NULL,
    profile_json jsonb NOT NULL,
    binding_json jsonb NOT NULL,
    manifest_json jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK (length(config_hash) = 64)
);

CREATE TABLE IF NOT EXISTS ingestion.runs (
    run_id text PRIMARY KEY,
    tenant_id text NOT NULL,
    project_id text NOT NULL,
    feed text NOT NULL CHECK (feed IN ('metadata', 'history', 'rules')),
    scheduled_at timestamptz NOT NULL,
    window_start timestamptz,
    window_end timestamptz,
    config_hash text NOT NULL REFERENCES ingestion.pipeline_versions(config_hash),
    inventory_version text,
    expected_job_count integer NOT NULL CHECK (expected_job_count >= 0),
    status text NOT NULL DEFAULT 'planned' CHECK (status IN ('planned', 'running', 'certified', 'partial', 'blocked')),
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, project_id, run_id),
    CHECK ((window_start IS NULL AND window_end IS NULL) OR (window_start IS NOT NULL AND window_end > window_start))
);

CREATE TABLE IF NOT EXISTS ingestion.jobs (
    job_id text PRIMARY KEY,
    run_id text NOT NULL,
    tenant_id text NOT NULL,
    project_id text NOT NULL,
    site_ref text NOT NULL,
    feed text NOT NULL CHECK (feed IN ('metadata', 'history', 'rules')),
    scope_ids jsonb NOT NULL CHECK (jsonb_typeof(scope_ids) = 'array'),
    window_start timestamptz,
    window_end timestamptz,
    config_hash text NOT NULL,
    inventory_version text,
    status text NOT NULL DEFAULT 'planned' CHECK (status IN ('planned', 'running', 'certified', 'failed', 'quarantined')),
    fence_token bigint NOT NULL DEFAULT 0,
    lease_owner text,
    lease_expires_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (tenant_id, project_id, run_id) REFERENCES ingestion.runs(tenant_id, project_id, run_id),
    CHECK ((window_start IS NULL AND window_end IS NULL) OR (window_start IS NOT NULL AND window_end > window_start))
);
CREATE INDEX IF NOT EXISTS jobs_run_status_idx ON ingestion.jobs(run_id, status);
CREATE INDEX IF NOT EXISTS jobs_lease_idx ON ingestion.jobs(lease_expires_at) WHERE status = 'running';
CREATE INDEX IF NOT EXISTS jobs_scope_idx ON ingestion.jobs(tenant_id, project_id, site_ref, feed, created_at);

CREATE TABLE IF NOT EXISTS ingestion.job_children (
    parent_job_id text NOT NULL REFERENCES ingestion.jobs(job_id),
    child_job_id text NOT NULL REFERENCES ingestion.jobs(job_id),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (parent_job_id, child_job_id),
    CHECK (parent_job_id <> child_job_id)
);

CREATE TABLE IF NOT EXISTS ingestion.attempts (
    job_id text NOT NULL REFERENCES ingestion.jobs(job_id),
    attempt_no integer NOT NULL CHECK (attempt_no >= 1),
    worker_id text NOT NULL,
    fence_token bigint NOT NULL,
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    outcome text,
    error_class text,
    PRIMARY KEY (job_id, attempt_no)
);

CREATE TABLE IF NOT EXISTS ingestion.dispatch_outbox (
    job_id text PRIMARY KEY REFERENCES ingestion.jobs(job_id),
    queue_class text NOT NULL,
    state text NOT NULL DEFAULT 'pending' CHECK (state IN ('pending', 'sending', 'sent')),
    claim_owner text,
    claim_until timestamptz,
    delivery_attempts integer NOT NULL DEFAULT 0 CHECK (delivery_attempts >= 0),
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    sqs_message_id text,
    last_error text,
    created_at timestamptz NOT NULL DEFAULT now(),
    sent_at timestamptz
);
CREATE INDEX IF NOT EXISTS dispatch_ready_idx ON ingestion.dispatch_outbox(state, next_attempt_at, claim_until, created_at)
    WHERE state <> 'sent';

CREATE TABLE IF NOT EXISTS ingestion.raw_artifacts (
    artifact_id text PRIMARY KEY,
    job_id text NOT NULL REFERENCES ingestion.jobs(job_id),
    object_key text NOT NULL,
    checksum text NOT NULL,
    byte_count bigint NOT NULL CHECK (byte_count >= 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (job_id, object_key)
);

CREATE TABLE IF NOT EXISTS ingestion.sink_receipts (
    batch_key text PRIMARY KEY,
    job_id text NOT NULL REFERENCES ingestion.jobs(job_id),
    sink_kind text NOT NULL CHECK (sink_kind IN ('s3', 'timescale')),
    row_count bigint NOT NULL CHECK (row_count >= 0),
    checksum text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ingestion.certifications (
    job_id text PRIMARY KEY REFERENCES ingestion.jobs(job_id),
    artifact_id text NOT NULL REFERENCES ingestion.raw_artifacts(artifact_id),
    batch_key text NOT NULL REFERENCES ingestion.sink_receipts(batch_key),
    completed_scope jsonb NOT NULL CHECK (jsonb_typeof(completed_scope) = 'array'),
    certified_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ingestion.checkpoints (
    tenant_id text NOT NULL,
    project_id text NOT NULL,
    site_ref text NOT NULL,
    feed text NOT NULL CHECK (feed IN ('metadata', 'history', 'rules')),
    certified_through timestamptz,
    inventory_version text,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, project_id, site_ref, feed)
);

CREATE TABLE IF NOT EXISTS ingestion.publication_outbox (
    publication_id text PRIMARY KEY,
    job_id text NOT NULL REFERENCES ingestion.jobs(job_id),
    state text NOT NULL DEFAULT 'pending' CHECK (state IN ('pending', 'delivered')),
    created_at timestamptz NOT NULL DEFAULT now(),
    delivered_at timestamptz
);

CREATE TABLE IF NOT EXISTS ingestion.replay_requests (
    replay_id text PRIMARY KEY,
    tenant_id text NOT NULL,
    project_id text NOT NULL,
    site_ref text NOT NULL,
    feed text NOT NULL CHECK (feed IN ('metadata', 'history', 'rules')),
    window_start timestamptz,
    window_end timestamptz,
    requested_by text NOT NULL,
    reason text NOT NULL,
    status text NOT NULL DEFAULT 'requested',
    created_at timestamptz NOT NULL DEFAULT now()
);
