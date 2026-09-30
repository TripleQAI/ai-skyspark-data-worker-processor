ALTER TABLE ingestion.dispatch_outbox
    ADD COLUMN IF NOT EXISTS redrive_count integer NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_requeued_at timestamptz;

ALTER TABLE ingestion.dispatch_outbox
    ADD CONSTRAINT dispatch_redrive_count_nonnegative CHECK (redrive_count >= 0);

CREATE INDEX IF NOT EXISTS dispatch_stale_sent_idx
    ON ingestion.dispatch_outbox(sent_at, job_id)
    WHERE state = 'sent';
