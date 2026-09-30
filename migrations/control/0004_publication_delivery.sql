ALTER TABLE ingestion.publication_outbox
    DROP CONSTRAINT IF EXISTS publication_outbox_state_check;

ALTER TABLE ingestion.publication_outbox
    ADD CONSTRAINT publication_outbox_state_check
    CHECK (state IN ('pending', 'sending', 'delivered'));

ALTER TABLE ingestion.publication_outbox
    ADD COLUMN IF NOT EXISTS claim_owner text,
    ADD COLUMN IF NOT EXISTS claim_until timestamptz,
    ADD COLUMN IF NOT EXISTS delivery_attempts integer NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS next_attempt_at timestamptz NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS event_id text,
    ADD COLUMN IF NOT EXISTS last_error text;

CREATE INDEX IF NOT EXISTS publication_ready_idx
    ON ingestion.publication_outbox(state, next_attempt_at, claim_until, created_at)
    WHERE state <> 'delivered';
