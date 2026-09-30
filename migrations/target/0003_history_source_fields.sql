ALTER TABLE ingestion_target.history_observations
    ADD COLUMN IF NOT EXISTS source_timestamp text,
    ADD COLUMN IF NOT EXISTS source_timezone text,
    ADD COLUMN IF NOT EXISTS source_status text;

ALTER TABLE ingestion_target.history_observation_revisions
    ADD COLUMN IF NOT EXISTS source_timestamp text,
    ADD COLUMN IF NOT EXISTS source_timezone text,
    ADD COLUMN IF NOT EXISTS source_status text;
