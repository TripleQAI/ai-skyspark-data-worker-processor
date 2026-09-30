ALTER TABLE ingestion.jobs DROP CONSTRAINT IF EXISTS jobs_status_check;
ALTER TABLE ingestion.jobs ADD CONSTRAINT jobs_status_check
    CHECK (status IN ('planned', 'running', 'certified', 'failed', 'quarantined', 'split'));
