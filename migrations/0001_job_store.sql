CREATE TABLE IF NOT EXISTS document_job (
    job_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (
        state IN ('accepted', 'queued', 'processing', 'succeeded', 'failed', 'rejected', 'cancelled')
    ),
    priority INTEGER NOT NULL CHECK (priority IN (10, 20, 30, 40)),
    state_version INTEGER NOT NULL CHECK (state_version >= 0),
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CHECK (payload->>'job_id' = job_id),
    CHECK ((payload->>'version')::INTEGER = state_version)
);

CREATE INDEX IF NOT EXISTS document_job_recovery_idx
    ON document_job (priority DESC, created_at)
    WHERE state NOT IN ('succeeded', 'failed', 'rejected', 'cancelled');

CREATE TABLE IF NOT EXISTS job_event (
    event_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES document_job(job_id) ON DELETE CASCADE,
    job_version INTEGER NOT NULL CHECK (job_version > 0),
    event_type TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL,
    UNIQUE (job_id, job_version),
    CHECK (payload->>'event_id' = event_id),
    CHECK ((payload->>'job_version')::INTEGER = job_version)
);

CREATE INDEX IF NOT EXISTS job_event_job_order_idx
    ON job_event (job_id, job_version);
