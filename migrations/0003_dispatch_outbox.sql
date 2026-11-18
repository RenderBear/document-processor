CREATE TABLE IF NOT EXISTS dispatch_outbox (
    command_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES document_job(job_id) ON DELETE CASCADE,
    subject TEXT NOT NULL,
    payload JSONB NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending', 'published')),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    published_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS dispatch_outbox_pending_idx
    ON dispatch_outbox (created_at, command_id)
    WHERE state = 'pending';

CREATE INDEX IF NOT EXISTS dispatch_outbox_job_idx
    ON dispatch_outbox (job_id, created_at DESC);
