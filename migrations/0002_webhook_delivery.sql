CREATE TABLE IF NOT EXISTS job_webhook_subscription (
    job_id TEXT PRIMARY KEY REFERENCES document_job(job_id) ON DELETE CASCADE,
    url TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS webhook_delivery (
    event_id TEXT PRIMARY KEY REFERENCES job_event(event_id) ON DELETE CASCADE,
    state TEXT NOT NULL CHECK (state IN ('pending', 'delivered', 'abandoned')),
    attempt_count INTEGER NOT NULL CHECK (attempt_count > 0),
    next_attempt_at TIMESTAMPTZ,
    last_error TEXT,
    delivered_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL,
    CHECK ((state = 'delivered') = (delivered_at IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS webhook_delivery_retry_idx
    ON webhook_delivery (next_attempt_at)
    WHERE state = 'pending';
