-- Introduce the public `validating` lifecycle state.
--
-- Acceptance now commits a job as `accepted`; an asynchronous preflight stage
-- derives the authoritative page count and document profile and moves the job
-- `accepted -> validating -> queued` (or `-> rejected`) before it is scheduled.
-- The existing single-column CHECK on document_job.state must admit the new
-- value.
--
-- The original constraint was declared inline, so its generated name is not
-- guaranteed across environments. Drop every state CHECK by definition rather
-- than by name, then add the canonical constraint. This keeps the migration
-- idempotent for the fail-closed ordered launcher and safe on a fresh database.

DO $$
DECLARE
    constraint_name text;
BEGIN
    FOR constraint_name IN
        SELECT con.conname
        FROM pg_constraint con
        JOIN pg_class rel ON rel.oid = con.conrelid
        WHERE rel.relname = 'document_job'
          AND con.contype = 'c'
          AND pg_get_constraintdef(con.oid) LIKE '%queued%'
    LOOP
        EXECUTE format('ALTER TABLE document_job DROP CONSTRAINT %I', constraint_name);
    END LOOP;
END $$;

ALTER TABLE document_job ADD CONSTRAINT document_job_state_check CHECK (
    state IN (
        'accepted', 'validating', 'queued', 'processing',
        'succeeded', 'failed', 'rejected', 'cancelled'
    )
);
