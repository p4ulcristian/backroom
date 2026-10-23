-- A background job that failed: queued again after :background-job/retry-ms, or dead when it has
-- had all its attempts or gave up for good.
UPDATE groundwork_background_jobs.background_jobs
SET background_job__state       = CASE WHEN CAST(:background-job/for-good AS boolean) OR background_job__attempts >= background_job__max_attempts THEN 'dead' ELSE 'queued' END,
    background_job__run_at      = now() + :background-job/retry-ms * interval '1 millisecond',
    background_job__last_error  = :background-job/last-error,
    background_job__locked_at   = NULL,
    background_job__finished_at = CASE WHEN CAST(:background-job/for-good AS boolean) OR background_job__attempts >= background_job__max_attempts THEN now() END
WHERE background_job__id = :background-job/id
RETURNING background_job__state
