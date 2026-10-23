-- Background jobs in a state, newest first.
SELECT background_job__id, background_job__kind, background_job__params, background_job__state, background_job__run_at, background_job__attempts,
       background_job__max_attempts, background_job__last_error, background_job__created_at
FROM groundwork_background_jobs.background_jobs WHERE background_job__state = :background-job/state
ORDER BY background_job__created_at DESC LIMIT :limit
