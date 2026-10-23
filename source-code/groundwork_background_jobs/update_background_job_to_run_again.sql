-- A dead or waiting background job, queued to run now with its attempts counted afresh.
UPDATE groundwork_background_jobs.background_jobs
SET background_job__state = 'queued', background_job__run_at = now(), background_job__attempts = 0, background_job__finished_at = NULL
WHERE background_job__id = :background-job/id AND background_job__state IN ('dead', 'queued')
RETURNING background_job__id
