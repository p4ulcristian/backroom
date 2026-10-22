-- A background job that ran.
UPDATE backroom.background_jobs SET background_job__state = 'done', background_job__finished_at = now(), background_job__locked_at = NULL
WHERE background_job__id = :background-job/id
