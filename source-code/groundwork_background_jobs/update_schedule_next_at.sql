-- A schedule's next time.
UPDATE groundwork_background_jobs.background_job_schedules SET background_job_schedule__next_at = :background-job-schedule/next-at
WHERE background_job_schedule__key = :background-job-schedule/key
