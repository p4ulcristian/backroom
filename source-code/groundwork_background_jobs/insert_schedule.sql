-- A schedule's first time, unless it has one already.
INSERT INTO groundwork_background_jobs.background_job_schedules (background_job_schedule__key, background_job_schedule__next_at)
VALUES (:background-job-schedule/key, :background-job-schedule/next-at)
ON CONFLICT (background_job_schedule__key) DO NOTHING
