-- A schedule's next time.
UPDATE backroom.background_job_schedules SET background_job_schedule__next_at = :background-job-schedule/next-at
WHERE background_job_schedule__key = :background-job-schedule/key
