-- Schedules due now, locked for this server only.
SELECT background_job_schedule__key FROM backroom.background_job_schedules
WHERE background_job_schedule__next_at <= now()
FOR UPDATE SKIP LOCKED
