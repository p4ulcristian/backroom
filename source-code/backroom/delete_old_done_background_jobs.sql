-- Done background jobs older than :background-job/forget-done-after-ms, forgotten.
DELETE FROM backroom.background_jobs
WHERE background_job__state = 'done'
  AND background_job__finished_at < now() - CAST(:background-job/forget-done-after-ms AS bigint) * interval '1 millisecond'
