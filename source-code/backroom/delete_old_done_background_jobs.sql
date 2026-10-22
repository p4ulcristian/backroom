-- Done background jobs older than :background-job/forget-done-after-ms, forgotten. A row another
-- server is forgetting at the same moment is skipped, not waited for: that server forgets it.
DELETE FROM backroom.background_jobs
WHERE background_job__id IN (
  SELECT background_job__id FROM backroom.background_jobs
  WHERE background_job__state = 'done'
    AND background_job__finished_at < now() - CAST(:background-job/forget-done-after-ms AS bigint) * interval '1 millisecond'
  FOR UPDATE SKIP LOCKED)
