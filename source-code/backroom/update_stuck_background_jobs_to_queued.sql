-- Background jobs left running by a server that stopped: queued again, once past their
-- time limit and :background-job/stuck-after-time-limit-ms.
UPDATE backroom.background_jobs SET background_job__state = 'queued', background_job__locked_at = NULL
WHERE background_job__state = 'running'
  AND background_job__locked_at < now() - (background_job__timeout_ms + CAST(:background-job/stuck-after-time-limit-ms AS bigint)) * interval '1 millisecond'
