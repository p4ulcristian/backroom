-- The next due background job, for one worker: no other worker, on any server, gets it.
UPDATE backroom.background_jobs
SET background_job__state = 'running', background_job__locked_at = now(), background_job__attempts = background_job__attempts + 1
WHERE background_job__id = (SELECT background_job__id FROM backroom.background_jobs
                 WHERE background_job__state = 'queued' AND background_job__run_at <= now()
                 ORDER BY background_job__run_at
                 FOR UPDATE SKIP LOCKED
                 LIMIT 1)
RETURNING background_job__id, background_job__kind, background_job__params, background_job__attempts, background_job__timeout_ms
