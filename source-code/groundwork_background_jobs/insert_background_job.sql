-- A background job, due at :background-job/run-at. With a :background-job/key, a background job of that key that is
-- still remembered is kept instead, and its id returned; added says which.
WITH inserted AS (
  INSERT INTO groundwork_background_jobs.background_jobs (background_job__kind, background_job__params, background_job__run_at, background_job__max_attempts, background_job__timeout_ms, background_job__key)
  VALUES (:background-job/kind, :background-job/params, :background-job/run-at, :background-job/max-attempts, :background-job/timeout-ms, :background-job/key)
  ON CONFLICT (background_job__key) DO NOTHING
  RETURNING background_job__id
)
SELECT background_job__id, true AS added FROM inserted
UNION ALL
SELECT background_job__id, false AS added FROM groundwork_background_jobs.background_jobs
WHERE background_job__key = :background-job/key AND NOT EXISTS (SELECT 1 FROM inserted)
