-- A background job that ran, marked in its own run's transaction: the work and the mark commit
-- together. Only while this run still holds it (running, at this attempt): a run past its time
-- limit, already marked failed or queued again, marks nothing, and its work is undone.
UPDATE groundwork_background_jobs.background_jobs SET background_job__state = 'done', background_job__finished_at = now(), background_job__locked_at = NULL
WHERE background_job__id = :background-job/id
  AND background_job__state = 'running'
  AND background_job__attempts = :background-job/attempts
