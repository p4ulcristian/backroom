-- Done background jobs are kept a week, then forgotten.
DELETE FROM backroom.background_jobs WHERE background_job__state = 'done' AND background_job__finished_at < now() - interval '7 days'
