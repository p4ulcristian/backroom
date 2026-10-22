# A job can give up for good

**Decision:** a job's work ends the job for good by throwing `(jobs/give-up "why")`. The job is dead at once, kept with that message, whatever attempts it had left. Any other failure waits and runs again until its attempts run out.

**Why:** some failures no retry fixes: the order was deleted, the address doesn't exist. Retrying them for hours hides the real reason behind a list of the same error and keeps a worker busy.

**Instead of:** every failure retried until its attempts run out.
