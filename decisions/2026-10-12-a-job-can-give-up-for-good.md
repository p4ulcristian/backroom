# A job can give up for good

> **Changed in part** by [Every value that sets behaviour is a setting](2026-10-22-every-value-that-sets-behaviour-is-a-setting.md) (2026-10-22, v0.1.2): `make-give-up-exception` takes the message and its data always, `{}` for none.

**Decision:** a job's work ends the job for good by throwing `(jobs/give-up "why")`. The job is dead at once, kept with that message, whatever attempts it had left. Any other failure waits and runs again until its attempts run out.

**Why:** some failures no retry fixes: the order was deleted, the address doesn't exist. Retrying them for hours hides the real reason behind a list of the same error and keeps a worker busy.

**Instead of:** every failure retried until its attempts run out.
