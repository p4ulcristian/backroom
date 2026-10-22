# A job kind is a map, and the library makes its own tables

> **Changed in part** by [Every value that sets behaviour is a setting](2026-10-22-every-value-that-sets-behaviour-is-a-setting.md) (2026-10-22, v0.1.2): the retry waits (30 s doubling up to an hour), the minute past a stuck job's time limit and the week done jobs are kept are settings, `:retry-first-wait-ms`, `:retry-wait-factor`, `:retry-max-wait-ms`, `:stuck-after-time-limit-ms` and `:forget-done-after-ms`.

> **Changed in part** by [backroom is its own library](2026-10-22-backroom-is-its-own-library.md) (2026-10-22): the library is backroom (`backroom.api`), its tables `backroom.background_jobs` and `backroom.background_job_schedules`.

> **Changed in part** by Groundwork's decision "Groundwork's own tables are table maps, in their own folder" (2026-10-22): `create-tables!` is gone; the tables are maps (`background-jobs-tables`) passed to groundwork.schema.

> **Changed in part** by Groundwork's decision "The background work is called background jobs" (2026-10-22): job is background job everywhere (`groundwork.background-jobs`, `:background-job/…`, `add-background-job-to-queue!`); the tables' names differ from the old Groundwork's.

> Builds on [A job can give up for good](2026-10-12-a-job-can-give-up-for-good.md) and Groundwork's decision "The jobs library is named groundwork.background-jobs".

**Decision:**

- **A job kind is a map**, like a step: `{:name :params-shape :run :timeout-ms :attempts}`. `add-job-to-queue!` takes the kind itself, and `start-workers!` takes the kinds this server runs. A kind's `:run` gets `{:transaction :job-id :attempt}` and the params as shaped data, and runs in one transaction.
- **The workers poll.** Every `:poll-ms` (and when a worker finishes) the dispatcher queues due schedules, queues stuck jobs again, and gives each free worker a due job. No LISTEN/NOTIFY yet: a new job is found within `:poll-ms`.
- **`create-tables!` makes the tables**, `groundwork.jobs` and `groundwork.job_schedules`, unless they exist, in the old Groundwork's shape, so its apps' tables are the same.
- What the old jobs did stays: claimed with FOR UPDATE SKIP LOCKED, retried with a growing wait up to an hour, dead after its attempts or a give-up, one job per `:key`, stuck jobs queued again, done jobs kept a week, schedules as cron lines in UTC run by one server.

**Why:** a kind as a map is read in one place and passed like any value; no registry to look a kind up in. Polling needs nothing but the pool, which is all groundwork.sql has today. The tables have to exist before groundwork.schema does.

**The price:** a job waits up to `:poll-ms` before it runs. `create-tables!` can make the tables but not change them: a change to their shape waits for groundwork.schema, where these tables become table maps.

**Not in yet:** LISTEN/NOTIFY for an instant start; a job running in a scope behind the wall (needs groundwork.server.access); an off switch for previews; the settings' schema.

**Instead of:**
- kinds gathered into one registry from the settings, enqueued by name (the old way): a name is shorter to pass, but the kind is looked up instead of handed
- numbered migrations for the library's tables: a change later is easy, but migrations are what table maps replace
