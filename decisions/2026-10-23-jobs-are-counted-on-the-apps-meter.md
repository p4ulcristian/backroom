# Background jobs are counted on the app's meter

> Changes in part [Every value that sets behaviour is a setting](2026-10-22-every-value-that-sets-behaviour-is-a-setting.md) (the settings gain `:metric-buckets`) and [The app passes its log](2026-10-22-the-app-passes-its-log.md) (`start-workers!`' options gain `:meter`). Since v0.5.0, on meter v0.1.2.

**Decision:** `start-workers!` takes the app's meter (meter's `make-meter`) as a required option, `:meter`, never nil, beside `:log`, and declares backroom's metrics on it before anything runs; `add-background-job-to-queue!` takes it too, and counts the job it adds:

| metric | type | labels | counted |
|---|---|---|---|
| `backroom_jobs_added_total` | counter | `kind` | by `add-background-job-to-queue!`, when a row is added (a second job of a key already queued isn't), and by the schedules |
| `backroom_job_runs_total` | counter | `kind`; `outcome`: `done`, `failed` (runs again), `gave-up` (dead: its last attempt, or a give-up), `outcome-not-saved` | after each run on this server |
| `backroom_job_run_seconds` | histogram | `kind` | each run, from its start to its end or its time limit |
| `backroom_jobs_running` | gauge, +1 at a claim, -1 when the run is saved | `kind` | |
| `backroom_dispatcher_failures_total` | counter | | each failed round, beside `:backroom/dispatcher-failed` |

- **The kinds' set is `start-workers!`' `:kinds`.** A job of a kind this server doesn't run (another server's) is counted as `"other"`; meter says the first at `:warn`.
- **The run time's buckets are a setting**, `:metric-buckets {:job-run-seconds […]}`, required, in seconds; meter checks they go up.
- **`add-background-job-to-queue!` counts into the meter `start-workers!` declared on.** A meter without backroom's declaration counts nothing there, and meter writes `:meter/metric-not-declared` once at `:error`: a server that adds jobs runs the workers too, on the same meter (house does). The insert says whether it added the row (`added`), so a second job of a remembered key isn't counted.
- **A job added in a transaction is counted when it is added**, not when the transaction commits: one undone later is counted too. cellar has no hook after a commit.
- **No count of the queue by state**: a `GROUP BY state, kind` on every scrape is a query against the queue's table on every server, every 15 s; `list-queued-background-jobs` and `list-dead-background-jobs` say what waits and what died. Not added: not trivially cheap on a long queue.

**Why:** what an app's background work does (how much comes in, how much fails, how long it takes, how much runs now) is what an operator asks first when something is late, and backroom tells the outcomes apart already. The kinds are a closed set the workers hold.

**The price:** every app writes `:meter` in `start-workers!` and in each `add-background-job-to-queue!`, and a bucket setting. A meter takes one set of kinds. A rolled-back add is counted.

**Instead of:**
- `:meter` only on `start-workers!`, the jobs added counted by the dispatcher: it sees claims, not adds, and adds on other servers not at all
- the pool's meter (cellar's pool carries one) read by backroom: a value taken from another library's map, not handed in
- counting adds after commit: needs a hook in cellar's transactions for one counter
- the queue's size by state and kind, read on every scrape: one query per scrape per server against a table that may hold millions of done rows
