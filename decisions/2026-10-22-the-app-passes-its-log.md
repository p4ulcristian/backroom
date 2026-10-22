# The app passes its log; backroom never decides how a line is written

> **Changed in part** by [Background jobs are counted on the app's meter](2026-10-23-jobs-are-counted-on-the-apps-meter.md) (2026-10-23, v0.5.0): `start-workers!` and `add-background-job-to-queue!` also take `:meter`, the app's meter.

> **Changed in part** by [Done background jobs are forgotten while the workers run](2026-10-23-done-background-jobs-are-forgotten-while-the-workers-run.md) (2026-10-23, v0.4.0): the workers' log also gets `:backroom/forgot-done` and `:backroom/forgetting-failed`.

> Changes in part [Every value that sets behaviour is a setting](2026-10-22-every-value-that-sets-behaviour-is-a-setting.md) (one log, `System.Logger` `backroom`). Since v0.3.0, on cellar v0.2.0 and logbook v0.1.0.

**Decision:** `start-workers!` takes the app's log, `(log level event data)` as logbook makes it, as a required key, `:log`, in its options, checked by malli as a function and then given to logbook's `with-context` once, so a function logbook didn't make is refused at the start (`:logbook/not-a-log`), not at the first run. backroom writes nowhere else.

**Each run gets its own log**, `(logbook/with-context log {:background-job/id id :background-job/kind kind})`, in its context beside `:transaction`, `:background-job-id` and `:attempt`:

```clojure
(def send-invoice-email
  {:name :billing/send-invoice-email :params-shape [:map [:order/id :uuid]] :timeout-ms 60000 :attempts 5
   :run  (fn [{:keys [transaction log]} {:order/keys [id]}]
           (log :info :billing/invoice-sent {:order/id id}))})   ; the line carries the background job's id and kind
```

What backroom writes, each with the run's log (so its context names the background job) but the dispatcher's:

| level | event | data |
|---|---|---|
| `:warn` | `:backroom/failed` | `:background-job/attempt`, `:background-job/retry-in-ms`, and `:error` the exception, or `:background-job/ran-out-of-time? true` |
| `:error` | `:backroom/gave-up` | `:background-job/attempts`, and the same `:error` or `:background-job/ran-out-of-time?`: the background job is dead, its last attempt failed or it threw a give-up |
| `:error` | `:backroom/outcome-not-saved` | `:error`: the run ended but its row couldn't be updated |
| `:warn` | `:backroom/dispatcher-failed` | `:error`: one round of looking for due background jobs failed; the next tries again (the workers' log, no background job) |

Whether a failure gave up is Postgres's answer: the update that saves the failure returns the background job's new state, `dead` or `queued`. `add-background-job-to-queue!` writes no line and takes no log.

**Why:** a dead background job is something to act on, so it is an error, kept for a monitor to pull ("An error is a log line, kept for pulling"); a failure that runs again is a warning. The run's own lines carry the background job as their context, as a request's carry the request, without the app writing it into each call ("Context is handed down as a value"). backroom depends on logbook for `with-context` only.

**The price:** `start-workers!` takes one key more, and backroom takes logbook; every caller of v0.2 changes.

**Instead of:**
- one warning for every failure, as before: a dead background job would look like one that runs again in a minute
- the background job's id and kind in each line's data: every kind's own lines would have to repeat them
- `:log` on `add-background-job-to-queue!` too: it writes no line
