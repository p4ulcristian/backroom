# groundwork-background-jobs

Background jobs for Clojure on Postgres: work that runs outside a request.
**The queue:** a background job is a row, saved on a pool or in the
request's transaction (then it exists only if that commits). **The
workers:** each server claims due background jobs (`FOR UPDATE SKIP
LOCKED`, so no two run the same one), runs each in one transaction within
its time limit, opened by the app as the kind's system actor, in which the
job is also marked done, retries a failure later and later (as long as its settings say), and
lets one give up for good. **The schedules:** cron lines in UTC that queue
a background job at their times, on one server.

```clojure
io.github.p4ulcristian/groundwork-background-jobs {:git/tag "v0.5.7" :git/sha "c19386e"}
```

Needs only [groundwork-postgres](https://github.com/p4ulcristian/groundwork-postgres) (Postgres, tables
as maps), [groundwork-shaped-json](https://github.com/p4ulcristian/groundwork-shaped-json) (params kept with
their types), [groundwork-logs](https://github.com/p4ulcristian/groundwork-logs) (the
app's log, handed down) and [groundwork-metrics](https://github.com/p4ulcristian/groundwork-metrics)
(the app's metrics, handed in). One public namespace, `groundwork-background-jobs.api`; its part is
`groundwork-background-jobs.cron`.

The app makes its shaper once and hands it in: `add-background-job-to-queue!`
and `start-workers!` take it under `:shaper` in their options. groundwork-background-jobs never
makes a shaper. So with the log: `start-workers!` takes the app's
`(log level event data)` under `:log`, each run gets one with
`{:background-job/id … :background-job/kind …}` as its context (its
context's `:log`), and groundwork-background-jobs writes only there: `:groundwork-background-jobs/failed`
(warn, runs again), `:groundwork-background-jobs/gave-up` (error, dead),
`:groundwork-background-jobs/outcome-not-saved`, `:groundwork-background-jobs/dispatcher-failed`,
`:groundwork-background-jobs/forgot-done` (info with how many, debug for none),
`:groundwork-background-jobs/forgetting-failed`
([why](decisions/2026-10-22-the-app-passes-its-log.md)).

And the meter: `start-workers!` takes the app's meter under `:meter` and
declares groundwork-background-jobs's metrics on it, the kinds of `:kinds` as their set;
`add-background-job-to-queue!` takes it too
([why](decisions/2026-10-23-jobs-are-counted-on-the-apps-meter.md)):
`groundwork_background_jobs_jobs_added_total{kind}`, `groundwork_background_jobs_job_runs_total{kind,outcome}`
(`done`, `failed`, `gave-up`, `outcome-not-saved`),
`groundwork_background_jobs_job_run_seconds{kind}` in the settings' `:metric-buckets`,
`groundwork_background_jobs_jobs_running{kind}` and `groundwork_background_jobs_dispatcher_failures_total`. A
kind this server doesn't run is counted as `"other"`.

## The api

| function | does |
|---|---|
| `background-jobs-tables` `background-jobs-table` `background-job-schedules-table` | its two tables, `groundwork_background_jobs.background_jobs` and `groundwork_background_jobs.background_job_schedules`, as maps for groundwork-postgres |
| `add-background-job-to-queue!` | saves a background job of a kind, its params checked by the kind's `:params-shape`, and counts it; options `:shaper` and `:meter` (the app's), `:run-in-ms` and `:key` (nil, or one at a time), all written out |
| `make-give-up-exception` | what a kind's `:run` throws to end its background job for good: dead at once, kept with the message |
| `start-workers!` `stop-workers!` `settings-shape` | the workers on this server (`:shaper` `:log` `:meter` `:run-in-transaction-as` `:kinds` `:schedules` and the settings); stopping waits for running ones, at most `:stop-wait-ms`; the settings as a malli shape |
| `list-queued-background-jobs` `list-dead-background-jobs` | what waits, and what had all its attempts or gave up, with its last error |
| `run-background-job-again-now!` | queues a dead or waiting background job to run now, its attempts counted afresh |

```clojure
(require '[groundwork-background-jobs.api :as background-jobs]
         '[groundwork-postgres.api :as postgres]
         '[groundwork-logs.api :as logs]
         '[groundwork-metrics.api :as metrics]
         '[groundwork-shaped-json.api :as shaped-json])

;; the app's shaper, made once (groundwork-shaped-json's settings, every key written out)
(def shaper
  (shaped-json/make-shaper-from-settings
   {:most-shapes-kept 1000
    :json-limits      {:max-nesting-depth 1000 :max-number-length 1000 :max-string-length 20000000
                       :max-name-length 50000 :max-document-length nil :max-token-count nil
                       :max-writing-nesting-depth 1000}}))

;; the app's log, made once (groundwork-logs's settings, every key written out)
(def log (logs/log-function (logs/make-logger {:lowest-level :info :format :json
                                                      :errors-kept 1000 :write-to :stdout})))

;; the app's meter, made once (groundwork-metrics's settings, every key written out)
(def meter (metrics/make-meter {:jvm-metrics true :most-series-per-metric 5000} {:log log}))

(def send-invoice-email
  {:name         :billing/send-invoice-email
   :system-actor :invoice-emails                 ; the named actor its one transaction runs as
   :params-shape [:map [:order/id :uuid]]
   :run          (fn [{:keys [transaction background-job-id attempt log]} {:order/keys [id]}] …) ; log: the job's
   :timeout-ms   60000
   :attempts     5})

(def send-nightly-report
  {:name :reports/send-nightly-report :system-actor :nightly-reports :params-shape [:map] :run (fn [_ _] …)
   :timeout-ms 600000 :attempts 3})

(postgres/migrate! pool {:tables     (into app-tables background-jobs/background-jobs-tables)
                       :extensions [] :functions [] :views []}
                 changes migrate-options) ; groundwork-postgres's migrate! options

(def workers (background-jobs/start-workers! pool {:shaper                      shaper
                                            :log                       log
                                            :meter                     meter
                                            ;; opens each job's one transaction as its kind's actor: groundwork-access's, in groundwork
                                            :run-in-transaction-as     (fn [pool actor log f] (access/run-as-system! pool actor {:read-only? false :log log} f))
                                            :kinds                     [send-invoice-email send-nightly-report]
                                            :schedules                 [{:kind send-nightly-report :params {} :cron "0 1 * * *"}]
                                            :workers                   4
                                            :poll-ms                   1000        ; how often to look for due ones
                                            :retry-first-wait-ms       30000       ; a failure runs again in 30 s,
                                            :retry-wait-factor         2           ; then 1 min, 2 min…
                                            :retry-max-wait-ms         3600000     ; never more than an hour apart
                                            :stuck-after-time-limit-ms 60000       ; left running by a stopped server: again past its limit and a minute
                                            :forget-done-after-ms      604800000   ; done ones kept a week
                                            :forget-done-every-ms      3600000     ; and deleted once past that, every hour
                                            :stop-wait-ms              30000       ; stop-workers! waits this long for running ones
                                            :metric-buckets            {:job-run-seconds [0.01 0.1 1 10 60 600]}}))   ; seconds a run takes

;; in the transaction that marks the order paid: no email for an order that wasn't
(background-jobs/add-background-job-to-queue! transaction send-invoice-email {:order/id order-id} {:shaper shaper :meter meter :run-in-ms 0 :key nil})

;; the order was deleted: no retry helps
(throw (background-jobs/make-give-up-exception "The order was deleted" {:order/id order-id}))
```

Every job kind names its system actor, `:system-actor`, a keyword without a
namespace: the app's `:run-in-transaction-as`, `(fn [pool system-actor log f])`,
opens the job's one transaction as it, carrying `log`, the job's (groundwork-postgres's
`:log`: work kept for after the commit that fails is written there, with
the job's id and kind), and returns what `f` returns
(groundwork-access's `run-as-system!`, in groundwork), so a job reaches what that
actor's sentences give it. The job is marked done inside that transaction:
the work and the mark commit together, or neither; a run past its time
limit, already marked failed, marks nothing and its work is undone. groundwork-background-jobs
knows no groundwork-access: it hands the kind's actor over
([why](decisions/2026-10-23-a-job-kind-names-its-system-actor.md)).

Every setting and option is required, and every key of a kind and a
schedule: a missing or unknown one is refused, named, with every other
problem at once, before anything runs. What stays fixed (cron lines in UTC,
among others), and why, is in
[the decision](decisions/2026-10-22-every-value-that-sets-behaviour-is-a-setting.md).

A background job can run twice (a server that stops mid-run leaves it
running; it is queued again once its time limit and
`:stuck-after-time-limit-ms` have passed), so its work must be safe to
repeat. Done background jobs older than `:forget-done-after-ms` are deleted
when the workers start and every `:forget-done-every-ms` while they run, on
every server at once: a row another server is deleting is skipped, not
waited for ([why](decisions/2026-10-23-done-background-jobs-are-forgotten-while-the-workers-run.md)).
The app's own Postgres role, which owns nothing, uses the tables once
`postgres/grant-tables-to-role!` is given them.

## Tests

```sh
clojure -X:test     # the Postgres tests are skipped without a database

# with a throwaway Postgres:
initdb -D /tmp/background-jobs-pg -A trust -U "$USER"
pg_ctl -D /tmp/background-jobs-pg -o "-p 5499 -k /tmp -c listen_addresses=localhost" -l /tmp/background-jobs-pg.log start
BACKGROUND_JOBS_TEST_PORT=5499 BACKGROUND_JOBS_TEST_USER=$USER clojure -X:test
pg_ctl -D /tmp/background-jobs-pg stop
```

A background job that gives up logs its warning during the tests; that is
expected.

Why it is the way it is: [`decisions/`](decisions/README.md).

MIT license.
