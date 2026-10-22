# backroom

Background jobs for Clojure on Postgres: work that runs outside a request.
**The queue:** a background job is a row, saved on a pool or in the
request's transaction (then it exists only if that commits). **The
workers:** each server claims due background jobs (`FOR UPDATE SKIP
LOCKED`, so no two run the same one), runs each in one transaction within
its time limit, retries a failure later and later (as long as its settings say), and
lets one give up for good. **The schedules:** cron lines in UTC that queue
a background job at their times, on one server.

```clojure
io.github.p4ulcristian/backroom {:git/tag "v0.4.0" :git/sha "f859f4a"}
```

Needs only [cellar](https://github.com/p4ulcristian/cellar) (Postgres, tables
as maps), [wire](https://github.com/p4ulcristian/wire) (params kept with
their types), [logbook](https://github.com/p4ulcristian/logbook) (the
app's log, handed down) and [meter](https://github.com/p4ulcristian/meter)
(the app's metrics, handed in). One public namespace, `backroom.api`; its part is
`backroom.cron`.

The app makes its wire once and hands it in: `add-background-job-to-queue!`
and `start-workers!` take it under `:wire` in their options. backroom never
makes a wire. So with the log: `start-workers!` takes the app's
`(log level event data)` under `:log`, each run gets one with
`{:background-job/id … :background-job/kind …}` as its context (its
context's `:log`), and backroom writes only there: `:backroom/failed`
(warn, runs again), `:backroom/gave-up` (error, dead),
`:backroom/outcome-not-saved`, `:backroom/dispatcher-failed`,
`:backroom/forgot-done` (info with how many, debug for none),
`:backroom/forgetting-failed`
([why](decisions/2026-10-22-the-app-passes-its-log.md)).

And the meter: `start-workers!` takes the app's meter under `:meter` and
declares backroom's metrics on it, the kinds of `:kinds` as their set;
`add-background-job-to-queue!` takes it too
([why](decisions/2026-10-23-jobs-are-counted-on-the-apps-meter.md)):
`backroom_jobs_added_total{kind}`, `backroom_job_runs_total{kind,outcome}`
(`done`, `failed`, `gave-up`, `outcome-not-saved`),
`backroom_job_run_seconds{kind}` in the settings' `:metric-buckets`,
`backroom_jobs_running{kind}` and `backroom_dispatcher_failures_total`. A
kind this server doesn't run is counted as `"other"`.

## The api

| function | does |
|---|---|
| `background-jobs-tables` `background-jobs-table` `background-job-schedules-table` | its two tables, `backroom.background_jobs` and `backroom.background_job_schedules`, as maps for cellar |
| `add-background-job-to-queue!` | saves a background job of a kind, its params checked by the kind's `:params-shape`, and counts it; options `:wire` and `:meter` (the app's), `:run-in-ms` and `:key` (nil, or one at a time), all written out |
| `make-give-up-exception` | what a kind's `:run` throws to end its background job for good: dead at once, kept with the message |
| `start-workers!` `stop-workers!` `settings-shape` | the workers on this server (`:wire` `:log` `:meter` `:kinds` `:schedules` and the settings); stopping waits for running ones, at most `:stop-wait-ms`; the settings as a malli shape |
| `list-queued-background-jobs` `list-dead-background-jobs` | what waits, and what had all its attempts or gave up, with its last error |
| `run-background-job-again-now!` | queues a dead or waiting background job to run now, its attempts counted afresh |

```clojure
(require '[backroom.api :as backroom]
         '[cellar.api :as cellar]
         '[logbook.api :as logbook]
         '[meter.api :as meter]
         '[wire.api :as wire])

;; the app's wire, made once (wire's settings, every key written out)
(def wire
  (wire/make-wire-from-settings
   {:most-shapes-kept 1000
    :json-limits      {:max-nesting-depth 1000 :max-number-length 1000 :max-string-length 20000000
                       :max-name-length 50000 :max-document-length nil :max-token-count nil
                       :max-writing-nesting-depth 1000}}))

;; the app's log, made once (logbook's settings, every key written out)
(def log (logbook/log-function (logbook/make-logbook {:lowest-level :info :format :json
                                                      :errors-kept 1000 :write-to :stdout})))

;; the app's meter, made once (meter's settings, every key written out)
(def meter (meter/make-meter {:jvm-metrics true :most-series-per-metric 5000} {:log log}))

(def send-invoice-email
  {:name         :billing/send-invoice-email
   :params-shape [:map [:order/id :uuid]]
   :run          (fn [{:keys [transaction background-job-id attempt log]} {:order/keys [id]}] …) ; log: the job's
   :timeout-ms   60000
   :attempts     5})

(def send-nightly-report
  {:name :reports/send-nightly-report :params-shape [:map] :run (fn [_ _] …) :timeout-ms 600000 :attempts 3})

(cellar/migrate! pool (into app-tables backroom/background-jobs-tables) changes migrate-options) ; cellar's migrate! options

(def workers (backroom/start-workers! pool {:wire                      wire
                                            :log                       log
                                            :meter                     meter
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
(backroom/add-background-job-to-queue! transaction send-invoice-email {:order/id order-id} {:wire wire :meter meter :run-in-ms 0 :key nil})

;; the order was deleted: no retry helps
(throw (backroom/make-give-up-exception "The order was deleted" {:order/id order-id}))
```

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
`cellar/grant-tables-to-role!` is given them.

## Tests

```sh
clojure -X:test     # the Postgres tests are skipped without a database

# with a throwaway Postgres:
initdb -D /tmp/backroom-pg -A trust -U "$USER"
pg_ctl -D /tmp/backroom-pg -o "-p 5499 -k /tmp -c listen_addresses=localhost" -l /tmp/backroom-pg.log start
BACKROOM_TEST_PORT=5499 BACKROOM_TEST_USER=$USER clojure -X:test
pg_ctl -D /tmp/backroom-pg stop
```

A background job that gives up logs its warning during the tests; that is
expected.

Why it is the way it is: [`decisions/`](decisions/README.md).

MIT license.
