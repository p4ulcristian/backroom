# backroom

Background jobs for Clojure on Postgres: work that runs outside a request.
**The queue:** a background job is a row, saved on a pool or in the
request's transaction (then it exists only if that commits). **The
workers:** each server claims due background jobs (`FOR UPDATE SKIP
LOCKED`, so no two run the same one), runs each in one transaction within
its time limit, retries a failure later and later (30 s up to an hour), and
lets one give up for good. **The schedules:** cron lines in UTC that queue
a background job at their times, on one server.

```clojure
io.github.p4ulcristian/backroom {:git/tag "v0.1.1" :git/sha "fcc5e94"}
```

Needs only [cellar](https://github.com/p4ulcristian/cellar) (Postgres, tables
as maps) and [wire](https://github.com/p4ulcristian/wire) (params kept with
their types). One public namespace, `backroom.api`; its part is
`backroom.cron`.

## The api

| function | does |
|---|---|
| `background-jobs-tables` `background-jobs-table` `background-job-schedules-table` | its two tables, `backroom.background_jobs` and `backroom.background_job_schedules`, as maps for cellar |
| `add-background-job-to-queue!` | saves a background job of a kind, its params checked by the kind's `:params-shape`; `:run-in-ms`, and `:key` for one at a time |
| `make-give-up-exception` | what a kind's `:run` throws to end its background job for good: dead at once, kept with the message |
| `start-workers!` `stop-workers!` `settings-shape` | the workers on this server (`:kinds` `:schedules` `:workers` `:poll-ms`); stopping waits for running ones (at most 30 s); the settings as a malli shape |
| `list-queued-background-jobs` `list-dead-background-jobs` | what waits, and what had all its attempts or gave up, with its last error |
| `run-background-job-again-now!` | queues a dead or waiting background job to run now, its attempts counted afresh |

```clojure
(require '[backroom.api :as backroom]
         '[cellar.api :as cellar])

(def send-invoice-email
  {:name         :billing/send-invoice-email
   :params-shape [:map [:order/id :uuid]]
   :run          (fn [{:keys [transaction background-job-id attempt]} {:order/keys [id]}] …)
   :timeout-ms   60000
   :attempts     5})

(def send-nightly-report
  {:name :reports/send-nightly-report :params-shape [:map] :run (fn [_ _] …) :timeout-ms 600000 :attempts 3})

(cellar/migrate! pool (into app-tables backroom/background-jobs-tables) changes)

(def workers (backroom/start-workers! pool {:kinds     [send-invoice-email send-nightly-report]
                                            :schedules [{:kind send-nightly-report :params {} :cron "0 1 * * *"}]
                                            :workers   4
                                            :poll-ms   1000}))

;; in the transaction that marks the order paid: no email for an order that wasn't
(backroom/add-background-job-to-queue! transaction send-invoice-email {:order/id order-id})

;; the order was deleted: no retry helps
(throw (backroom/make-give-up-exception "The order was deleted" {:order/id order-id}))
```

A background job can run twice (a server that stops mid-run leaves it
running; it is queued again once its time limit and a minute have passed),
so its work must be safe to repeat. Done background jobs are kept a week.
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
