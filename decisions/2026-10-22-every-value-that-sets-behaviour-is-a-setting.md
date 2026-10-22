# Every value that sets behaviour is a setting; no hidden defaults

> **Changed in part** by [Done background jobs are forgotten while the workers run](2026-10-23-done-background-jobs-are-forgotten-while-the-workers-run.md) (2026-10-23, v0.4.0): a ninth setting, `:forget-done-every-ms`; done background jobs are deleted every that long while the workers run, not only when they start.

> **Changed in part** by [The app passes its log](2026-10-22-the-app-passes-its-log.md) (2026-10-22, v0.3.0): backroom writes to the app's log, `:log` in `start-workers!`'s options, not to `System.Logger` `backroom`.

> **Changed in part** by [The app passes its wire; backroom never makes one](2026-10-22-the-app-passes-its-wire.md) (2026-10-22, v0.2.0): `add-background-job-to-queue!` and `start-workers!` take the app's wire, `:wire`, in their options; the checks are malli's own.

> **Changed in part** by cellar's [A column says only the options it has](https://github.com/p4ulcristian/cellar/blob/main/decisions/2026-10-22-a-column-says-only-the-options-it-has.md) (2026-10-22, v0.1.3, with cellar v0.1.3): backroom's columns say only the options they have; its tables still write out `:indexes`, `:gates` and `:on-change`.

> Changes in part [A job kind is a map, and the library makes its own tables](2026-10-22-a-job-kind-is-a-map-and-the-library-makes-its-own-tables.md) (the retry waits, the stuck job's minute and the week done jobs are kept were fixed) and [A job can give up for good](2026-10-12-a-job-can-give-up-for-good.md) (the give-up exception takes its data always). Since v0.1.2.

**Decision:** every value that sets how backroom behaves is written out by the app: a setting, a required option or a required key, checked by malli before anything runs, the maps closed, a missing or unknown key refused, named, with every other problem at once (`:backroom/bad-options`, `:backroom/bad-kind`). No function fills in a value: there is no shorter arity, no `:or`, no optional key. What stays fixed is listed below, each with its reason.

- **The settings**, `settings-shape`, every key required:

  ```clojure
  [:map {:closed true}
   [:workers [:int {:min 1}]]
   [:poll-ms [:int {:min 1}]]
   [:retry-first-wait-ms [:int {:min 0}]]          ; was 30000
   [:retry-wait-factor [:and number? [:>= 1]]]     ; was 2
   [:retry-max-wait-ms [:int {:min 0}]]            ; was 3600000
   [:stuck-after-time-limit-ms [:int {:min 0}]]    ; was 60000
   [:forget-done-after-ms [:int {:min 0}]]         ; was 7 days
   [:stop-wait-ms [:int {:min 0}]]]               ; was 5 s for the dispatcher, then 30 s for the jobs
  ```

  `start-workers!` takes them with `:kinds` and `:schedules`, one closed map. `stop-workers!` had two fixed waits, 5 s for the dispatcher and 30 s more for the running background jobs; they are one deadline, `:stop-wait-ms`, and `stop-workers!` returns whether everything had finished.

- **A kind writes out every key**: `{:name :params-shape :run :timeout-ms :attempts}`, closed, checked when a background job is queued and when the workers start.
- **A schedule writes out every key**: `{:kind :params :cron}`, closed.
- **No arity fills in a value**: `add-background-job-to-queue!` takes its options always, `{:run-in-ms 0 :key nil}` for a background job now with no key; `make-give-up-exception` takes its data always, `{}` for none.
- **backroom's tables write out every key** cellar v0.1.2 reads: each column's five options, `:indexes`, `:gates` and `:on-change`.

**What stays fixed, and why:**

- **Cron lines are in UTC**: UTC has no daylight saving time. In a zone with it, a line at 02:30 would be skipped one night a year and run twice another; a time zone is a design change (what a skipped or doubled time does), not a setting.
- **The search for a cron line's next time gives up after 600000 steps** and says the line never comes: a bound on the search, years past any line that can come (29 February on a Monday), not a choice an app makes.
- **A background job's run is interrupted when it runs past its `:timeout-ms`**, and counts as failed: the time limit is the kind's, and an interrupted run that didn't stop holds no worker.
- **Claimed in order of `run_at`, one at a time per free worker, with `FOR UPDATE SKIP LOCKED`**: no two servers run the same background job ([A job kind is a map, and the library makes its own tables](2026-10-22-a-job-kind-is-a-map-and-the-library-makes-its-own-tables.md)).
- **The dispatcher wakes at most once more per wake-up** (a queue of one): when it wakes it looks at everything due, so a second wake-up would find nothing new.
- **Each run is one transaction, read-write**: a kind's work is kept or undone whole.
- **One log, `System.Logger` named `backroom`**.

**Why:** a value left to a default is a value the app doesn't show. An hour's cap on retries, a minute before a stuck background job is taken again, a week of done background jobs in the table, 35 s that a stopping server waits: each is behaviour someone feels (a payment retried too late, a deploy that waits), and each was invisible in the app's settings.

**The price:** the settings grow from 2 keys to 8; every call of `add-background-job-to-queue!` writes its options, every give-up its data. An app moving from v0.1.1 changes its settings, its calls of those two, and takes cellar v0.1.2 (its own table maps and pool settings change with it).

**Instead of:**
- defaults the app may override: a good value without thought, but the value in effect is invisible
- the retry waits per kind: a finer choice no kind has asked for yet; the settings say it once for every kind, and a per-kind wait can come later as a kind's key
- two stop waits: the app cares how long a stopping server takes in all, not how that splits
- cron lines in a time zone the app chooses: the daylight-saving hour would skip or double a run, silently
