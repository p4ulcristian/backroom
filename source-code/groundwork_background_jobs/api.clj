(ns groundwork-background-jobs.api
  "Work that runs outside a request, in Postgres: a queue, workers, and
   schedules. groundwork-background-jobs's one public namespace.

     ;; a background job kind: a map, like a step
     (def send-invoice-email
       {:name         :app.mail/send-invoice-email
        :system-actor :invoice-emails                         ; the named actor its transaction runs as
        :params-shape [:map [:order/id :int]]
        :run          (fn [background-job-context params] …)   ; background-job-context: :transaction :background-job-id :attempt :log
        :timeout-ms   60000
        :attempts     5})

     ;; the app's shaper, log and meter, made once by the app (groundwork-background-jobs makes none)
     (def shaper (groundwork_shaped_json.api/make-shaper-from-settings shaped-json-settings))
     (def log (groundwork_logs.api/log-function (groundwork_logs.api/make-logger logs-settings)))
     (def meter (groundwork_metrics.api/make-meter meter-settings {:log log}))

     ;; in a request's transaction: the background job exists only if that commits
     (background-jobs/add-background-job-to-queue! transaction send-invoice-email {:order/id 42}
                                            {:shaper groundwork-shaped-json :meter meter :run-in-ms 0 :key nil})

     ;; on each server that runs background jobs
     (background-jobs/start-workers! pool {:shaper                      groundwork-shaped-json
                                    :log                       log
                                    :meter                     meter
                                    ;; each job's one transaction, as its kind's actor (groundwork-access's, in groundwork)
                                    :run-in-transaction-as     (fn [pool actor log f] (access/run-as-system! pool actor {:read-only? false :log log} f))
                                    :kinds                     [send-invoice-email]
                                    :schedules                 [{:kind send-nightly-report :params {} :cron \"0 1 * * *\"}]
                                    :workers                   4
                                    :poll-ms                   1000
                                    :retry-first-wait-ms       30000
                                    :retry-wait-factor         2
                                    :retry-max-wait-ms         3600000
                                    :stuck-after-time-limit-ms 60000
                                    :forget-done-after-ms      604800000
                                    :forget-done-every-ms      3600000
                                    :stop-wait-ms              30000
                                    :metric-buckets            {:job-run-seconds [0.01 0.1 1 10 60 600]}})

   A background job is a row in groundwork_background_jobs.background_jobs; its tables are maps
   (background-jobs-tables) the app passes to groundwork-postgres with its own. Every :poll-ms the workers look for
   due background jobs; each free worker claims one (FOR UPDATE SKIP LOCKED, so no two
   servers run the same one) and runs its kind's :run in one transaction,
   within :timeout-ms: the transaction :run-in-transaction-as opens as the
   kind's :system-actor, in which the background job is also marked done,
   so the work and the mark commit together, or neither (a run past its
   time limit, already marked failed, marks nothing and its work is
   undone). A background job that throws or runs out of time runs again
   later, waiting longer each time (:retry-first-wait-ms, times
   :retry-wait-factor each time, up to :retry-max-wait-ms), until it has had
   its :attempts; then it is dead, kept with its last error. A background
   job that throws (make-give-up-exception …) is dead at once. A server that
   stops mid-background-job leaves it running; it is queued again once its
   time limit and :stuck-after-time-limit-ms have passed. A background job
   can run twice, so its work must be safe to repeat. Done background jobs
   older than :forget-done-after-ms are deleted when the workers start and
   every :forget-done-every-ms while they run, on every server at once
   (FOR UPDATE SKIP LOCKED: a row another server is deleting is skipped).
   Every setting and option is required, checked first (groundwork_background_jobs.settings).

   A schedule puts its kind's background job in the queue at each time its cron line
   (UTC) allows; one server does it, whichever claims the schedule first.

   groundwork-background-jobs writes to the app's log, :log, never anywhere else: each run
   gets a log with {:background-job/id … :background-job/kind …} as its
   context, in its context's :log, so its own lines carry the job; a
   failure that runs again later is :warn :groundwork-background-jobs/failed, one that ends
   the background job (its last attempt, or a give-up) :error
   :groundwork-background-jobs/gave-up, both with the exception or the time limit; each
   round of forgetting :groundwork-background-jobs/forgot-done (:info when it deleted any,
   :debug when none), or :warn :groundwork-background-jobs/forgetting-failed.

   start-workers! declares groundwork-background-jobs's metrics on the app's meter, :meter
   (groundwork_background_jobs.metrics): groundwork_background_jobs_jobs_added_total{kind},
   groundwork_background_jobs_job_runs_total{kind,outcome}, groundwork_background_jobs_job_run_seconds{kind},
   groundwork_background_jobs_jobs_running{kind}, groundwork_background_jobs_dispatcher_failures_total; the
   kinds' set is its :kinds. add-background-job-to-queue! takes the meter
   too, and counts the job it adds."
  (:require
   [clojure.string :as str]
   [groundwork-background-jobs.cron :as cron]
   [groundwork-background-jobs.metrics :as metrics]
   [groundwork-background-jobs.settings :as settings]
   [groundwork-postgres.api :as postgres]
   [groundwork-logs.api :as logs]
   [groundwork-shaped-json.api :as shaped-json])
  (:import
   (java.time Instant OffsetDateTime ZoneOffset)
   (java.util.concurrent ArrayBlockingQueue Semaphore TimeUnit)))

(defn- make-query [file-name]
  (postgres/make-query-from-sql-file (str "groundwork_background_jobs/" file-name)))

(def ^:private insert-background-job             (make-query "insert_background_job.sql"))
(def ^:private claim-next-background-job         (make-query "claim_next_background_job.sql"))
(def ^:private update-background-job-to-done     (make-query "update_background_job_to_done.sql"))
(def ^:private update-background-job-to-failed   (make-query "update_background_job_to_failed.sql"))
(def ^:private update-stuck-background-jobs      (make-query "update_stuck_background_jobs_to_queued.sql"))
(def ^:private delete-old-done-background-jobs   (make-query "delete_old_done_background_jobs.sql"))
(def ^:private insert-schedule        (make-query "insert_schedule.sql"))
(def ^:private find-due-schedules     (make-query "find_due_schedules.sql"))
(def ^:private update-schedule        (make-query "update_schedule_next_at.sql"))
(def ^:private find-background-jobs-in-state     (make-query "find_background_jobs_in_state.sql"))
(def ^:private update-background-job-to-run-again (make-query "update_background_job_to_run_again.sql"))

(defn- turn-instant-into-utc [^Instant instant]
  (OffsetDateTime/ofInstant instant ZoneOffset/UTC))

(defn- turn-kind-name-into-text [kind-name]
  (subs (str kind-name) 1))

;; -----------------------------------------------------------------------------
;; Tables, as maps for groundwork-postgres

(def settings-shape
  "The part of an app's settings that is groundwork-background-jobs's, every key required:
   what start-workers! takes besides :kinds and :schedules. See
   groundwork-background-jobs.settings/settings-shape for each key."
  settings/settings-shape)

(def background-jobs-table
  "The queue: one row per background job."
  {:table     :groundwork-background-jobs/background-jobs
   :columns   [[:background-job/id           :uuid         {:primary-key? true :default :random-uuid}]
               [:background-job/kind         :string]
               [:background-job/params       :string]      ; JSON text, through groundwork-shaped-json
               [:background-job/state        [:enum "queued" "running" "done" "dead"] {:default "queued"}]
               [:background-job/run-at       :time/instant {:default :now}]
               [:background-job/attempts     :int          {:default 0}]
               [:background-job/max-attempts :int]
               [:background-job/timeout-ms   :int]
               [:background-job/last-error   :string       {:optional? true}]
               [:background-job/locked-at    :time/instant {:optional? true}]
               [:background-job/created-at   :time/instant {:default :now}]
               [:background-job/finished-at  :time/instant {:optional? true}]
               [:background-job/key          :string       {:optional? true :unique? true}]]
   :indexes   [{:columns [[:background-job/state :ascending :nulls-last] [:background-job/run-at :ascending :nulls-last]]
                :method :btree :ops {} :unique? false :where nil}]
   :uniques   []
   :checks    []
   :references []
   :triggers   []
   :gates     nil
   :on-change nil})

(def background-job-schedules-table
  "The schedules: the next time each runs."
  {:table     :groundwork-background-jobs/background-job-schedules
   :columns   [[:background-job-schedule/key     :string       {:primary-key? true}]
               [:background-job-schedule/next-at :time/instant]]
   :indexes   []
   :uniques   []
   :checks    []
   :references []
   :triggers   []
   :gates     nil
   :on-change nil})

(def background-jobs-tables
  "Both tables, to pass to groundwork-postgres with the app's own."
  [background-jobs-table background-job-schedules-table])

;; -----------------------------------------------------------------------------
;; Queueing

(defn- turn-params-into-json-text
  "A background job's params as JSON text by its kind's :params-shape,
   through the app's shaper; params that don't fit it, or carry a key it
   doesn't name, throw."
  [shaper kind-name params-shape params]
  (let [{:keys [json-data errors]} (shaped-json/turn-shaped-data-into-json-data-or-errors shaper params-shape params)]
    (when errors
      (throw (ex-info (str "Background job " kind-name "'s params don't fit its shape")
                      {:type   :groundwork-background-jobs/bad-params
                       :kind   kind-name
                       :errors errors})))
    (shaped-json/turn-data-into-json-text shaper json-data)))

(defn add-background-job-to-queue!
  "Saves a background job of a kind, on a pool or in a transaction (then it
   exists only if that commits). Its params must fit the kind's
   :params-shape. Options, every key required: :shaper, the app's shaper,
   which the params are kept by; :meter, the app's meter, which the job
   added is counted on (groundwork_background_jobs_jobs_added_total, declared by
   start-workers!); :run-in-ms, run no sooner (0 for now); :key, nil, or
   one background job of this key for as long as it is remembered: a
   second one queues nothing, isn't counted, and returns the first one's
   id. Returns the background job's id."
  [pool-or-transaction kind params options]
  (settings/check-kind kind)
  (settings/check-queue-options options)
  (let [{:keys [name params-shape timeout-ms attempts]} kind
        {:keys [shaper meter run-in-ms key]}              options
        {:keys [added] :as row} (postgres/find-one-row pool-or-transaction insert-background-job
                                                     {:background-job/kind         (turn-kind-name-into-text name)
                                                      :background-job/params       (turn-params-into-json-text shaper name params-shape params)
                                                      :background-job/run-at       (turn-instant-into-utc (.plusMillis (Instant/now) (long run-in-ms)))
                                                      :background-job/max-attempts attempts
                                                      :background-job/timeout-ms   timeout-ms
                                                      :background-job/key          key})]
    (when added (metrics/count-added! meter name))
    (:background-job/id row)))

(defn make-give-up-exception
  "The exception a background job's :run throws to end the background job
   for good, when no retry can fix it (the order it was for is gone). The
   background job is dead at once, kept with this message. The data is
   written out, {} for none.

     (throw (background-jobs/make-give-up-exception \"The order was deleted\" {:order/id id}))"
  [message data]
  (ex-info message (assoc data :type :groundwork-background-jobs/give-up)))

(defn- does-failure-give-up? [throwable]
  (boolean (some #(= :groundwork-background-jobs/give-up (:type (ex-data %)))
                 (take-while some? (iterate ex-cause throwable)))))

;; -----------------------------------------------------------------------------
;; Running one background job

(defn- find-wait-before-retry-ms
  "The wait after a background job's attempt-th run failed: the first wait,
   times the factor for each failure before, never past the max."
  [{:keys [retry-first-wait-ms retry-wait-factor retry-max-wait-ms]} attempt]
  (long (min retry-max-wait-ms (* retry-first-wait-ms (Math/pow retry-wait-factor (dec attempt))))))

(defn- mark-done-in-run!
  "Marks the background job done in its run's own transaction; throws,
   rolling the work back, when this run no longer holds it (marked failed
   past its time limit, or queued again)."
  [transaction id attempts]
  (when (zero? (postgres/run-statement! transaction update-background-job-to-done
                                      {:background-job/id id :background-job/attempts attempts}))
    (throw (ex-info "The background job was no longer this run's to mark done: its work is undone"
                    {:type :groundwork-background-jobs/run-overtaken :background-job/id id :background-job/attempt attempts}))))

(defn- run-background-job-and-catch
  "Runs the background job's kind, and marks it done, in one transaction
   that run-in-transaction-as opens as the kind's :system-actor, with the
   background job's log, which the transaction carries (a :run's work kept
   for after the commit that fails is written there). Returns :ok or what
   was thrown."
  [shaper pool kinds-by-name run-in-transaction-as log {:background-job/keys [id kind params attempts]}]
  (try
    (let [{:keys [run params-shape system-actor] :as background-job-kind} (get kinds-by-name kind)]
      (when-not background-job-kind
        (throw (ex-info (str "No background job kind " kind " on this server")
                        {:type :groundwork-background-jobs/unknown-kind :kind kind})))
      (let [{:keys [shaped-data errors]} (shaped-json/turn-json-data-into-shaped-data-or-errors
                                          shaper params-shape (shaped-json/turn-json-text-into-data shaper params))]
        (when errors
          (throw (ex-info "The background job's params don't fit its shape" {:errors errors})))
        (run-in-transaction-as pool system-actor log
                               (fn [transaction]
                                 (run {:transaction transaction :background-job-id id :attempt attempts :log log} shaped-data)
                                 (mark-done-in-run! transaction id attempts)))
        :ok))
    (catch Throwable throwable throwable)))

(defn- log-failure!
  "A failure that runs again later is a warning; one that left the background
   job dead (its last attempt, or a give-up) an error, kept for pulling."
  [log state attempts retry-ms result]
  (let [failure (if (= :timed-out result) {:background-job/ran-out-of-time? true} {:error result})]
    (if (= "dead" state)
      (log :error :groundwork-background-jobs/gave-up (assoc failure :background-job/attempts attempts))
      (log :warn :groundwork-background-jobs/failed (assoc failure :background-job/attempt attempts :background-job/retry-in-ms retry-ms)))))

(defn- run-claimed-background-job!
  "Runs a claimed background job within its time limit and saves what
   happened; log is the background job's. Records the run's time; returns
   how it ended: :done, :failed or :gave-up (groundwork-background-jobs.metrics/outcomes)."
  [{:keys [shaper pool kinds-by-name run-in-transaction-as meter] :as running} log {:background-job/keys [id kind attempts timeout-ms] :as background-job}]
  (let [outcome (promise)
        started (System/nanoTime)
        thread  (Thread/startVirtualThread #(deliver outcome (run-background-job-and-catch shaper pool kinds-by-name run-in-transaction-as
                                                                                            log background-job)))
        result  (deref outcome timeout-ms :timed-out)]
    (metrics/record-run-time! meter kind started)
    (when (= :timed-out result) (.interrupt thread))
    (if (= :ok result)
      :done                             ; marked in the run's own transaction
      (let [retry-ms (find-wait-before-retry-ms running attempts)
            state    (:background-job/state
                      (postgres/find-one-row pool update-background-job-to-failed
                                           {:background-job/id         id
                                            :background-job/retry-ms   retry-ms
                                            :background-job/for-good   (and (instance? Throwable result) (does-failure-give-up? result))
                                            :background-job/last-error (if (= :timed-out result)
                                                                         (str "Ran longer than " timeout-ms " ms")
                                                                         (str (.getName (class result)) ": " (ex-message result)))}))]
        (log-failure! log state attempts retry-ms result)
        (if (= "dead" state) :gave-up :failed)))))

;; -----------------------------------------------------------------------------
;; The workers

(defn- make-schedule-key [shaper {:keys [kind params cron]}]
  (str (turn-kind-name-into-text (:name kind)) " " cron " "
       (turn-params-into-json-text shaper (:name kind) (:params-shape kind) params)))

(defn- add-due-scheduled-background-jobs!
  "Queues the background job of every due schedule this server claims, and moves the
   schedule to its next time."
  [shaper meter pool schedules-by-key]
  ;; groundwork-background-jobs's own, never handed to a kind's :run: keeps no work
  (postgres/run-in-transaction! pool {:read-only? false :log nil}
    (fn [transaction]
      (doseq [{key :background-job-schedule/key} (postgres/find-rows transaction find-due-schedules {})
              :let [{:keys [kind params cron]} (get schedules-by-key key)]
              :when kind]
        (add-background-job-to-queue! transaction kind params {:shaper shaper :meter meter :run-in-ms 0 :key nil})
        (postgres/run-statement! transaction update-schedule
                               {:background-job-schedule/key     key
                                :background-job-schedule/next-at (turn-instant-into-utc
                                                                  (cron/find-next-time-for-cron-line cron (Instant/now)))})))))

(defn- run-dispatcher!
  "Until stopped: queue due schedules, queue stuck background jobs again, and give
   every free worker a due background job; then wait :poll-ms, or less when a worker
   finishes or the workers stop."
  [{:keys [shaper log meter pool stopped ^Semaphore free-workers ^ArrayBlockingQueue wake schedules-by-key poll-ms stuck-after-time-limit-ms]
    :as   running}]
  (while (not @stopped)
    (try
      (add-due-scheduled-background-jobs! shaper meter pool schedules-by-key)
      (postgres/run-statement! pool update-stuck-background-jobs {:background-job/stuck-after-time-limit-ms stuck-after-time-limit-ms})
      (loop []
        (when (and (not @stopped) (.tryAcquire free-workers))
          (if-let [background-job (some-> (postgres/find-one-row pool claim-next-background-job {})
                                          (update :background-job/kind keyword))]
            (do (Thread/startVirtualThread
                 (fn []
                   (let [kind    (:background-job/kind background-job)
                         job-log (logs/with-context log {:background-job/id   (:background-job/id background-job)
                                                            :background-job/kind kind})]
                     (metrics/add-to-running! meter kind 1)
                     (try (metrics/count-run! meter kind (run-claimed-background-job! running job-log background-job))
                          (catch Throwable e
                            (metrics/count-run! meter kind :outcome-not-saved)
                            (job-log :error :groundwork-background-jobs/outcome-not-saved {:error e}))
                          (finally
                            (metrics/add-to-running! meter kind -1)
                            (.release free-workers)
                            (.offer wake :worker-free))))))
                (recur))
            (.release free-workers))))
      (catch Throwable e
        (when-not @stopped
          (metrics/count-dispatcher-failure! meter)
          (log :warn :groundwork-background-jobs/dispatcher-failed {:error e}))))
    (.poll wake (long poll-ms) TimeUnit/MILLISECONDS)))

(defn- run-forgetter!
  "Deletes the done background jobs older than :forget-done-after-ms, then
   waits :forget-done-every-ms, or less when the workers stop; again until
   they stop. Its own thread, so a long delete holds up no claim."
  [{:keys [log pool stopped ^ArrayBlockingQueue forget-wake forget-done-after-ms forget-done-every-ms]}]
  ;; the first round always runs, even when the workers stop at once
  (loop []
    (try
      (let [forgotten (postgres/run-statement! pool delete-old-done-background-jobs
                                             {:background-job/forget-done-after-ms forget-done-after-ms})]
        (log (if (pos? forgotten) :info :debug) :groundwork-background-jobs/forgot-done
             {:background-job/forgotten            forgotten
              :background-job/forget-done-after-ms forget-done-after-ms}))
      (catch Throwable e
        (log :warn :groundwork-background-jobs/forgetting-failed {:error e})))
    (.poll forget-wake (long forget-done-every-ms) TimeUnit/MILLISECONDS)
    (when-not @stopped (recur))))

(defn start-workers!
  "Starts the workers on this server. Options, every key required, checked
   first (groundwork-background-jobs.settings/workers-options-shape):

     :shaper      the app's shaper           every background job's params are read by it
     :log       the app's log            what goes wrong is written to it; each run gets
                                         one with its background job's id and kind
     :meter     the app's meter          groundwork-background-jobs's metrics are declared on it and counted
                                         into (groundwork-background-jobs.metrics), the kinds as their set
     :run-in-transaction-as (fn [pool system-actor log f]), the app's: opens each
                                         job's one transaction as its kind's :system-actor,
                                         carrying log, the job's (groundwork-postgres's :log; groundwork-access's
                                         run-as-system!, in groundwork)
     :kinds     [background job kinds]   every kind this server runs
     :schedules [{:kind a-background-job-kind :params {…} :cron \"0 1 * * *\"}]
     and the settings (settings-shape): :workers :poll-ms
     :retry-first-wait-ms :retry-wait-factor :retry-max-wait-ms
     :stuck-after-time-limit-ms :forget-done-after-ms :forget-done-every-ms
     :stop-wait-ms :metric-buckets

   Forgets done background jobs older than :forget-done-after-ms now, and
   every :forget-done-every-ms while the workers run. Returns the running
   workers."
  [pool options]
  (settings/check-workers-options options)
  (let [{:keys [shaper log meter kinds schedules workers]} options
        names (map :name kinds)]
    ;; a log groundwork-logs didn't make can't carry a job's context: refused now, not at the first run
    (logs/with-context log {})
    (when (not= (count names) (count (set names)))
      (throw (ex-info (str "Background job kinds named twice: " (str/join " " names))
                      {:type :groundwork-background-jobs/kind-named-twice})))
    ;; and a meter meter didn't make is refused here too (:groundwork-metrics/not-a-meter)
    (metrics/declare-metrics! meter options)
    (doseq [{:keys [cron]} schedules]
      (cron/turn-cron-line-into-allowed-values cron))
    (let [schedules-by-key (into {} (map (juxt #(make-schedule-key shaper %) identity)) schedules)
          running          (merge (select-keys options [:shaper :log :meter :run-in-transaction-as :workers :poll-ms :retry-first-wait-ms :retry-wait-factor
                                                        :retry-max-wait-ms :stuck-after-time-limit-ms :forget-done-after-ms
                                                        :forget-done-every-ms :stop-wait-ms])
                                  {:pool             pool
                                   :kinds-by-name    (into {} (map (juxt :name identity)) kinds)
                                   :schedules-by-key schedules-by-key
                                   :stopped          (atom false)
                                   :free-workers     (Semaphore. (int workers))
                                   ;; one wake-up is enough: the dispatcher looks at everything when it wakes
                                   :wake             (ArrayBlockingQueue. 1)
                                   :forget-wake      (ArrayBlockingQueue. 1)})]
      (doseq [[key {:keys [cron]}] schedules-by-key]
        (postgres/run-statement! pool insert-schedule
                               {:background-job-schedule/key     key
                                :background-job-schedule/next-at (turn-instant-into-utc
                                                                  (cron/find-next-time-for-cron-line cron (Instant/now)))}))
      (assoc running
             :dispatcher (Thread/startVirtualThread #(run-dispatcher! running))
             :forgetter  (Thread/startVirtualThread #(run-forgetter! running))))))

(defn stop-workers!
  "Stops taking background jobs and forgetting done ones, and waits for the
   dispatcher, the forgetting and the running background jobs to finish, at
   most :stop-wait-ms in all. Returns whether they had all finished; one
   that hadn't goes on in the background, and is queued again later if its
   server stops."
  [{:keys [stopped ^Thread dispatcher ^Thread forgetter ^Semaphore free-workers ^ArrayBlockingQueue wake
           ^ArrayBlockingQueue forget-wake workers stop-wait-ms]}]
  (reset! stopped true)
  (.offer wake :stop)
  (.offer forget-wake :stop)
  (let [deadline (+ (System/nanoTime) (* 1000000 (long stop-wait-ms)))
        left-ms  #(max 0 (quot (- deadline (System/nanoTime)) 1000000))]
    (and (.join dispatcher (java.time.Duration/ofMillis (left-ms)))
         (.join forgetter (java.time.Duration/ofMillis (left-ms)))
         (if (.tryAcquire free-workers (int workers) (long (left-ms)) TimeUnit/MILLISECONDS)
           (do (.release free-workers (int workers)) true)
           false))))

;; -----------------------------------------------------------------------------
;; Looking at the queue

(defn list-queued-background-jobs
  "Background jobs waiting, newest first, at most limit."
  [pool limit]
  (postgres/find-rows pool find-background-jobs-in-state {:background-job/state "queued" :limit limit}))

(defn list-dead-background-jobs
  "Background jobs that had all their attempts or gave up, with their last error,
   newest first, at most limit."
  [pool limit]
  (postgres/find-rows pool find-background-jobs-in-state {:background-job/state "dead" :limit limit}))

(defn run-background-job-again-now!
  "Queues a dead or waiting background job to run now, its attempts counted afresh.
   Whether there was such a background job."
  [pool background-job-id]
  (some? (postgres/find-one-row pool update-background-job-to-run-again {:background-job/id background-job-id})))
