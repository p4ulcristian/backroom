(ns backroom.api
  "Work that runs outside a request, in Postgres: a queue, workers, and
   schedules. backroom's one public namespace.

     ;; a background job kind: a map, like a step
     (def send-invoice-email
       {:name         :app.mail/send-invoice-email
        :system-actor :invoice-emails                         ; the named actor its transaction runs as
        :params-shape [:map [:order/id :int]]
        :run          (fn [background-job-context params] …)   ; background-job-context: :transaction :background-job-id :attempt :log
        :timeout-ms   60000
        :attempts     5})

     ;; the app's wire, log and meter, made once by the app (backroom makes none)
     (def wire (wire.api/make-wire-from-settings wire-settings))
     (def log (logbook.api/log-function (logbook.api/make-logbook logbook-settings)))
     (def meter (meter.api/make-meter meter-settings {:log log}))

     ;; in a request's transaction: the background job exists only if that commits
     (backroom/add-background-job-to-queue! transaction send-invoice-email {:order/id 42}
                                            {:wire wire :meter meter :run-in-ms 0 :key nil})

     ;; on each server that runs background jobs
     (backroom/start-workers! pool {:wire                      wire
                                    :log                       log
                                    :meter                     meter
                                    ;; each job's one transaction, as its kind's actor (doorman's, in groundwork)
                                    :run-in-transaction-as     (fn [pool actor f] (doorman/run-as-system! pool actor {:read-only? false} f))
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

   A background job is a row in backroom.background_jobs; its tables are maps
   (background-jobs-tables) the app passes to cellar with its own. Every :poll-ms the workers look for
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
   Every setting and option is required, checked first (backroom.settings).

   A schedule puts its kind's background job in the queue at each time its cron line
   (UTC) allows; one server does it, whichever claims the schedule first.

   backroom writes to the app's log, :log, never anywhere else: each run
   gets a log with {:background-job/id … :background-job/kind …} as its
   context, in its context's :log, so its own lines carry the job; a
   failure that runs again later is :warn :backroom/failed, one that ends
   the background job (its last attempt, or a give-up) :error
   :backroom/gave-up, both with the exception or the time limit; each
   round of forgetting :backroom/forgot-done (:info when it deleted any,
   :debug when none), or :warn :backroom/forgetting-failed.

   start-workers! declares backroom's metrics on the app's meter, :meter
   (backroom.metrics): backroom_jobs_added_total{kind},
   backroom_job_runs_total{kind,outcome}, backroom_job_run_seconds{kind},
   backroom_jobs_running{kind}, backroom_dispatcher_failures_total; the
   kinds' set is its :kinds. add-background-job-to-queue! takes the meter
   too, and counts the job it adds."
  (:require
   [clojure.string :as str]
   [backroom.cron :as cron]
   [backroom.metrics :as metrics]
   [backroom.settings :as settings]
   [cellar.api :as cellar]
   [logbook.api :as logbook]
   [wire.api :as wire])
  (:import
   (java.time Instant OffsetDateTime ZoneOffset)
   (java.util.concurrent ArrayBlockingQueue Semaphore TimeUnit)))

(defn- make-query [file-name]
  (cellar/make-query-from-sql-file (str "backroom/" file-name)))

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
;; Tables, as maps for cellar

(def settings-shape
  "The part of an app's settings that is backroom's, every key required:
   what start-workers! takes besides :kinds and :schedules. See
   backroom.settings/settings-shape for each key."
  settings/settings-shape)

(def background-jobs-table
  "The queue: one row per background job."
  {:table     :backroom/background-jobs
   :columns   [[:background-job/id           :uuid         {:primary-key? true :default :random-uuid}]
               [:background-job/kind         :string]
               [:background-job/params       :string]      ; JSON text, through wire
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
  {:table     :backroom/background-job-schedules
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
  "Both tables, to pass to cellar with the app's own."
  [background-jobs-table background-job-schedules-table])

;; -----------------------------------------------------------------------------
;; Queueing

(defn- turn-params-into-json-text
  "A background job's params as JSON text by its kind's :params-shape,
   through the app's wire; params that don't fit it, or carry a key it
   doesn't name, throw."
  [wire kind-name params-shape params]
  (let [{:keys [json-data errors]} (wire/turn-shaped-data-into-json-data-or-errors wire params-shape params)]
    (when errors
      (throw (ex-info (str "Background job " kind-name "'s params don't fit its shape")
                      {:type   :backroom/bad-params
                       :kind   kind-name
                       :errors errors})))
    (wire/turn-data-into-json-text wire json-data)))

(defn add-background-job-to-queue!
  "Saves a background job of a kind, on a pool or in a transaction (then it
   exists only if that commits). Its params must fit the kind's
   :params-shape. Options, every key required: :wire, the app's wire,
   which the params are kept by; :meter, the app's meter, which the job
   added is counted on (backroom_jobs_added_total, declared by
   start-workers!); :run-in-ms, run no sooner (0 for now); :key, nil, or
   one background job of this key for as long as it is remembered: a
   second one queues nothing, isn't counted, and returns the first one's
   id. Returns the background job's id."
  [pool-or-transaction kind params options]
  (settings/check-kind kind)
  (settings/check-queue-options options)
  (let [{:keys [name params-shape timeout-ms attempts]} kind
        {:keys [wire meter run-in-ms key]}              options
        {:keys [added] :as row} (cellar/find-one-row pool-or-transaction insert-background-job
                                                     {:background-job/kind         (turn-kind-name-into-text name)
                                                      :background-job/params       (turn-params-into-json-text wire name params-shape params)
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

     (throw (backroom/make-give-up-exception \"The order was deleted\" {:order/id id}))"
  [message data]
  (ex-info message (assoc data :type :backroom/give-up)))

(defn- does-failure-give-up? [throwable]
  (boolean (some #(= :backroom/give-up (:type (ex-data %)))
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
  (when (zero? (cellar/run-statement! transaction update-background-job-to-done
                                      {:background-job/id id :background-job/attempts attempts}))
    (throw (ex-info "The background job was no longer this run's to mark done: its work is undone"
                    {:type :backroom/run-overtaken :background-job/id id :background-job/attempt attempts}))))

(defn- run-background-job-and-catch
  "Runs the background job's kind, and marks it done, in one transaction
   that run-in-transaction-as opens as the kind's :system-actor, with the
   background job's log. Returns :ok or what was thrown."
  [wire pool kinds-by-name run-in-transaction-as log {:background-job/keys [id kind params attempts]}]
  (try
    (let [{:keys [run params-shape system-actor] :as background-job-kind} (get kinds-by-name kind)]
      (when-not background-job-kind
        (throw (ex-info (str "No background job kind " kind " on this server")
                        {:type :backroom/unknown-kind :kind kind})))
      (let [{:keys [shaped-data errors]} (wire/turn-json-data-into-shaped-data-or-errors
                                          wire params-shape (wire/turn-json-text-into-data wire params))]
        (when errors
          (throw (ex-info "The background job's params don't fit its shape" {:errors errors})))
        (run-in-transaction-as pool system-actor
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
      (log :error :backroom/gave-up (assoc failure :background-job/attempts attempts))
      (log :warn :backroom/failed (assoc failure :background-job/attempt attempts :background-job/retry-in-ms retry-ms)))))

(defn- run-claimed-background-job!
  "Runs a claimed background job within its time limit and saves what
   happened; log is the background job's. Records the run's time; returns
   how it ended: :done, :failed or :gave-up (backroom.metrics/outcomes)."
  [{:keys [wire pool kinds-by-name run-in-transaction-as meter] :as running} log {:background-job/keys [id kind attempts timeout-ms] :as background-job}]
  (let [outcome (promise)
        started (System/nanoTime)
        thread  (Thread/startVirtualThread #(deliver outcome (run-background-job-and-catch wire pool kinds-by-name run-in-transaction-as
                                                                                            log background-job)))
        result  (deref outcome timeout-ms :timed-out)]
    (metrics/record-run-time! meter kind started)
    (when (= :timed-out result) (.interrupt thread))
    (if (= :ok result)
      :done                             ; marked in the run's own transaction
      (let [retry-ms (find-wait-before-retry-ms running attempts)
            state    (:background-job/state
                      (cellar/find-one-row pool update-background-job-to-failed
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

(defn- make-schedule-key [wire {:keys [kind params cron]}]
  (str (turn-kind-name-into-text (:name kind)) " " cron " "
       (turn-params-into-json-text wire (:name kind) (:params-shape kind) params)))

(defn- add-due-scheduled-background-jobs!
  "Queues the background job of every due schedule this server claims, and moves the
   schedule to its next time."
  [wire meter pool schedules-by-key]
  (cellar/run-in-transaction! pool {:read-only? false}
    (fn [transaction]
      (doseq [{key :background-job-schedule/key} (cellar/find-rows transaction find-due-schedules {})
              :let [{:keys [kind params cron]} (get schedules-by-key key)]
              :when kind]
        (add-background-job-to-queue! transaction kind params {:wire wire :meter meter :run-in-ms 0 :key nil})
        (cellar/run-statement! transaction update-schedule
                               {:background-job-schedule/key     key
                                :background-job-schedule/next-at (turn-instant-into-utc
                                                                  (cron/find-next-time-for-cron-line cron (Instant/now)))})))))

(defn- run-dispatcher!
  "Until stopped: queue due schedules, queue stuck background jobs again, and give
   every free worker a due background job; then wait :poll-ms, or less when a worker
   finishes or the workers stop."
  [{:keys [wire log meter pool stopped ^Semaphore free-workers ^ArrayBlockingQueue wake schedules-by-key poll-ms stuck-after-time-limit-ms]
    :as   running}]
  (while (not @stopped)
    (try
      (add-due-scheduled-background-jobs! wire meter pool schedules-by-key)
      (cellar/run-statement! pool update-stuck-background-jobs {:background-job/stuck-after-time-limit-ms stuck-after-time-limit-ms})
      (loop []
        (when (and (not @stopped) (.tryAcquire free-workers))
          (if-let [background-job (some-> (cellar/find-one-row pool claim-next-background-job {})
                                          (update :background-job/kind keyword))]
            (do (Thread/startVirtualThread
                 (fn []
                   (let [kind    (:background-job/kind background-job)
                         job-log (logbook/with-context log {:background-job/id   (:background-job/id background-job)
                                                            :background-job/kind kind})]
                     (metrics/add-to-running! meter kind 1)
                     (try (metrics/count-run! meter kind (run-claimed-background-job! running job-log background-job))
                          (catch Throwable e
                            (metrics/count-run! meter kind :outcome-not-saved)
                            (job-log :error :backroom/outcome-not-saved {:error e}))
                          (finally
                            (metrics/add-to-running! meter kind -1)
                            (.release free-workers)
                            (.offer wake :worker-free))))))
                (recur))
            (.release free-workers))))
      (catch Throwable e
        (when-not @stopped
          (metrics/count-dispatcher-failure! meter)
          (log :warn :backroom/dispatcher-failed {:error e}))))
    (.poll wake (long poll-ms) TimeUnit/MILLISECONDS)))

(defn- run-forgetter!
  "Deletes the done background jobs older than :forget-done-after-ms, then
   waits :forget-done-every-ms, or less when the workers stop; again until
   they stop. Its own thread, so a long delete holds up no claim."
  [{:keys [log pool stopped ^ArrayBlockingQueue forget-wake forget-done-after-ms forget-done-every-ms]}]
  ;; the first round always runs, even when the workers stop at once
  (loop []
    (try
      (let [forgotten (cellar/run-statement! pool delete-old-done-background-jobs
                                             {:background-job/forget-done-after-ms forget-done-after-ms})]
        (log (if (pos? forgotten) :info :debug) :backroom/forgot-done
             {:background-job/forgotten            forgotten
              :background-job/forget-done-after-ms forget-done-after-ms}))
      (catch Throwable e
        (log :warn :backroom/forgetting-failed {:error e})))
    (.poll forget-wake (long forget-done-every-ms) TimeUnit/MILLISECONDS)
    (when-not @stopped (recur))))

(defn start-workers!
  "Starts the workers on this server. Options, every key required, checked
   first (backroom.settings/workers-options-shape):

     :wire      the app's wire           every background job's params are read by it
     :log       the app's log            what goes wrong is written to it; each run gets
                                         one with its background job's id and kind
     :meter     the app's meter          backroom's metrics are declared on it and counted
                                         into (backroom.metrics), the kinds as their set
     :run-in-transaction-as (fn [pool system-actor f]), the app's: opens each
                                         job's one transaction as its kind's :system-actor
                                         (doorman's run-as-system!, in groundwork)
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
  (let [{:keys [wire log meter kinds schedules workers]} options
        names (map :name kinds)]
    ;; a log logbook didn't make can't carry a job's context: refused now, not at the first run
    (logbook/with-context log {})
    (when (not= (count names) (count (set names)))
      (throw (ex-info (str "Background job kinds named twice: " (str/join " " names))
                      {:type :backroom/kind-named-twice})))
    ;; and a meter meter didn't make is refused here too (:meter/not-a-meter)
    (metrics/declare-metrics! meter options)
    (doseq [{:keys [cron]} schedules]
      (cron/turn-cron-line-into-allowed-values cron))
    (let [schedules-by-key (into {} (map (juxt #(make-schedule-key wire %) identity)) schedules)
          running          (merge (select-keys options [:wire :log :meter :run-in-transaction-as :workers :poll-ms :retry-first-wait-ms :retry-wait-factor
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
        (cellar/run-statement! pool insert-schedule
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
  (cellar/find-rows pool find-background-jobs-in-state {:background-job/state "queued" :limit limit}))

(defn list-dead-background-jobs
  "Background jobs that had all their attempts or gave up, with their last error,
   newest first, at most limit."
  [pool limit]
  (cellar/find-rows pool find-background-jobs-in-state {:background-job/state "dead" :limit limit}))

(defn run-background-job-again-now!
  "Queues a dead or waiting background job to run now, its attempts counted afresh.
   Whether there was such a background job."
  [pool background-job-id]
  (some? (cellar/find-one-row pool update-background-job-to-run-again {:background-job/id background-job-id})))
