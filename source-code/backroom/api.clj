(ns backroom.api
  "Work that runs outside a request, in Postgres: a queue, workers, and
   schedules. backroom's one public namespace.

     ;; a background job kind: a map, like a step
     (def send-invoice-email
       {:name         :app.mail/send-invoice-email
        :params-shape [:map [:order/id :int]]
        :run          (fn [background-job-context params] …)   ; background-job-context: :transaction :background-job-id :attempt
        :timeout-ms   60000
        :attempts     5})

     ;; in a request's transaction: the background job exists only if that commits
     (backroom/add-background-job-to-queue! transaction send-invoice-email {:order/id 42} {:run-in-ms 0 :key nil})

     ;; on each server that runs background jobs
     (backroom/start-workers! pool {:kinds                     [send-invoice-email]
                                    :schedules                 [{:kind send-nightly-report :params {} :cron \"0 1 * * *\"}]
                                    :workers                   4
                                    :poll-ms                   1000
                                    :retry-first-wait-ms       30000
                                    :retry-wait-factor         2
                                    :retry-max-wait-ms         3600000
                                    :stuck-after-time-limit-ms 60000
                                    :forget-done-after-ms      604800000
                                    :stop-wait-ms              30000})

   A background job is a row in backroom.background_jobs; its tables are maps
   (background-jobs-tables) the app passes to cellar with its own. Every :poll-ms the workers look for
   due background jobs; each free worker claims one (FOR UPDATE SKIP LOCKED, so no two
   servers run the same one) and runs its kind's :run in one transaction,
   within :timeout-ms. A background job that throws or runs out of time runs again
   later, waiting longer each time (:retry-first-wait-ms, times
   :retry-wait-factor each time, up to :retry-max-wait-ms), until it has had
   its :attempts; then it is dead, kept with its last error. A background
   job that throws (make-give-up-exception …) is dead at once. A server that
   stops mid-background-job leaves it running; it is queued again once its
   time limit and :stuck-after-time-limit-ms have passed. A background job
   can run twice, so its work must be safe to repeat. Done background jobs
   older than :forget-done-after-ms are deleted when the workers start.
   Every setting and option is required, checked first (backroom.settings).

   A schedule puts its kind's background job in the queue at each time its cron line
   (UTC) allows; one server does it, whichever claims the schedule first."
  (:require
   [clojure.string :as str]
   [backroom.cron :as cron]
   [backroom.settings :as settings]
   [cellar.api :as cellar]
   [wire.api :as wire])
  (:import
   (java.lang System$Logger System$Logger$Level)
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

(def ^:private ^System$Logger logger (System/getLogger "backroom"))

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

(def ^:private no-options
  {:primary-key? false :optional? false :unique? false :default nil :references nil})

(def background-jobs-table
  "The queue: one row per background job."
  {:table     :backroom/background-jobs
   :columns   [[:background-job/id           :uuid         (assoc no-options :primary-key? true :default :random-uuid)]
               [:background-job/kind         :string       no-options]
               [:background-job/params       :string       no-options]      ; JSON text, through wire
               [:background-job/state        [:enum "queued" "running" "done" "dead"] (assoc no-options :default "queued")]
               [:background-job/run-at       :time/instant (assoc no-options :default :now)]
               [:background-job/attempts     :int          (assoc no-options :default 0)]
               [:background-job/max-attempts :int          no-options]
               [:background-job/timeout-ms   :int          no-options]
               [:background-job/last-error   :string       (assoc no-options :optional? true)]
               [:background-job/locked-at    :time/instant (assoc no-options :optional? true)]
               [:background-job/created-at   :time/instant (assoc no-options :default :now)]
               [:background-job/finished-at  :time/instant (assoc no-options :optional? true)]
               [:background-job/key          :string       (assoc no-options :optional? true :unique? true)]]
   :indexes   [[:background-job/state :background-job/run-at]]
   :gates     nil
   :on-change nil})

(def background-job-schedules-table
  "The schedules: the next time each runs."
  {:table     :backroom/background-job-schedules
   :columns   [[:background-job-schedule/key     :string       (assoc no-options :primary-key? true)]
               [:background-job-schedule/next-at :time/instant no-options]]
   :indexes   []
   :gates     nil
   :on-change nil})

(def background-jobs-tables
  "Both tables, to pass to cellar with the app's own."
  [background-jobs-table background-job-schedules-table])

;; -----------------------------------------------------------------------------
;; Queueing

(defn- turn-params-into-json-text
  "A background job's params as JSON text by its kind's :params-shape;
   params that don't fit it, or carry a key it doesn't name, throw."
  [kind-name params-shape params]
  (let [{:keys [json-data errors]} (wire/turn-shaped-data-into-json-data-or-errors params-shape params)]
    (when errors
      (throw (ex-info (str "Background job " kind-name "'s params don't fit its shape")
                      {:type   :backroom/bad-params
                       :kind   kind-name
                       :errors errors})))
    (wire/turn-data-into-json-text json-data)))

(defn add-background-job-to-queue!
  "Saves a background job of a kind, on a pool or in a transaction (then it
   exists only if that commits). Its params must fit the kind's
   :params-shape. Options, every key required: :run-in-ms, run no sooner
   (0 for now); :key, nil, or one background job of this key for as long
   as it is remembered: a second one queues nothing and returns the first
   one's id. Returns the background job's id."
  [pool-or-transaction kind params options]
  (settings/check-kind kind)
  (settings/check-queue-options options)
  (let [{:keys [name params-shape timeout-ms attempts]} kind
        {:keys [run-in-ms key]}                         options]
    (:background-job/id (cellar/find-one-row pool-or-transaction insert-background-job
                                             {:background-job/kind         (turn-kind-name-into-text name)
                                              :background-job/params       (turn-params-into-json-text name params-shape params)
                                              :background-job/run-at       (turn-instant-into-utc (.plusMillis (Instant/now) (long run-in-ms)))
                                              :background-job/max-attempts attempts
                                              :background-job/timeout-ms   timeout-ms
                                              :background-job/key          key}))))

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

(defn- run-background-job-and-catch
  "Runs the background job's kind in one transaction. Returns :ok or what
   was thrown."
  [pool kinds-by-name {:background-job/keys [id kind params attempts]}]
  (try
    (let [{:keys [run params-shape] :as background-job-kind} (get kinds-by-name kind)]
      (when-not background-job-kind
        (throw (ex-info (str "No background job kind " kind " on this server")
                        {:type :backroom/unknown-kind :kind kind})))
      (let [{:keys [shaped-data errors]} (wire/turn-json-data-into-shaped-data-or-errors
                                          params-shape (wire/turn-json-text-into-data params))]
        (when errors
          (throw (ex-info "The background job's params don't fit its shape" {:errors errors})))
        (cellar/run-in-transaction! pool {:read-only? false}
          (fn [transaction]
            (run {:transaction transaction :background-job-id id :attempt attempts} shaped-data)))
        :ok))
    (catch Throwable throwable throwable)))

(defn- run-claimed-background-job!
  "Runs a claimed background job within its time limit and saves what happened."
  [{:keys [pool kinds-by-name] :as running} {:background-job/keys [id kind attempts timeout-ms] :as background-job}]
  (let [outcome (promise)
        thread  (Thread/startVirtualThread #(deliver outcome (run-background-job-and-catch pool kinds-by-name background-job)))
        result  (deref outcome timeout-ms :timed-out)]
    (when (= :timed-out result) (.interrupt thread))
    (if (= :ok result)
      (cellar/run-statement! pool update-background-job-to-done {:background-job/id id})
      (do
        (when (instance? Throwable result)
          (.log logger System$Logger$Level/WARNING (str "Background job " kind " " id " failed") ^Throwable result))
        (cellar/run-statement! pool update-background-job-to-failed
                               {:background-job/id         id
                                :background-job/retry-ms   (find-wait-before-retry-ms running attempts)
                                :background-job/for-good   (and (instance? Throwable result) (does-failure-give-up? result))
                                :background-job/last-error (if (= :timed-out result)
                                                             (str "Ran longer than " timeout-ms " ms")
                                                             (str (.getName (class result)) ": " (ex-message result)))})))))

;; -----------------------------------------------------------------------------
;; The workers

(defn- make-schedule-key [{:keys [kind params cron]}]
  (str (turn-kind-name-into-text (:name kind)) " " cron " "
       (turn-params-into-json-text (:name kind) (:params-shape kind) params)))

(defn- add-due-scheduled-background-jobs!
  "Queues the background job of every due schedule this server claims, and moves the
   schedule to its next time."
  [pool schedules-by-key]
  (cellar/run-in-transaction! pool {:read-only? false}
    (fn [transaction]
      (doseq [{key :background-job-schedule/key} (cellar/find-rows transaction find-due-schedules {})
              :let [{:keys [kind params cron]} (get schedules-by-key key)]
              :when kind]
        (add-background-job-to-queue! transaction kind params {:run-in-ms 0 :key nil})
        (cellar/run-statement! transaction update-schedule
                               {:background-job-schedule/key     key
                                :background-job-schedule/next-at (turn-instant-into-utc
                                                                  (cron/find-next-time-for-cron-line cron (Instant/now)))})))))

(defn- run-dispatcher!
  "Until stopped: queue due schedules, queue stuck background jobs again, and give
   every free worker a due background job; then wait :poll-ms, or less when a worker
   finishes or the workers stop."
  [{:keys [pool stopped ^Semaphore free-workers ^ArrayBlockingQueue wake schedules-by-key poll-ms stuck-after-time-limit-ms]
    :as   running}]
  (while (not @stopped)
    (try
      (add-due-scheduled-background-jobs! pool schedules-by-key)
      (cellar/run-statement! pool update-stuck-background-jobs {:background-job/stuck-after-time-limit-ms stuck-after-time-limit-ms})
      (loop []
        (when (and (not @stopped) (.tryAcquire free-workers))
          (if-let [background-job (cellar/find-one-row pool claim-next-background-job {})]
            (do (Thread/startVirtualThread
                 (fn []
                   (try (run-claimed-background-job! running (update background-job :background-job/kind keyword))
                        (catch Throwable e
                          (.log logger System$Logger$Level/ERROR "A background job's outcome couldn't be saved" e))
                        (finally
                          (.release free-workers)
                          (.offer wake :worker-free)))))
                (recur))
            (.release free-workers))))
      (catch Throwable e
        (when-not @stopped
          (.log logger System$Logger$Level/WARNING "The background job dispatcher failed; trying again" e))))
    (.poll wake (long poll-ms) TimeUnit/MILLISECONDS)))

(defn start-workers!
  "Starts the workers on this server. Options, every key required, checked
   first (backroom.settings/workers-options-shape):

     :kinds     [background job kinds]   every kind this server runs
     :schedules [{:kind a-background-job-kind :params {…} :cron \"0 1 * * *\"}]
     and the settings (settings-shape): :workers :poll-ms
     :retry-first-wait-ms :retry-wait-factor :retry-max-wait-ms
     :stuck-after-time-limit-ms :forget-done-after-ms :stop-wait-ms

   Forgets done background jobs older than :forget-done-after-ms. Returns
   the running workers."
  [pool options]
  (settings/check-workers-options options)
  (let [{:keys [kinds schedules workers forget-done-after-ms]} options
        names (map :name kinds)]
    (when (not= (count names) (count (set names)))
      (throw (ex-info (str "Background job kinds named twice: " (str/join " " names))
                      {:type :backroom/kind-named-twice})))
    (doseq [{:keys [cron]} schedules]
      (cron/turn-cron-line-into-allowed-values cron))
    (let [schedules-by-key (into {} (map (juxt make-schedule-key identity)) schedules)
          running          (merge (select-keys options [:workers :poll-ms :retry-first-wait-ms :retry-wait-factor
                                                        :retry-max-wait-ms :stuck-after-time-limit-ms :stop-wait-ms])
                                  {:pool             pool
                                   :kinds-by-name    (into {} (map (juxt :name identity)) kinds)
                                   :schedules-by-key schedules-by-key
                                   :stopped          (atom false)
                                   :free-workers     (Semaphore. (int workers))
                                   ;; one wake-up is enough: the dispatcher looks at everything when it wakes
                                   :wake             (ArrayBlockingQueue. 1)})]
      (cellar/run-statement! pool delete-old-done-background-jobs {:background-job/forget-done-after-ms forget-done-after-ms})
      (doseq [[key {:keys [cron]}] schedules-by-key]
        (cellar/run-statement! pool insert-schedule
                               {:background-job-schedule/key     key
                                :background-job-schedule/next-at (turn-instant-into-utc
                                                                  (cron/find-next-time-for-cron-line cron (Instant/now)))}))
      (assoc running :dispatcher (Thread/startVirtualThread #(run-dispatcher! running))))))

(defn stop-workers!
  "Stops taking background jobs, and waits for the dispatcher and the
   running background jobs to finish, at most :stop-wait-ms in all. Returns
   whether they had all finished; one that hadn't goes on in the
   background, and is queued again later if its server stops."
  [{:keys [stopped ^Thread dispatcher ^Semaphore free-workers ^ArrayBlockingQueue wake workers stop-wait-ms]}]
  (reset! stopped true)
  (.offer wake :stop)
  (let [deadline (+ (System/nanoTime) (* 1000000 (long stop-wait-ms)))
        left-ms  #(max 0 (quot (- deadline (System/nanoTime)) 1000000))]
    (and (.join dispatcher (java.time.Duration/ofMillis (left-ms)))
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
