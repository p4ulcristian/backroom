(ns groundwork-background-jobs.api-test
  "Runs against a real Postgres when BACKGROUND_JOBS_TEST_PORT is set (on
   localhost, user from BACKGROUND_JOBS_TEST_USER, no password, no TLS);
   says it was skipped otherwise."
  (:require
   [clojure.test :refer [deftest is testing]]
   [groundwork-background-jobs.api :as background-jobs]
   [groundwork-background-jobs.test-shaper :refer [shaper]]
   [groundwork-postgres.api :as postgres]
   [clojure.string :as str]
   [jsonista.core :as json]
   [groundwork-logs.api :as logs]
   [groundwork-metrics.api :as metrics]))

(def ^:private port (some-> (System/getenv "BACKGROUND_JOBS_TEST_PORT") parse-long))

(def ^:private settings
  {:workers                   2
   :poll-ms                   100
   :retry-first-wait-ms       30000
   :retry-wait-factor         2
   :retry-max-wait-ms         3600000
   :stuck-after-time-limit-ms 60000
   :forget-done-after-ms      604800000
   :forget-done-every-ms      3600000
   :stop-wait-ms              5000
   :metric-buckets            {:job-run-seconds [0.01 0.1 1 10]}})

(def ^:private lines
  "The lines the tests' groundwork-logs wrote, as JSON data."
  (atom []))

(def ^:private log
  (logs/log-function (logs/make-logger {:lowest-level :debug :format :json :errors-kept 100
                                               :write-to     #(swap! lines conj (json/read-value %))})))

(defn- make-meter
  "A new meter, as an app makes its own: each pool and each start-workers!
   with other kinds takes its own."
  []
  (metrics/make-meter {:jvm-metrics false :most-series-per-metric 10000} {:log log}))

(def ^:private queue-meter
  "The meter the tests that don't read it add background jobs with."
  (make-meter))

(def ^:private now {:shaper shaper :meter queue-meter :run-in-ms 0 :key nil})

(def ^:private actors-asked
  "The system actors the tests' run-in-transaction-as was asked to open a
   transaction as, in order."
  (atom []))

(defn- run-in-transaction-as
  "The tests' :run-in-transaction-as, as an app's (groundwork-access's run-as-system!
   in groundwork) opens one: a transaction on the pool, carrying the job's
   log; it notes the actor."
  [pool system-actor log f]
  (swap! actors-asked conj system-actor)
  (postgres/run-in-transaction! pool {:read-only? false :log log} f))

(defn- with-new-meter
  "start-workers!'s options with the tests' groundwork-shaped-json, log and
   run-in-transaction-as, and a new meter."
  [options]
  (assoc options :shaper shaper :log log :meter (make-meter) :run-in-transaction-as run-in-transaction-as))

(defn- make-pool [user]
  (postgres/start-pool! {:host "localhost" :port port :database "postgres" :user user :password nil
                       :tls :off :tls-root-cert nil
                       :pool-size 4
                       :connection-wait-ms 30000 :connection-check-wait-ms 5000
                       :connection-max-lifetime-ms 1800000 :connection-keepalive-every-ms 120000
                       :leak-warning-after-ms :none
                       :connect-timeout-seconds 10 :read-timeout-seconds :none :tcp-keepalive? false
                       :statement-timeout-ms 5000 :lock-timeout-ms :none :idle-in-transaction-timeout-ms :none
                       :time-zone "UTC"
                       :metric-buckets {:query-seconds [0.01 0.1 1] :connection-acquire-seconds [0.01 0.1 1]
                                        :connection-usage-seconds [0.01 0.1 1]}}
                      {:log log :meter (make-meter)}))

(defn- find-lines [event]
  (filterv #(= event (get % "event")) @lines))

(defn- run-sql! [pool text]
  (postgres/run-statement! pool (postgres/make-query-from-sql-text "test" text) {}))

(defn- make-tables-afresh! [pool]
  (run-sql! pool "DROP SCHEMA IF EXISTS groundwork_background_jobs CASCADE; DROP SCHEMA IF EXISTS groundwork_postgres CASCADE")
  @(:slow-part (postgres/migrate! pool {:tables background-jobs/background-jobs-tables :extensions [] :functions [] :views []} []
                                {:instant-part {:statement-timeout-ms 5000 :lock-timeout-ms 5000}
                                 :slow-part    {:statement-timeout-ms :none :lock-timeout-ms 5000}
                                 :log          (fn [_ _ _])})))

(def ^:private emails-sent (atom []))

(def ^:private send-invoice-email
  {:name         :test.mail/send-invoice-email
   :system-actor :test-jobs
   :params-shape [:map [:order/id :int]]
   :run          (fn [{job-log :log} params]
                   (job-log :info :test.mail/sent {:order/id (:order/id params)})
                   (swap! emails-sent conj (:order/id params)))
   :timeout-ms   5000
   :attempts     3})

(def ^:private give-up
  {:name         :test/give-up
   :system-actor :test-jobs
   :params-shape [:map]
   :run          (fn [_ _] (throw (background-jobs/make-give-up-exception "The order is gone" {})))
   :timeout-ms   5000
   :attempts     3})

(defn- wait-until [done?]
  (loop [n 0]
    (when (and (not (done?)) (< n 100))
      (Thread/sleep 50)
      (recur (inc n)))))

(deftest background-jobs-against-postgres
  (if-not port
    (println "groundwork-background-jobs.api-test: skipped, BACKGROUND_JOBS_TEST_PORT is not set")
    (let [pool (make-pool (System/getenv "BACKGROUND_JOBS_TEST_USER"))]
      (try
        (make-tables-afresh! pool)
        (reset! emails-sent [])
        (let [workers (background-jobs/start-workers! pool (with-new-meter (assoc settings :kinds [send-invoice-email give-up] :schedules [])))]
          (try
            (testing "a queued background job runs, with its params as shaped data, and a log that knows it"
              (let [id (background-jobs/add-background-job-to-queue! pool send-invoice-email {:order/id 42} now)]
                (wait-until #(seq @emails-sent))
                (is (= [42] @emails-sent))
                (is (= [{"background-job/id" (str id) "background-job/kind" "test.mail/send-invoice-email"}]
                       (mapv #(get % "context") (find-lines "test.mail/sent")))
                    "the run's own line carries the background job's id and kind")))
            (testing "params that don't fit the shape are refused"
              (is (thrown? clojure.lang.ExceptionInfo
                           (background-jobs/add-background-job-to-queue! pool send-invoice-email {:order/id "42"} now))))
            (testing "a background job that gives up is dead at once, with its message"
              (let [id (background-jobs/add-background-job-to-queue! pool give-up {} now)]
                (wait-until #(some (fn [background-job] (= id (:background-job/id background-job))) (background-jobs/list-dead-background-jobs pool 10)))
                (is (= "clojure.lang.ExceptionInfo: The order is gone"
                       (:background-job/last-error (first (filter #(= id (:background-job/id %)) (background-jobs/list-dead-background-jobs pool 10))))))
                (let [[gave-up] (find-lines "groundwork-background-jobs/gave-up")]
                  (is (= ["error" {"background-job/id" (str id) "background-job/kind" "test/give-up"}
                          {"background-job/attempts" 1} "The order is gone"]
                         [(get gave-up "level") (get gave-up "context") (get gave-up "data")
                          (get-in gave-up ["error" "message"])])
                      "a background job given up is an error, with its exception"))))
            (testing "one background job per key"
              (is (= (background-jobs/add-background-job-to-queue! pool send-invoice-email {:order/id 1} {:shaper shaper :meter queue-meter :run-in-ms 60000 :key "k"})
                     (background-jobs/add-background-job-to-queue! pool send-invoice-email {:order/id 1} {:shaper shaper :meter queue-meter :run-in-ms 60000 :key "k"}))))
            (finally (is (background-jobs/stop-workers! workers) "every running background job had finished"))))
        (finally (postgres/stop-pool! pool))))))

(deftest a-role-that-owns-nothing-queues-and-claims
  (if-not port
    (println "groundwork-background-jobs.api-test: skipped, BACKGROUND_JOBS_TEST_PORT is not set")
    (let [owner-pool (make-pool (System/getenv "BACKGROUND_JOBS_TEST_USER"))]
      (try
        (make-tables-afresh! owner-pool)
        (reset! emails-sent [])
        (run-sql! owner-pool "DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'groundwork_background_jobs_app') THEN CREATE ROLE groundwork_background_jobs_app LOGIN; END IF; END $$")
        (postgres/grant-tables-to-role! owner-pool background-jobs/background-jobs-tables "groundwork_background_jobs_app")
        (let [app-pool (make-pool "groundwork_background_jobs_app")]
          (try
            (let [workers (background-jobs/start-workers! app-pool (with-new-meter (assoc settings :workers 1 :kinds [send-invoice-email] :schedules [])))]
              (try
                (testing "the app's role, granted groundwork-background-jobs's tables by groundwork-postgres, queues a background job and a worker claims and runs it"
                  (background-jobs/add-background-job-to-queue! app-pool send-invoice-email {:order/id 7} now)
                  (wait-until #(seq @emails-sent))
                  (is (= [7] @emails-sent))
                  (is (= [] (background-jobs/list-queued-background-jobs app-pool 10))))
                (finally (background-jobs/stop-workers! workers))))
            (finally (postgres/stop-pool! app-pool))))
        (finally (postgres/stop-pool! owner-pool))))))

(defn- find-refusal [f]
  (try (f) :nothing
       (catch clojure.lang.ExceptionInfo e (select-keys (ex-data e) [:type :errors]))))

(deftest every-setting-and-option-is-required
  (testing "start-workers! names each missing setting, before anything runs"
    (let [{:keys [type errors]} (find-refusal #(background-jobs/start-workers! nil {:shaper shaper :log log :kinds [] :schedules [] :workers 1 :poll-ms 100}))]
      (is (= :groundwork-background-jobs/bad-options type))
      (is (= #{:meter :run-in-transaction-as :retry-first-wait-ms :retry-wait-factor :retry-max-wait-ms :stuck-after-time-limit-ms
               :forget-done-after-ms :forget-done-every-ms :stop-wait-ms :metric-buckets}
             (set (keys errors))))))
  (testing "and an unknown one"
    (is (= {:type :groundwork-background-jobs/bad-options :errors {:workerz ["disallowed key"]}}
           (find-refusal #(background-jobs/start-workers! nil (with-new-meter (assoc settings :kinds [] :schedules [] :workerz 3)))))))
  (testing "a kind writes out every key"
    (is (= {:type :groundwork-background-jobs/bad-kind :errors {:attempts ["missing required key"]}}
           (find-refusal #(background-jobs/add-background-job-to-queue! nil (dissoc send-invoice-email :attempts) {:order/id 1} now)))))
  (testing "a kind names its system actor, a keyword without a namespace"
    (is (= {:type :groundwork-background-jobs/bad-kind :errors {:system-actor ["missing required key"]}}
           (find-refusal #(background-jobs/add-background-job-to-queue! nil (dissoc send-invoice-email :system-actor) {:order/id 1} now))))
    (is (= :groundwork-background-jobs/bad-options
           (:type (find-refusal #(background-jobs/start-workers! nil (with-new-meter (assoc settings :kinds [(assoc send-invoice-email :system-actor :test/jobs)]
                                                                                      :schedules []))))))
        "start-workers! checks its kinds too"))
  (testing "add-background-job-to-queue! says :run-in-ms and :key"
    (is (= {:type :groundwork-background-jobs/bad-options :errors {:key ["missing required key"]}}
           (find-refusal #(background-jobs/add-background-job-to-queue! nil send-invoice-email {:order/id 1} (dissoc now :key))))))
  (testing "groundwork-background-jobs makes no shaper and no log: start-workers! takes the app's both"
    (is (= {:type :groundwork-background-jobs/bad-options :errors {:shaper ["missing required key"] :log ["missing required key"]
                                                 :meter ["missing required key"] :run-in-transaction-as ["missing required key"]}}
           (find-refusal #(background-jobs/start-workers! nil (assoc settings :kinds [] :schedules [])))))
    (is (= :groundwork-logs/not-a-log
           (:type (ex-data (try (background-jobs/start-workers! nil (assoc settings :shaper shaper :log (fn [_ _ _]) :meter (make-meter)
                                                                    :run-in-transaction-as run-in-transaction-as
                                                                    :kinds [] :schedules []))
                                (catch clojure.lang.ExceptionInfo e e)))))
        "a log groundwork-logs didn't make can't carry a background job's context: refused at the start"))
  (testing "add-background-job-to-queue! takes the app's shaper and meter"
    (is (= {:type :groundwork-background-jobs/bad-options :errors {:shaper ["missing required key"] :meter ["missing required key"]}}
           (find-refusal #(background-jobs/add-background-job-to-queue! nil send-invoice-email {:order/id 1} {:run-in-ms 0 :key nil})))))
  (testing "the meter is the app's: one meter didn't make is refused at the start"
    (is (= {:type :groundwork-background-jobs/bad-options :errors {:meter ["should be the app's meter, made by groundwork-metrics.api/make-meter"]}}
           (find-refusal #(background-jobs/start-workers! nil (assoc (with-new-meter (assoc settings :kinds [] :schedules [])) :meter nil)))))
    (is (= {:type :groundwork-metrics/not-a-meter}
           (find-refusal #(background-jobs/start-workers! nil (assoc (with-new-meter (assoc settings :kinds [] :schedules [])) :meter {}))))))
  (testing "the run time's buckets are a setting; meter checks they go up"
    (is (= {:type :groundwork-metrics/bad-metrics}
           (select-keys (find-refusal #(background-jobs/start-workers! nil (with-new-meter (assoc settings :kinds [] :schedules []
                                                                                          :metric-buckets {:job-run-seconds [2 1]}))))
                        [:type])))))

(def ^:private runs (atom 0))

(def ^:private fails-once
  {:name         :test/fails-once
   :system-actor :test-jobs
   :params-shape [:map]
   :run          (fn [_ _] (when (= 1 (swap! runs inc)) (throw (ex-info "the first time" {}))))
   :timeout-ms   5000
   :attempts     3})

(def ^:private count-done
  (postgres/make-query-from-sql-text "done jobs" "SELECT count(*) AS n FROM groundwork_background_jobs.background_jobs WHERE background_job__state = 'done'"))

(defn- insert-done-background-jobs! [pool how-many]
  (run-sql! pool (str "INSERT INTO groundwork_background_jobs.background_jobs (background_job__kind, background_job__params, background_job__state,"
                      " background_job__max_attempts, background_job__timeout_ms, background_job__finished_at)"
                      " SELECT 'test/fails-once', '{}', 'done', 3, 1000, now() - interval '1 second'"
                      " FROM generate_series(1, " how-many ")")))

(deftest the-settings-set-the-waits
  (if-not port
    (println "groundwork-background-jobs.api-test: skipped, BACKGROUND_JOBS_TEST_PORT is not set")
    (let [pool (make-pool (System/getenv "BACKGROUND_JOBS_TEST_USER"))]
      (try
        (make-tables-afresh! pool)
        (reset! runs 0)
        (testing "a failure runs again after :retry-first-wait-ms"
          (let [workers (background-jobs/start-workers! pool (with-new-meter (assoc settings :retry-first-wait-ms 100 :kinds [fails-once] :schedules [])))]
            (try
              (background-jobs/add-background-job-to-queue! pool fails-once {} now)
              (wait-until #(= 1 (:n (postgres/find-one-row pool count-done {}))))
              (is (= 2 @runs))
              (is (= [["warn" "test/fails-once" {"background-job/attempt" 1 "background-job/retry-in-ms" 100} "the first time"]]
                     (mapv #(vector (get % "level") (get-in % ["context" "background-job/kind"]) (get % "data")
                                    (get-in % ["error" "message"]))
                           (find-lines "groundwork-background-jobs/failed")))
                  "a failure that runs again is a warning, with the wait")
              (finally (background-jobs/stop-workers! workers)))))
        (testing "done background jobs older than :forget-done-after-ms are forgotten when the workers start"
          (reset! lines [])
          (background-jobs/stop-workers! (background-jobs/start-workers! pool (with-new-meter (assoc settings :forget-done-after-ms 0 :kinds [] :schedules []))))
          (is (= 0 (:n (postgres/find-one-row pool count-done {}))))
          (is (= [["info" {"background-job/forgotten" 1 "background-job/forget-done-after-ms" 0}]]
                 (mapv #(vector (get % "level") (get % "data")) (find-lines "groundwork-background-jobs/forgot-done")))
              "each round is a line, info when it forgot any"))
        (testing "and every :forget-done-every-ms while they run"
          (let [workers (background-jobs/start-workers! pool (with-new-meter (assoc settings :forget-done-after-ms 0 :forget-done-every-ms 100
                                                             :kinds [] :schedules [])))]
            (try
              (insert-done-background-jobs! pool 3)
              (is (= 3 (:n (postgres/find-one-row pool count-done {}))))
              (wait-until #(= 0 (:n (postgres/find-one-row pool count-done {}))))
              (is (= 0 (:n (postgres/find-one-row pool count-done {}))) "forgotten without a restart")
              (finally (background-jobs/stop-workers! workers)))))
        (testing "a background job left running is queued again past its time limit and :stuck-after-time-limit-ms"
          (run-sql! pool (str "INSERT INTO groundwork_background_jobs.background_jobs (background_job__kind, background_job__params, background_job__state,"
                              " background_job__max_attempts, background_job__timeout_ms, background_job__locked_at)"
                              " VALUES ('test/fails-once', '{}', 'running', 3, 1000, now() - interval '2 seconds')"))
          (let [workers (background-jobs/start-workers! pool (with-new-meter (assoc settings :stuck-after-time-limit-ms 0 :kinds [fails-once] :schedules [])))]
            (try
              (wait-until #(= 1 (:n (postgres/find-one-row pool count-done {}))))
              (is (= 1 (:n (postgres/find-one-row pool count-done {}))))
              (finally (background-jobs/stop-workers! workers)))))
        (finally (postgres/stop-pool! pool))))))

(deftest several-servers-forget-at-once
  (if-not port
    (println "groundwork-background-jobs.api-test: skipped, BACKGROUND_JOBS_TEST_PORT is not set")
    (let [pools (vec (repeatedly 3 #(make-pool (System/getenv "BACKGROUND_JOBS_TEST_USER"))))]
      (try
        (make-tables-afresh! (first pools))
        (insert-done-background-jobs! (first pools) 5000)
        (reset! lines [])
        (testing "three servers forgetting every 10 ms skip each other's rows: each row is deleted once, and none fails"
          (let [servers (mapv #(background-jobs/start-workers! % (with-new-meter (assoc settings :forget-done-after-ms 0 :forget-done-every-ms 10
                                                                 :kinds [] :schedules [])))
                              pools)]
            (try
              (wait-until #(= 0 (:n (postgres/find-one-row (first pools) count-done {}))))
              (finally (run! background-jobs/stop-workers! servers))))
          (is (= 0 (:n (postgres/find-one-row (first pools) count-done {}))))
          (is (= 5000 (reduce + (map #(get-in % ["data" "background-job/forgotten"]) (find-lines "groundwork-background-jobs/forgot-done")))))
          (is (= [] (find-lines "groundwork-background-jobs/forgetting-failed"))))
        (finally (run! postgres/stop-pool! pools))))))

(defn- find-metric-lines [a-meter prefix]
  (filterv #(str/starts-with? % prefix) (str/split-lines (metrics/write-metrics-text a-meter))))

(defn- find-slow-running [a-meter]
  (filterv #(str/includes? % "slow") (find-metric-lines a-meter "groundwork_background_jobs_jobs_running")))

(def ^:private always-fails
  {:name         :test/always-fails
   :system-actor :test-jobs
   :params-shape [:map]
   :run          (fn [_ _] (throw (ex-info "never works" {})))
   :timeout-ms   5000
   :attempts     2})

(def ^:private slow
  {:name         :test/slow
   :system-actor :test-jobs
   :params-shape [:map]
   :run          (fn [_ _] (Thread/sleep 600))
   :timeout-ms   5000
   :attempts     1})

(deftest the-workers-count-on-the-apps-meter
  (if-not port
    (println "groundwork-background-jobs.api-test: skipped, BACKGROUND_JOBS_TEST_PORT is not set")
    (let [pool  (make-pool (System/getenv "BACKGROUND_JOBS_TEST_USER"))
          ;; one meter for the app: the pool's metrics and groundwork-background-jobs's beside them
          meter (:groundwork-postgres/meter pool)
          now   (assoc now :meter meter)]
      (try
        (make-tables-afresh! pool)
        (reset! emails-sent [])
        ;; a log of its own: the other tests read the shared one's lines
        (let [own-log (logs/log-function (logs/make-logger {:lowest-level :debug :format :json :errors-kept 10
                                                                    :write-to (fn [_])}))
              workers (background-jobs/start-workers! pool (assoc settings :shaper shaper :log own-log :meter meter :retry-first-wait-ms 100
                                                           :run-in-transaction-as run-in-transaction-as
                                                           :kinds [send-invoice-email give-up always-fails slow] :schedules []))]
          (try
            (testing "declared at the start: a metric without labels is there at 0"
              (is (= ["groundwork_background_jobs_dispatcher_failures_total 0"] (find-metric-lines meter "groundwork_background_jobs_dispatcher_failures_total"))))
            (background-jobs/add-background-job-to-queue! pool send-invoice-email {:order/id 1} now)
            (background-jobs/add-background-job-to-queue! pool give-up {} now)
            (background-jobs/add-background-job-to-queue! pool always-fails {} now)
            (testing "a job of a key already queued isn't added, nor counted"
              (background-jobs/add-background-job-to-queue! pool send-invoice-email {:order/id 2} (assoc now :run-in-ms 60000 :key "k"))
              (background-jobs/add-background-job-to-queue! pool send-invoice-email {:order/id 2} (assoc now :run-in-ms 60000 :key "k")))
            (testing "a job of a kind this server doesn't run is other"
              (run-sql! pool (str "INSERT INTO groundwork_background_jobs.background_jobs (background_job__kind, background_job__params,"
                                  " background_job__max_attempts, background_job__timeout_ms)"
                                  " VALUES ('elsewhere/kind', '{}', 1, 1000)")))
            (wait-until #(= 2 (count (background-jobs/list-dead-background-jobs pool 10))))
            (wait-until #(= 1 (count @emails-sent)))
            (wait-until #(= 5 (count (find-metric-lines meter "groundwork_background_jobs_job_runs_total"))))
            (testing "each job added, by kind"
              (is (= ["groundwork_background_jobs_jobs_added_total{kind=\"test.mail/send-invoice-email\"} 2"
                      "groundwork_background_jobs_jobs_added_total{kind=\"test/always-fails\"} 1"
                      "groundwork_background_jobs_jobs_added_total{kind=\"test/give-up\"} 1"]
                     (find-metric-lines meter "groundwork_background_jobs_jobs_added_total"))))
            (testing "each run, by kind and how it ended"
              (is (= ["groundwork_background_jobs_job_runs_total{kind=\"other\",outcome=\"gave-up\"} 1"
                      "groundwork_background_jobs_job_runs_total{kind=\"test.mail/send-invoice-email\",outcome=\"done\"} 1"
                      "groundwork_background_jobs_job_runs_total{kind=\"test/always-fails\",outcome=\"failed\"} 1"
                      "groundwork_background_jobs_job_runs_total{kind=\"test/always-fails\",outcome=\"gave-up\"} 1"
                      "groundwork_background_jobs_job_runs_total{kind=\"test/give-up\",outcome=\"gave-up\"} 1"]
                     (find-metric-lines meter "groundwork_background_jobs_job_runs_total"))))
            (testing "each run's time, by kind"
              (is (= ["groundwork_background_jobs_job_run_seconds_count{kind=\"other\"} 1"
                      "groundwork_background_jobs_job_run_seconds_count{kind=\"test.mail/send-invoice-email\"} 1"
                      "groundwork_background_jobs_job_run_seconds_count{kind=\"test/always-fails\"} 2"
                      "groundwork_background_jobs_job_run_seconds_count{kind=\"test/give-up\"} 1"]
                     (find-metric-lines meter "groundwork_background_jobs_job_run_seconds_count"))))
            (testing "the jobs running now, by kind"
              (background-jobs/add-background-job-to-queue! pool slow {} now)
              (wait-until #(= ["groundwork_background_jobs_jobs_running{kind=\"test/slow\"} 1"]
                              (find-slow-running meter)))
              (is (= ["groundwork_background_jobs_jobs_running{kind=\"test/slow\"} 1"]
                     (find-slow-running meter)))
              (wait-until #(= ["groundwork_background_jobs_jobs_running{kind=\"test/slow\"} 0"]
                              (find-slow-running meter)))
              (is (= ["groundwork_background_jobs_jobs_running{kind=\"test/slow\"} 0"]
                     (find-slow-running meter)))
              (is (= ["groundwork_background_jobs_job_run_seconds_bucket{kind=\"test/slow\",le=\"0.1\"} 0"
                      "groundwork_background_jobs_job_run_seconds_bucket{kind=\"test/slow\",le=\"1.0\"} 1"]
                     (filterv #(re-find #"le=\"(0\.1|1\.0)\"" %) (find-metric-lines meter "groundwork_background_jobs_job_run_seconds_bucket{kind=\"test/slow\"")))
                  "in the settings' buckets"))
            (finally (background-jobs/stop-workers! workers))))
        (finally (postgres/stop-pool! pool))))))


(def ^:private order-emails
  {:name         :test.mail/send-order-email
   :system-actor :order-emails
   :params-shape [:map [:order/id :int]]
   :run          (fn [_ params] (swap! emails-sent conj (:order/id params)))
   :timeout-ms   5000
   :attempts     1})

(def ^:private count-in-state
  (postgres/make-query-from-sql-text "jobs in a state" "SELECT count(*) AS n FROM groundwork_background_jobs.background_jobs WHERE background_job__state = CAST(:state AS text)"))

(deftest a-job-runs-as-its-kinds-system-actor-in-one-transaction
  (if-not port
    (println "groundwork-background-jobs.api-test: skipped, BACKGROUND_JOBS_TEST_PORT is not set")
    (let [pool (make-pool (System/getenv "BACKGROUND_JOBS_TEST_USER"))]
      (try
        (make-tables-afresh! pool)
        (reset! emails-sent [])
        (reset! actors-asked [])
        (testing "the transaction is opened by the app's run-in-transaction-as, as the kind's actor"
          (let [workers (background-jobs/start-workers! pool (with-new-meter (assoc settings :kinds [order-emails send-invoice-email] :schedules [])))]
            (try
              (background-jobs/add-background-job-to-queue! pool order-emails {:order/id 1042} now)
              (wait-until #(= 1 (:n (postgres/find-one-row pool count-in-state {:state "done"}))))
              (is (= [1042] @emails-sent))
              (is (= [:order-emails] @actors-asked) "the kind's :system-actor, nothing else")
              (finally (background-jobs/stop-workers! workers)))))
        (testing "the work and the done-mark commit together: a transaction that doesn't commit leaves the job not done"
          (let [opened  (atom 0)
                workers (background-jobs/start-workers! pool (assoc (with-new-meter (assoc settings :kinds [order-emails] :schedules []))
                                                             :run-in-transaction-as
                                                             (fn [pool _actor log f]
                                                               (swap! opened inc)
                                                               (postgres/run-in-transaction! pool {:read-only? false :log log}
                                                                 (fn [transaction]
                                                                   (f transaction)
                                                                   (throw (ex-info "the commit failed" {})))))))]
            (try
              (background-jobs/add-background-job-to-queue! pool order-emails {:order/id 7} now)
              (wait-until #(= 1 (:n (postgres/find-one-row pool count-in-state {:state "dead"}))))
              (is (= 1 @opened))
              (is (= 1 (:n (postgres/find-one-row pool count-in-state {:state "done"}))) "only the first job is done")
              (is (= "clojure.lang.ExceptionInfo: the commit failed"
                     (:background-job/last-error (first (background-jobs/list-dead-background-jobs pool 10))))
                  "its one attempt failed, with the transaction's error")
              (finally (background-jobs/stop-workers! workers)))))
        (testing "the transaction carries the job's log: work kept for after its commit that fails is written with the job's id and kind"
          (reset! lines [])
          (let [clears  {:name         :test.cache/clear
                         :system-actor :order-emails
                         :params-shape [:map]
                         :run          (fn [{:keys [transaction]} _]
                                         (postgres/run-after-commit! transaction #(throw (ex-info "the cache is gone" {}))))
                         :timeout-ms   5000
                         :attempts     1}
                workers (background-jobs/start-workers! pool (with-new-meter (assoc settings :kinds [clears] :schedules [])))]
            (try
              (let [id (background-jobs/add-background-job-to-queue! pool clears {} now)]
                (wait-until #(seq (find-lines "groundwork-postgres/after-commit-failed")))
                (is (= [{"background-job/id" (str id) "background-job/kind" "test.cache/clear"}]
                       (mapv #(get % "context") (find-lines "groundwork-postgres/after-commit-failed")))))
              (finally (background-jobs/stop-workers! workers)))))
        (finally (postgres/stop-pool! pool))))))
