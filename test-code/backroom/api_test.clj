(ns backroom.api-test
  "Runs against a real Postgres when BACKROOM_TEST_PORT is set (on
   localhost, user from BACKROOM_TEST_USER, no password, no TLS);
   says it was skipped otherwise."
  (:require
   [clojure.test :refer [deftest is testing]]
   [backroom.api :as backroom]
   [backroom.test-wire :refer [wire]]
   [cellar.api :as cellar]
   [jsonista.core :as json]
   [logbook.api :as logbook]))

(def ^:private port (some-> (System/getenv "BACKROOM_TEST_PORT") parse-long))

(def ^:private settings
  {:workers                   2
   :poll-ms                   100
   :retry-first-wait-ms       30000
   :retry-wait-factor         2
   :retry-max-wait-ms         3600000
   :stuck-after-time-limit-ms 60000
   :forget-done-after-ms      604800000
   :forget-done-every-ms      3600000
   :stop-wait-ms              5000})

(def ^:private now {:wire wire :run-in-ms 0 :key nil})

(def ^:private lines
  "The lines the tests' logbook wrote, as JSON data."
  (atom []))

(def ^:private log
  (logbook/log-function (logbook/make-logbook {:lowest-level :debug :format :json :errors-kept 100
                                               :write-to     #(swap! lines conj (json/read-value %))})))

(defn- make-pool [user]
  (cellar/start-pool! {:host "localhost" :port port :database "postgres" :user user :password nil
                       :tls :off :tls-root-cert nil
                       :pool-size 4
                       :connection-wait-ms 30000 :connection-check-wait-ms 5000
                       :connection-max-lifetime-ms 1800000 :connection-keepalive-every-ms 120000
                       :leak-warning-after-ms :none
                       :connect-timeout-seconds 10 :read-timeout-seconds :none :tcp-keepalive? false
                       :statement-timeout-ms 5000 :lock-timeout-ms :none :idle-in-transaction-timeout-ms :none
                       :time-zone "UTC"}
                      {:log log}))

(defn- find-lines [event]
  (filterv #(= event (get % "event")) @lines))

(defn- run-sql! [pool text]
  (cellar/run-statement! pool (cellar/make-query-from-sql-text "test" text) {}))

(defn- make-tables-afresh! [pool]
  (run-sql! pool "DROP SCHEMA IF EXISTS backroom CASCADE; DROP SCHEMA IF EXISTS cellar CASCADE")
  @(:slow-part (cellar/migrate! pool backroom/background-jobs-tables []
                                {:instant-part {:statement-timeout-ms 5000 :lock-timeout-ms 5000}
                                 :slow-part    {:statement-timeout-ms :none :lock-timeout-ms 5000}
                                 :log          (fn [_ _ _])})))

(def ^:private emails-sent (atom []))

(def ^:private send-invoice-email
  {:name         :test.mail/send-invoice-email
   :params-shape [:map [:order/id :int]]
   :run          (fn [{job-log :log} params]
                   (job-log :info :test.mail/sent {:order/id (:order/id params)})
                   (swap! emails-sent conj (:order/id params)))
   :timeout-ms   5000
   :attempts     3})

(def ^:private give-up
  {:name         :test/give-up
   :params-shape [:map]
   :run          (fn [_ _] (throw (backroom/make-give-up-exception "The order is gone" {})))
   :timeout-ms   5000
   :attempts     3})

(defn- wait-until [done?]
  (loop [n 0]
    (when (and (not (done?)) (< n 100))
      (Thread/sleep 50)
      (recur (inc n)))))

(deftest background-jobs-against-postgres
  (if-not port
    (println "backroom.api-test: skipped, BACKROOM_TEST_PORT is not set")
    (let [pool (make-pool (System/getenv "BACKROOM_TEST_USER"))]
      (try
        (make-tables-afresh! pool)
        (reset! emails-sent [])
        (let [workers (backroom/start-workers! pool (assoc settings :wire wire :log log :kinds [send-invoice-email give-up] :schedules []))]
          (try
            (testing "a queued background job runs, with its params as shaped data, and a log that knows it"
              (let [id (backroom/add-background-job-to-queue! pool send-invoice-email {:order/id 42} now)]
                (wait-until #(seq @emails-sent))
                (is (= [42] @emails-sent))
                (is (= [{"background-job/id" (str id) "background-job/kind" "test.mail/send-invoice-email"}]
                       (mapv #(get % "context") (find-lines "test.mail/sent")))
                    "the run's own line carries the background job's id and kind")))
            (testing "params that don't fit the shape are refused"
              (is (thrown? clojure.lang.ExceptionInfo
                           (backroom/add-background-job-to-queue! pool send-invoice-email {:order/id "42"} now))))
            (testing "a background job that gives up is dead at once, with its message"
              (let [id (backroom/add-background-job-to-queue! pool give-up {} now)]
                (wait-until #(some (fn [background-job] (= id (:background-job/id background-job))) (backroom/list-dead-background-jobs pool 10)))
                (is (= "clojure.lang.ExceptionInfo: The order is gone"
                       (:background-job/last-error (first (filter #(= id (:background-job/id %)) (backroom/list-dead-background-jobs pool 10))))))
                (let [[gave-up] (find-lines "backroom/gave-up")]
                  (is (= ["error" {"background-job/id" (str id) "background-job/kind" "test/give-up"}
                          {"background-job/attempts" 1} "The order is gone"]
                         [(get gave-up "level") (get gave-up "context") (get gave-up "data")
                          (get-in gave-up ["error" "message"])])
                      "a background job given up is an error, with its exception"))))
            (testing "one background job per key"
              (is (= (backroom/add-background-job-to-queue! pool send-invoice-email {:order/id 1} {:wire wire :run-in-ms 60000 :key "k"})
                     (backroom/add-background-job-to-queue! pool send-invoice-email {:order/id 1} {:wire wire :run-in-ms 60000 :key "k"}))))
            (finally (is (backroom/stop-workers! workers) "every running background job had finished"))))
        (finally (cellar/stop-pool! pool))))))

(deftest a-role-that-owns-nothing-queues-and-claims
  (if-not port
    (println "backroom.api-test: skipped, BACKROOM_TEST_PORT is not set")
    (let [owner-pool (make-pool (System/getenv "BACKROOM_TEST_USER"))]
      (try
        (make-tables-afresh! owner-pool)
        (reset! emails-sent [])
        (run-sql! owner-pool "DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'backroom_app') THEN CREATE ROLE backroom_app LOGIN; END IF; END $$")
        (cellar/grant-tables-to-role! owner-pool backroom/background-jobs-tables "backroom_app")
        (let [app-pool (make-pool "backroom_app")]
          (try
            (let [workers (backroom/start-workers! app-pool (assoc settings :wire wire :log log :workers 1 :kinds [send-invoice-email] :schedules []))]
              (try
                (testing "the app's role, granted backroom's tables by cellar, queues a background job and a worker claims and runs it"
                  (backroom/add-background-job-to-queue! app-pool send-invoice-email {:order/id 7} now)
                  (wait-until #(seq @emails-sent))
                  (is (= [7] @emails-sent))
                  (is (= [] (backroom/list-queued-background-jobs app-pool 10))))
                (finally (backroom/stop-workers! workers))))
            (finally (cellar/stop-pool! app-pool))))
        (finally (cellar/stop-pool! owner-pool))))))

(defn- find-refusal [f]
  (try (f) :nothing
       (catch clojure.lang.ExceptionInfo e (select-keys (ex-data e) [:type :errors]))))

(deftest every-setting-and-option-is-required
  (testing "start-workers! names each missing setting, before anything runs"
    (let [{:keys [type errors]} (find-refusal #(backroom/start-workers! nil {:wire wire :log log :kinds [] :schedules [] :workers 1 :poll-ms 100}))]
      (is (= :backroom/bad-options type))
      (is (= #{:retry-first-wait-ms :retry-wait-factor :retry-max-wait-ms :stuck-after-time-limit-ms
               :forget-done-after-ms :forget-done-every-ms :stop-wait-ms}
             (set (keys errors))))))
  (testing "and an unknown one"
    (is (= {:type :backroom/bad-options :errors {:workerz ["disallowed key"]}}
           (find-refusal #(backroom/start-workers! nil (assoc settings :wire wire :log log :kinds [] :schedules [] :workerz 3))))))
  (testing "a kind writes out every key"
    (is (= {:type :backroom/bad-kind :errors {:attempts ["missing required key"]}}
           (find-refusal #(backroom/add-background-job-to-queue! nil (dissoc send-invoice-email :attempts) {:order/id 1} now)))))
  (testing "add-background-job-to-queue! says :run-in-ms and :key"
    (is (= {:type :backroom/bad-options :errors {:key ["missing required key"]}}
           (find-refusal #(backroom/add-background-job-to-queue! nil send-invoice-email {:order/id 1} {:wire wire :run-in-ms 0})))))
  (testing "backroom makes no wire and no log: start-workers! takes the app's both"
    (is (= {:type :backroom/bad-options :errors {:wire ["missing required key"] :log ["missing required key"]}}
           (find-refusal #(backroom/start-workers! nil (assoc settings :kinds [] :schedules [])))))
    (is (= :logbook/not-a-log
           (:type (ex-data (try (backroom/start-workers! nil (assoc settings :wire wire :log (fn [_ _ _]) :kinds [] :schedules []))
                                (catch clojure.lang.ExceptionInfo e e)))))
        "a log logbook didn't make can't carry a background job's context: refused at the start"))
  (testing "add-background-job-to-queue! takes the app's wire"
    (is (= {:type :backroom/bad-options :errors {:wire ["missing required key"]}}
           (find-refusal #(backroom/add-background-job-to-queue! nil send-invoice-email {:order/id 1} {:run-in-ms 0 :key nil}))))))

(def ^:private runs (atom 0))

(def ^:private fails-once
  {:name         :test/fails-once
   :params-shape [:map]
   :run          (fn [_ _] (when (= 1 (swap! runs inc)) (throw (ex-info "the first time" {}))))
   :timeout-ms   5000
   :attempts     3})

(def ^:private count-done
  (cellar/make-query-from-sql-text "done jobs" "SELECT count(*) AS n FROM backroom.background_jobs WHERE background_job__state = 'done'"))

(defn- insert-done-background-jobs! [pool how-many]
  (run-sql! pool (str "INSERT INTO backroom.background_jobs (background_job__kind, background_job__params, background_job__state,"
                      " background_job__max_attempts, background_job__timeout_ms, background_job__finished_at)"
                      " SELECT 'test/fails-once', '{}', 'done', 3, 1000, now() - interval '1 second'"
                      " FROM generate_series(1, " how-many ")")))

(deftest the-settings-set-the-waits
  (if-not port
    (println "backroom.api-test: skipped, BACKROOM_TEST_PORT is not set")
    (let [pool (make-pool (System/getenv "BACKROOM_TEST_USER"))]
      (try
        (make-tables-afresh! pool)
        (reset! runs 0)
        (testing "a failure runs again after :retry-first-wait-ms"
          (let [workers (backroom/start-workers! pool (assoc settings :wire wire :log log :retry-first-wait-ms 100 :kinds [fails-once] :schedules []))]
            (try
              (backroom/add-background-job-to-queue! pool fails-once {} now)
              (wait-until #(= 1 (:n (cellar/find-one-row pool count-done {}))))
              (is (= 2 @runs))
              (is (= [["warn" "test/fails-once" {"background-job/attempt" 1 "background-job/retry-in-ms" 100} "the first time"]]
                     (mapv #(vector (get % "level") (get-in % ["context" "background-job/kind"]) (get % "data")
                                    (get-in % ["error" "message"]))
                           (find-lines "backroom/failed")))
                  "a failure that runs again is a warning, with the wait")
              (finally (backroom/stop-workers! workers)))))
        (testing "done background jobs older than :forget-done-after-ms are forgotten when the workers start"
          (reset! lines [])
          (backroom/stop-workers! (backroom/start-workers! pool (assoc settings :wire wire :log log :forget-done-after-ms 0 :kinds [] :schedules [])))
          (is (= 0 (:n (cellar/find-one-row pool count-done {}))))
          (is (= [["info" {"background-job/forgotten" 1 "background-job/forget-done-after-ms" 0}]]
                 (mapv #(vector (get % "level") (get % "data")) (find-lines "backroom/forgot-done")))
              "each round is a line, info when it forgot any"))
        (testing "and every :forget-done-every-ms while they run"
          (let [workers (backroom/start-workers! pool (assoc settings :wire wire :log log :forget-done-after-ms 0 :forget-done-every-ms 100
                                                             :kinds [] :schedules []))]
            (try
              (insert-done-background-jobs! pool 3)
              (is (= 3 (:n (cellar/find-one-row pool count-done {}))))
              (wait-until #(= 0 (:n (cellar/find-one-row pool count-done {}))))
              (is (= 0 (:n (cellar/find-one-row pool count-done {}))) "forgotten without a restart")
              (finally (backroom/stop-workers! workers)))))
        (testing "a background job left running is queued again past its time limit and :stuck-after-time-limit-ms"
          (run-sql! pool (str "INSERT INTO backroom.background_jobs (background_job__kind, background_job__params, background_job__state,"
                              " background_job__max_attempts, background_job__timeout_ms, background_job__locked_at)"
                              " VALUES ('test/fails-once', '{}', 'running', 3, 1000, now() - interval '2 seconds')"))
          (let [workers (backroom/start-workers! pool (assoc settings :wire wire :log log :stuck-after-time-limit-ms 0 :kinds [fails-once] :schedules []))]
            (try
              (wait-until #(= 1 (:n (cellar/find-one-row pool count-done {}))))
              (is (= 1 (:n (cellar/find-one-row pool count-done {}))))
              (finally (backroom/stop-workers! workers)))))
        (finally (cellar/stop-pool! pool))))))

(deftest several-servers-forget-at-once
  (if-not port
    (println "backroom.api-test: skipped, BACKROOM_TEST_PORT is not set")
    (let [pools (vec (repeatedly 3 #(make-pool (System/getenv "BACKROOM_TEST_USER"))))]
      (try
        (make-tables-afresh! (first pools))
        (insert-done-background-jobs! (first pools) 5000)
        (reset! lines [])
        (testing "three servers forgetting every 10 ms skip each other's rows: each row is deleted once, and none fails"
          (let [servers (mapv #(backroom/start-workers! % (assoc settings :wire wire :log log :forget-done-after-ms 0 :forget-done-every-ms 10
                                                                 :kinds [] :schedules []))
                              pools)]
            (try
              (wait-until #(= 0 (:n (cellar/find-one-row (first pools) count-done {}))))
              (finally (run! backroom/stop-workers! servers))))
          (is (= 0 (:n (cellar/find-one-row (first pools) count-done {}))))
          (is (= 5000 (reduce + (map #(get-in % ["data" "background-job/forgotten"]) (find-lines "backroom/forgot-done")))))
          (is (= [] (find-lines "backroom/forgetting-failed"))))
        (finally (run! cellar/stop-pool! pools))))))
