(ns backroom.api-test
  "Runs against a real Postgres when BACKROOM_TEST_PORT is set (on
   localhost, user from BACKROOM_TEST_USER, no password, no TLS);
   says it was skipped otherwise."
  (:require
   [clojure.test :refer [deftest is testing]]
   [backroom.api :as backroom]
   [cellar.api :as cellar]))

(def ^:private port (some-> (System/getenv "BACKROOM_TEST_PORT") parse-long))

(defn- make-pool [user]
  (cellar/start-pool! {:host "localhost" :port port :database "postgres" :user user :password nil
                       :tls :off :tls-root-cert nil
                       :pool-size 4
                       :connection-wait-ms 30000 :connection-check-wait-ms 5000
                       :connection-max-lifetime-ms 1800000 :connection-keepalive-every-ms 120000
                       :leak-warning-after-ms :none
                       :connect-timeout-seconds 10 :read-timeout-seconds :none :tcp-keepalive? false
                       :statement-timeout-ms 5000 :lock-timeout-ms :none :idle-in-transaction-timeout-ms :none
                       :time-zone "UTC"}))

(def ^:private settings
  {:workers                   2
   :poll-ms                   100
   :retry-first-wait-ms       30000
   :retry-wait-factor         2
   :retry-max-wait-ms         3600000
   :stuck-after-time-limit-ms 60000
   :forget-done-after-ms      604800000
   :stop-wait-ms              5000})

(def ^:private now {:run-in-ms 0 :key nil})

(defn- run-sql! [pool text]
  (cellar/run-statement! pool (cellar/make-query-from-sql-text "test" text) {}))

(defn- make-tables-afresh! [pool]
  (with-out-str (run-sql! pool "DROP SCHEMA IF EXISTS backroom CASCADE; DROP SCHEMA IF EXISTS cellar CASCADE")
                @(:slow-part (cellar/migrate! pool backroom/background-jobs-tables []
                                              {:instant-part {:statement-timeout-ms 5000 :lock-timeout-ms 5000}
                                               :slow-part    {:statement-timeout-ms :none :lock-timeout-ms 5000}}))))

(def ^:private emails-sent (atom []))

(def ^:private send-invoice-email
  {:name         :test.mail/send-invoice-email
   :params-shape [:map [:order/id :int]]
   :run          (fn [_background-job-context params] (swap! emails-sent conj (:order/id params)))
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
        (let [workers (backroom/start-workers! pool (assoc settings :kinds [send-invoice-email give-up] :schedules []))]
          (try
            (testing "a queued background job runs, with its params as shaped data"
              (backroom/add-background-job-to-queue! pool send-invoice-email {:order/id 42} now)
              (wait-until #(seq @emails-sent))
              (is (= [42] @emails-sent)))
            (testing "params that don't fit the shape are refused"
              (is (thrown? clojure.lang.ExceptionInfo
                           (backroom/add-background-job-to-queue! pool send-invoice-email {:order/id "42"} now))))
            (testing "a background job that gives up is dead at once, with its message"
              (let [id (backroom/add-background-job-to-queue! pool give-up {} now)]
                (wait-until #(some (fn [background-job] (= id (:background-job/id background-job))) (backroom/list-dead-background-jobs pool 10)))
                (is (= "clojure.lang.ExceptionInfo: The order is gone"
                       (:background-job/last-error (first (filter #(= id (:background-job/id %)) (backroom/list-dead-background-jobs pool 10))))))))
            (testing "one background job per key"
              (is (= (backroom/add-background-job-to-queue! pool send-invoice-email {:order/id 1} {:run-in-ms 60000 :key "k"})
                     (backroom/add-background-job-to-queue! pool send-invoice-email {:order/id 1} {:run-in-ms 60000 :key "k"}))))
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
            (let [workers (backroom/start-workers! app-pool (assoc settings :workers 1 :kinds [send-invoice-email] :schedules []))]
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
    (let [{:keys [type errors]} (find-refusal #(backroom/start-workers! nil {:kinds [] :schedules [] :workers 1 :poll-ms 100}))]
      (is (= :backroom/bad-options type))
      (is (= #{:retry-first-wait-ms :retry-wait-factor :retry-max-wait-ms :stuck-after-time-limit-ms
               :forget-done-after-ms :stop-wait-ms}
             (set (keys errors))))))
  (testing "and an unknown one"
    (is (= {:type :backroom/bad-options :errors {:workerz ["disallowed key"]}}
           (find-refusal #(backroom/start-workers! nil (assoc settings :kinds [] :schedules [] :workerz 3))))))
  (testing "a kind writes out every key"
    (is (= {:type :backroom/bad-kind :errors {:attempts ["missing required key"]}}
           (find-refusal #(backroom/add-background-job-to-queue! nil (dissoc send-invoice-email :attempts) {:order/id 1} now)))))
  (testing "add-background-job-to-queue! says :run-in-ms and :key"
    (is (= {:type :backroom/bad-options :errors {:key ["missing required key"]}}
           (find-refusal #(backroom/add-background-job-to-queue! nil send-invoice-email {:order/id 1} {:run-in-ms 0}))))))

(def ^:private runs (atom 0))

(def ^:private fails-once
  {:name         :test/fails-once
   :params-shape [:map]
   :run          (fn [_ _] (when (= 1 (swap! runs inc)) (throw (ex-info "the first time" {}))))
   :timeout-ms   5000
   :attempts     3})

(def ^:private count-done
  (cellar/make-query-from-sql-text "done jobs" "SELECT count(*) AS n FROM backroom.background_jobs WHERE background_job__state = 'done'"))

(deftest the-settings-set-the-waits
  (if-not port
    (println "backroom.api-test: skipped, BACKROOM_TEST_PORT is not set")
    (let [pool (make-pool (System/getenv "BACKROOM_TEST_USER"))]
      (try
        (make-tables-afresh! pool)
        (reset! runs 0)
        (testing "a failure runs again after :retry-first-wait-ms"
          (let [workers (backroom/start-workers! pool (assoc settings :retry-first-wait-ms 100 :kinds [fails-once] :schedules []))]
            (try
              (backroom/add-background-job-to-queue! pool fails-once {} now)
              (wait-until #(= 1 (:n (cellar/find-one-row pool count-done {}))))
              (is (= 2 @runs))
              (finally (backroom/stop-workers! workers)))))
        (testing "done background jobs older than :forget-done-after-ms are forgotten when the workers start"
          (backroom/stop-workers! (backroom/start-workers! pool (assoc settings :forget-done-after-ms 0 :kinds [] :schedules [])))
          (is (= 0 (:n (cellar/find-one-row pool count-done {})))))
        (testing "a background job left running is queued again past its time limit and :stuck-after-time-limit-ms"
          (run-sql! pool (str "INSERT INTO backroom.background_jobs (background_job__kind, background_job__params, background_job__state,"
                              " background_job__max_attempts, background_job__timeout_ms, background_job__locked_at)"
                              " VALUES ('test/fails-once', '{}', 'running', 3, 1000, now() - interval '2 seconds')"))
          (let [workers (backroom/start-workers! pool (assoc settings :stuck-after-time-limit-ms 0 :kinds [fails-once] :schedules []))]
            (try
              (wait-until #(= 1 (:n (cellar/find-one-row pool count-done {}))))
              (is (= 1 (:n (cellar/find-one-row pool count-done {}))))
              (finally (backroom/stop-workers! workers)))))
        (finally (cellar/stop-pool! pool))))))
