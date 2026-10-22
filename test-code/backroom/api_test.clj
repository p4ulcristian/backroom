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
                       :tls :off :pool-size 4 :statement-timeout-ms 5000 :time-zone "UTC"}))

(defn- run-sql! [pool text]
  (cellar/run-statement! pool (cellar/make-query-from-sql-text "test" text) {}))

(defn- make-tables-afresh! [pool]
  (with-out-str (run-sql! pool "DROP SCHEMA IF EXISTS backroom CASCADE; DROP SCHEMA IF EXISTS cellar CASCADE")
                @(:slow-part (cellar/migrate! pool backroom/background-jobs-tables []))))

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
   :run          (fn [_ _] (throw (backroom/make-give-up-exception "The order is gone")))
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
        (let [workers (backroom/start-workers! pool {:kinds [send-invoice-email give-up] :schedules []
                                                     :workers 2 :poll-ms 100})]
          (try
            (testing "a queued background job runs, with its params as shaped data"
              (backroom/add-background-job-to-queue! pool send-invoice-email {:order/id 42})
              (wait-until #(seq @emails-sent))
              (is (= [42] @emails-sent)))
            (testing "params that don't fit the shape are refused"
              (is (thrown? clojure.lang.ExceptionInfo
                           (backroom/add-background-job-to-queue! pool send-invoice-email {:order/id "42"}))))
            (testing "a background job that gives up is dead at once, with its message"
              (let [id (backroom/add-background-job-to-queue! pool give-up {})]
                (wait-until #(some (fn [background-job] (= id (:background-job/id background-job))) (backroom/list-dead-background-jobs pool 10)))
                (is (= "clojure.lang.ExceptionInfo: The order is gone"
                       (:background-job/last-error (first (filter #(= id (:background-job/id %)) (backroom/list-dead-background-jobs pool 10))))))))
            (testing "one background job per key"
              (is (= (backroom/add-background-job-to-queue! pool send-invoice-email {:order/id 1} {:key "k" :run-in-ms 60000})
                     (backroom/add-background-job-to-queue! pool send-invoice-email {:order/id 1} {:key "k" :run-in-ms 60000}))))
            (finally (backroom/stop-workers! workers))))
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
            (let [workers (backroom/start-workers! app-pool {:kinds [send-invoice-email] :schedules []
                                                             :workers 1 :poll-ms 100})]
              (try
                (testing "the app's role, granted backroom's tables by cellar, queues a background job and a worker claims and runs it"
                  (backroom/add-background-job-to-queue! app-pool send-invoice-email {:order/id 7})
                  (wait-until #(seq @emails-sent))
                  (is (= [7] @emails-sent))
                  (is (= [] (backroom/list-queued-background-jobs app-pool 10))))
                (finally (backroom/stop-workers! workers))))
            (finally (cellar/stop-pool! app-pool))))
        (finally (cellar/stop-pool! owner-pool))))))
