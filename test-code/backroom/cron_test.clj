(ns backroom.cron-test
  (:require
   [clojure.test :refer [deftest is]]
   [backroom.cron :as cron])
  (:import
   (java.time Instant)))

(deftest the-next-time-a-cron-line-allows
  (is (= (Instant/parse "2026-10-22T18:00:00Z")
         (cron/find-next-time-for-cron-line "0 18 * * *" (Instant/parse "2026-10-22T10:00:00Z"))))
  (is (= (Instant/parse "2026-10-23T01:00:00Z")
         (cron/find-next-time-for-cron-line "0 1 * * *" (Instant/parse "2026-10-22T10:00:00Z"))))
  (is (thrown? clojure.lang.ExceptionInfo (cron/turn-cron-line-into-allowed-values "every day"))))
