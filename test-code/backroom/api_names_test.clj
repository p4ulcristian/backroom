(ns backroom.api-names-test
  "The public names of backroom.api. Adding or removing one fails here, so
   the api changes on purpose."
  (:require
   [clojure.test :refer [deftest is]]
   [backroom.api]))

(deftest the-api-names
  (is (= '#{settings-shape
           background-jobs-table
           background-job-schedules-table
           background-jobs-tables
           add-background-job-to-queue!
           make-give-up-exception
           start-workers!
           stop-workers!
           list-queued-background-jobs
           list-dead-background-jobs
           run-background-job-again-now!}
         (set (keys (ns-publics 'backroom.api))))))
