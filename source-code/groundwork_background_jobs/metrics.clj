(ns groundwork-background-jobs.metrics
  "groundwork-background-jobs's metrics, declared on the app's meter by start-workers!:

     groundwork_background_jobs_jobs_added_total{kind}           counter: background jobs added to the queue
     groundwork_background_jobs_job_runs_total{kind,outcome}     counter: runs, by how they ended
     groundwork_background_jobs_job_run_seconds{kind}            histogram: how long a run took
     groundwork_background_jobs_jobs_running{kind}               gauge: runs going on now, on this server
     groundwork_background_jobs_dispatcher_failures_total        counter: dispatcher rounds that failed

   The kinds' set is start-workers!' :kinds; a kind outside it (one another
   server runs) is counted as \"other\"."
  (:require
   [groundwork-metrics.api :as metrics]))

(def outcomes
  "How a run ended:
   :done              its :run returned, and it is saved done
   :failed            it threw or ran out of time, and runs again later
   :gave-up           it threw or ran out of time on its last attempt, or gave up: dead
   :outcome-not-saved its outcome couldn't be saved; it is queued again once stuck"
  #{:done :failed :gave-up :outcome-not-saved})

(defn declare-metrics!
  "Declares groundwork-background-jobs's metrics on the meter: the kinds' set is the
   options' :kinds, the run time's buckets the options' :metric-buckets."
  [meter {:keys [kinds] {:keys [job-run-seconds]} :metric-buckets}]
  (let [kind-names (set (map :name kinds))]
    (metrics/declare-metrics!
     meter
     [{:name    :groundwork-background-jobs/jobs-added-total
       :type    :counter
       :help    "Background jobs added to the queue, by kind (one added in a transaction undone later too)."
       :labels  {:kind kind-names}
       :buckets nil
       :read    nil}
      {:name    :groundwork-background-jobs/job-runs-total
       :type    :counter
       :help    "Background job runs on this server, by kind and how they ended."
       :labels  {:kind kind-names :outcome outcomes}
       :buckets nil
       :read    nil}
      {:name    :groundwork-background-jobs/job-run-seconds
       :type    :histogram
       :help    "Time a background job's run took, in seconds, by kind (a run out of time counts its time limit)."
       :labels  {:kind kind-names}
       :buckets job-run-seconds
       :read    nil}
      {:name    :groundwork-background-jobs/jobs-running
       :type    :gauge
       :help    "Background jobs running on this server now, by kind."
       :labels  {:kind kind-names}
       :buckets nil
       :read    nil}
      {:name    :groundwork-background-jobs/dispatcher-failures-total
       :type    :counter
       :help    "Rounds of the dispatcher that failed (written to the log as :groundwork-background-jobs/dispatcher-failed)."
       :labels  {}
       :buckets nil
       :read    nil}])))

(defn count-added! [meter kind-name]
  (metrics/add-to-counter! meter :groundwork-background-jobs/jobs-added-total {:kind kind-name} 1))

(defn count-run! [meter kind-name outcome]
  (metrics/add-to-counter! meter :groundwork-background-jobs/job-runs-total {:kind kind-name :outcome outcome} 1))

(defn record-run-time! [meter kind-name started-nanos]
  (metrics/record-in-histogram! meter :groundwork-background-jobs/job-run-seconds {:kind kind-name}
                              (/ (- (System/nanoTime) started-nanos) 1e9)))

(defn add-to-running! [meter kind-name amount]
  (metrics/add-to-gauge! meter :groundwork-background-jobs/jobs-running {:kind kind-name} amount))

(defn count-dispatcher-failure! [meter]
  (metrics/add-to-counter! meter :groundwork-background-jobs/dispatcher-failures-total {} 1))
