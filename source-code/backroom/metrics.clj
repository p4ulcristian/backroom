(ns backroom.metrics
  "backroom's metrics, declared on the app's meter by start-workers!:

     backroom_jobs_added_total{kind}           counter: background jobs added to the queue
     backroom_job_runs_total{kind,outcome}     counter: runs, by how they ended
     backroom_job_run_seconds{kind}            histogram: how long a run took
     backroom_jobs_running{kind}               gauge: runs going on now, on this server
     backroom_dispatcher_failures_total        counter: dispatcher rounds that failed

   The kinds' set is start-workers!' :kinds; a kind outside it (one another
   server runs) is counted as \"other\"."
  (:require
   [meter.api :as meter]))

(def outcomes
  "How a run ended:
   :done              its :run returned, and it is saved done
   :failed            it threw or ran out of time, and runs again later
   :gave-up           it threw or ran out of time on its last attempt, or gave up: dead
   :outcome-not-saved its outcome couldn't be saved; it is queued again once stuck"
  #{:done :failed :gave-up :outcome-not-saved})

(defn declare-metrics!
  "Declares backroom's metrics on the meter: the kinds' set is the
   options' :kinds, the run time's buckets the options' :metric-buckets."
  [meter {:keys [kinds] {:keys [job-run-seconds]} :metric-buckets}]
  (let [kind-names (set (map :name kinds))]
    (meter/declare-metrics!
     meter
     [{:name    :backroom/jobs-added-total
       :type    :counter
       :help    "Background jobs added to the queue, by kind (one added in a transaction undone later too)."
       :labels  {:kind kind-names}
       :buckets nil
       :read    nil}
      {:name    :backroom/job-runs-total
       :type    :counter
       :help    "Background job runs on this server, by kind and how they ended."
       :labels  {:kind kind-names :outcome outcomes}
       :buckets nil
       :read    nil}
      {:name    :backroom/job-run-seconds
       :type    :histogram
       :help    "Time a background job's run took, in seconds, by kind (a run out of time counts its time limit)."
       :labels  {:kind kind-names}
       :buckets job-run-seconds
       :read    nil}
      {:name    :backroom/jobs-running
       :type    :gauge
       :help    "Background jobs running on this server now, by kind."
       :labels  {:kind kind-names}
       :buckets nil
       :read    nil}
      {:name    :backroom/dispatcher-failures-total
       :type    :counter
       :help    "Rounds of the dispatcher that failed (written to the log as :backroom/dispatcher-failed)."
       :labels  {}
       :buckets nil
       :read    nil}])))

(defn count-added! [meter kind-name]
  (meter/add-to-counter! meter :backroom/jobs-added-total {:kind kind-name} 1))

(defn count-run! [meter kind-name outcome]
  (meter/add-to-counter! meter :backroom/job-runs-total {:kind kind-name :outcome outcome} 1))

(defn record-run-time! [meter kind-name started-nanos]
  (meter/record-in-histogram! meter :backroom/job-run-seconds {:kind kind-name}
                              (/ (- (System/nanoTime) started-nanos) 1e9)))

(defn add-to-running! [meter kind-name amount]
  (meter/add-to-gauge! meter :backroom/jobs-running {:kind kind-name} amount))

(defn count-dispatcher-failure! [meter]
  (meter/add-to-counter! meter :backroom/dispatcher-failures-total {} 1))
