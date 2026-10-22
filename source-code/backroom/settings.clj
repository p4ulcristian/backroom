(ns backroom.settings
  "backroom's settings, and the data its functions take, as malli shapes,
   checked before anything runs. Nothing has a default: every key is
   written out (nil where it is a choice), and a missing or unknown one
   refuses, named, with every other problem at once. They are checked by
   malli itself, not through wire: they are values the app hands in, not
   data crossing an edge. The functions that keep or read a background
   job's params take the app's wire under :wire in their options; backroom
   never makes one. start-workers! takes the app's log under :log; backroom
   never decides how a line is written."
  (:require
   [malli.core :as malli]
   [malli.error :as malli-error]))

(def ^:private settings-entries
  [[:workers [:int {:min 1}]]
   [:poll-ms [:int {:min 1}]]
   [:retry-first-wait-ms [:int {:min 0}]]
   [:retry-wait-factor [:and number? [:>= 1]]]
   [:retry-max-wait-ms [:int {:min 0}]]
   [:stuck-after-time-limit-ms [:int {:min 0}]]
   [:forget-done-after-ms [:int {:min 0}]]
   [:stop-wait-ms [:int {:min 0}]]])

(def settings-shape
  "The part of an app's settings that is backroom's, every key required.
   :workers: background jobs running at once on this server.
   :poll-ms: how often the workers look for due background jobs (and due
   schedules, and stuck background jobs).
   :retry-first-wait-ms, :retry-wait-factor, :retry-max-wait-ms: a failed
   background job runs again after the first wait, each later failure
   waiting factor times longer, never longer than the max (30000, 2,
   3600000: 30 s, 1 min, 2 min… up to an hour).
   :stuck-after-time-limit-ms: a background job left running (its server
   stopped mid-run) is queued again once its own :timeout-ms and this much
   more have passed.
   :forget-done-after-ms: done background jobs older than this are deleted
   when the workers start.
   :stop-wait-ms: how long stop-workers! waits for the running background
   jobs to finish."
  (into [:map {:closed true}] settings-entries))

(def kind-shape
  "A background job kind, every key required. :name, a namespaced keyword;
   :params-shape, the malli shape its params fit; :run, (fn [context
   params]), the context {:transaction :background-job-id :attempt :log},
   :log the workers' log with the background job's id and kind as its
   context; :timeout-ms, how long one run may take; :attempts, how many
   runs it gets before it is dead."
  [:map {:closed true}
   [:name :qualified-keyword]
   [:params-shape :any]
   [:run fn?]
   [:timeout-ms [:int {:min 1}]]
   [:attempts [:int {:min 1}]]])

(def schedule-shape
  "A schedule: a kind, its params, and a cron line in UTC."
  [:map {:closed true}
   [:kind kind-shape]
   [:params :any]
   [:cron :string]])

(def wire-entry
  "The app's wire (wire.api's make-wire-from-settings), made once by the
   app: a background job's params go into the queue as JSON text by it and
   come out by it."
  [:wire [some? {:error/message "should be the app's wire, made by wire.api/make-wire-from-settings"}]])

(def log-entry
  "The app's log function, (log level event data), made by logbook's
   log-function or with-context: a failure, a background job given up and
   a dispatcher that failed are written to it, and each run gets one with
   its background job's id and kind as context."
  [:log [fn? {:error/message "should be the app's log function, made by logbook"}]])

(def workers-options-shape
  "start-workers!'s options: the app's wire and log, the settings, the
   kinds this server runs and the schedules, every key required."
  (into [:map {:closed true}
         wire-entry
         log-entry
         [:kinds [:sequential kind-shape]]
         [:schedules [:sequential schedule-shape]]]
        settings-entries))

(def queue-options-shape
  "add-background-job-to-queue!'s options, every key required.
   :wire: the app's wire, which the params are kept by.
   :run-in-ms: run no sooner than this, 0 for now.
   :key: nil, or text: one background job of this key for as long as it is
   remembered; a second one queues nothing and returns the first one's id."
  [:map {:closed true}
   wire-entry
   [:run-in-ms [:int {:min 0}]]
   [:key [:maybe :string]]])

(defn check-data-for-shape
  "Returns the data; throws {:type kind :errors …} with every problem at
   once when it doesn't fit the shape."
  [what kind shape data]
  (when-let [explanation (malli/explain shape data)]
    (let [errors (malli-error/humanize explanation)]
      (throw (ex-info (str what " have problems: " (pr-str errors))
                      {:type kind :errors errors}))))
  data)

(defn check-workers-options [options]
  (check-data-for-shape "The options" :backroom/bad-options workers-options-shape options))

(defn check-kind [kind]
  (check-data-for-shape "The kind's keys" :backroom/bad-kind kind-shape kind))

(defn check-queue-options [options]
  (check-data-for-shape "The options" :backroom/bad-options queue-options-shape options))
