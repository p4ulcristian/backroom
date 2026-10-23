(ns groundwork-background-jobs.cron
  "Cron lines in UTC: minute hour day-of-month month day-of-week; *,
   numbers, lists a,b, ranges a-b, steps */n.

     (find-next-time-for-cron-line \"0 18 * * *\" now)   ; the next 18:00 UTC"
  (:require
   [clojure.string :as str])
  (:import
   (java.time Instant LocalDateTime ZoneOffset)
   (java.time.temporal ChronoUnit)))

(defn- field-values
  "The values a cron field allows, between lo and hi: *, n, a-b, each
   optionally /step, comma-separated."
  [field lo hi]
  (set (mapcat (fn [part]
                 (let [[span step] (str/split part #"/" 2)
                       step        (if step (parse-long step) 1)
                       [a b]       (cond
                                     (= "*" span)              [lo hi]
                                     (str/includes? span "-") (mapv parse-long (str/split span #"-" 2))
                                     :else                     (let [n (parse-long span)]
                                                                 [n (if (str/includes? part "/") hi n)]))]
                   (when-not (and a b step (<= lo a b hi) (pos? step))
                     (throw (ex-info (str "Not a cron field: " field) {:type :groundwork-background-jobs/bad-cron :field field})))
                   (range a (inc b) step)))
               (str/split field #","))))

(defn turn-cron-line-into-allowed-values
  "\"0 18 * * *\" → the allowed minutes, hours, days, months, weekdays.
   A line that isn't cron throws."
  [line]
  (let [[mi h dom mo dow :as fields] (str/split (str/trim line) #"\s+")]
    (when-not (= 5 (count fields))
      (throw (ex-info (str "A cron line has five fields: " line) {:type :groundwork-background-jobs/bad-cron :line line})))
    {:minutes  (field-values mi 0 59)
     :hours    (field-values h 0 23)
     :days     (field-values dom 1 31)
     :months   (field-values mo 1 12)
     :weekdays (set (map #(mod % 7) (field-values dow 0 7)))
     :any-day? (= "*" dom)
     :any-dow? (= "*" dow)}))

;; how many steps (a month, a day, an hour or a minute each) the search for
;; the next time takes before it says the line never comes: a fixed bound,
;; not a choice, years past any line that can come (29 February on a Monday)
(def ^:private most-steps 600000)

(defn find-next-time-for-cron-line
  "The first minute after `after` (an Instant) that a cron line allows, UTC."
  ^Instant [line ^Instant after]
  (let [{:keys [minutes hours days months weekdays any-day? any-dow?]} (turn-cron-line-into-allowed-values line)
        day-ok? (fn [^LocalDateTime t]
                  (let [dom? (contains? days (.getDayOfMonth t))
                        dow? (contains? weekdays (mod (.getValue (.getDayOfWeek t)) 7))]
                    (cond (and any-day? any-dow?) true
                          any-day?                dow?
                          any-dow?                dom?
                          :else                   (or dom? dow?))))]
    (loop [t (.plusMinutes (.truncatedTo (LocalDateTime/ofInstant after ZoneOffset/UTC) ChronoUnit/MINUTES) 1)
           n 0]
      (cond
        (> n most-steps)
        (throw (ex-info (str "This cron line never comes: " line) {:type :groundwork-background-jobs/bad-cron :line line}))

        (not (contains? months (.getMonthValue t)))
        (recur (.withDayOfMonth (.plusMonths (.truncatedTo t ChronoUnit/DAYS) 1) 1) (inc n))

        (not (day-ok? t))
        (recur (.plusDays (.truncatedTo t ChronoUnit/DAYS) 1) (inc n))

        (not (contains? hours (.getHour t)))
        (recur (.plusHours (.truncatedTo t ChronoUnit/HOURS) 1) (inc n))

        (not (contains? minutes (.getMinute t)))
        (recur (.plusMinutes t 1) (inc n))

        :else
        (.toInstant t ZoneOffset/UTC)))))
