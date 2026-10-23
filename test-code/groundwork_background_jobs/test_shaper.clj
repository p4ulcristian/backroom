(ns groundwork-background-jobs.test-shaper
  "The tests' groundwork-shaped-json, made as an app makes its own: once, from groundwork-shaped-json's
   settings, every key written out (the values groundwork-shaped-json v0.1 had in effect)."
  (:require
   [groundwork-shaped-json.api :as shaped-json]))

(def shaper
  (shaped-json/make-shaper-from-settings
   {:most-shapes-kept 1000
    :json-limits      {:max-nesting-depth         1000
                       :max-number-length         1000
                       :max-string-length         20000000
                       :max-name-length           50000
                       :max-document-length       nil
                       :max-token-count           nil
                       :max-writing-nesting-depth 1000}}))
