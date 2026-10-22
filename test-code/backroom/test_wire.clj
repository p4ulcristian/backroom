(ns backroom.test-wire
  "The tests' wire, made as an app makes its own: once, from wire's
   settings, every key written out (the values wire v0.1 had in effect)."
  (:require
   [wire.api :as wire]))

(def wire
  (wire/make-wire-from-settings
   {:most-shapes-kept 1000
    :json-limits      {:max-nesting-depth         1000
                       :max-number-length         1000
                       :max-string-length         20000000
                       :max-name-length           50000
                       :max-document-length       nil
                       :max-token-count           nil
                       :max-writing-nesting-depth 1000}}))
