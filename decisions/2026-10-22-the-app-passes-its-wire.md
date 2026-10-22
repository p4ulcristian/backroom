# The app passes its wire; backroom never makes one

> Changes in part [Every value that sets behaviour is a setting; no hidden defaults](2026-10-22-every-value-that-sets-behaviour-is-a-setting.md): two options more, and the checks are malli's own. Since v0.2.0, on wire v0.2.0.

**Decision:** wire v0.2.0 makes a wire from settings, and every one of its functions takes the wire first. The app makes its wire once and passes the same value to every function of backroom's that keeps or reads a background job's params, as a required key, `:wire`, in that function's options; backroom never makes a wire and carries no wire settings.

- `add-background-job-to-queue!`: `{:wire wire :run-in-ms 0 :key nil}`; the params go into the queue as JSON text by it.
- `start-workers!`: `{:wire wire :kinds … :schedules … :workers … …}`; the workers read every claimed background job's params by it, and a schedule's params (its key, and the background jobs it queues) go through it too.

The key is checked by malli with the rest of the options (`[:wire some?]`; wire has no predicate for a made wire): a missing one refuses, `:backroom/bad-options`, naming `:wire`.

The settings, the options and the kinds are checked by malli itself, as cellar and showroom check theirs, not through wire's `find-errors-in-data-for-shape`: they are values the app hands in, not data crossing an edge. The error texts are malli's, which wire's were for these shapes.

```clojure
(def wire (wire.api/make-wire-from-settings
           {:most-shapes-kept 1000
            :json-limits      {:max-nesting-depth 1000 :max-number-length 1000 :max-string-length 20000000
                               :max-name-length 50000 :max-document-length nil :max-token-count nil
                               :max-writing-nesting-depth 1000}}))

(backroom/add-background-job-to-queue! transaction send-invoice-email {:order/id id} {:wire wire :run-in-ms 0 :key nil})
(backroom/start-workers! pool (assoc settings :wire wire :kinds kinds :schedules schedules))
```

**Why:** a wire is settings (Jackson's limits, how many shapes are kept) and what it keeps (each shape's tools). Made once by the app, one value is in effect for the browser's messages and the queue alike, and the app's settings show it. A library that made its own would hide those settings again and keep its own tools beside the app's. In the options, beside the other values each function requires, it is written out like them and refused like them.

**The price:** every queueing call writes `:wire`, so the code that queues (a handler's step) needs the app's wire at hand. Every caller of v0.1 changes.

**Instead of:**
- a wire made inside backroom from settings of its own: two wires in one app, settings in two places
- the wire as a first argument, as wire's own functions take it: unlike every other value backroom requires, which are named options
- the wire in backroom's settings: the settings are values an app writes in its config; a wire is made from values, and `settings-shape` is a part of the app's settings
- checking the settings through wire: every function would take a wire
