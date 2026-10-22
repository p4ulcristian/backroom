# backroom is its own library

> Changes in part [A job kind is a map, and the library makes its own tables](2026-10-22-a-job-kind-is-a-map-and-the-library-makes-its-own-tables.md): its names.

**Decision:** backroom is a library of its own, in its own public repository, under the MIT license; an app or a framework takes it from git by tag. It was Groundwork's library `groundwork.server.background-jobs`; now it is the namespace `backroom`, with one public namespace, `backroom.api`, and its part `backroom.cron`. Its `.sql` files are on the classpath under `backroom/`. Its own `deps.edn` names only cellar and wire, by tag, and its tests run with nothing else on the classpath. It knows nothing of who uses it:

- its tables are in their own folder, `backroom.background_jobs` and `backroom.background_job_schedules` (`groundwork.…` before), their keys `:backroom/background-jobs` and `:backroom/background-job-schedules`; `cellar/grant-tables-to-role!`, given them, lets the app's own role, which owns nothing, use them;
- its table maps say only what cellar reads; what a framework on top needs on them (Groundwork's `:sync`) it adds itself;
- what it throws is typed `:backroom/…` (`:backroom/give-up`, `:backroom/bad-params`, `:backroom/bad-cron`), and it logs as `backroom`.

Only the library is called backroom. A unit of work stays a **background job**, never "job" alone, in every name: `:background-job/…`, `background_job__…`, `add-background-job-to-queue!` (decided in Groundwork, "The background work is called background jobs").

**Why:** a queue, workers and schedules in Postgres are one idea any Clojure app on Postgres has, and the library needed nothing of Groundwork but cellar and wire. Its own repository, with a `deps.edn` naming only those, proves it, and lets an app take it alone. No deployed database had `groundwork.background_jobs` yet, so the new names cost nothing. The name: the room at the back where work gets done out of sight, next to cellar.

**The price:** a change to backroom is a new tag, and the app moves to it on purpose. The decisions it was built on that are about Groundwork as a whole, and the naming of a background job, stayed with Groundwork.

**Instead of:**
- staying a folder in Groundwork's tree: no app can take it alone, and nothing proves its deps
- a name under `groundwork`: backroom isn't Groundwork's
- the names errand, conveyor and background-jobs: backroom was chosen over them
