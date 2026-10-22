# backroom decisions

One file per decision: `YYYY-MM-DD-what-we-decided.md`. The title is the decision. Each file says what we decided, why, and what we chose against. A decision changes by a new file; the old one says so at the top (`Replaced by` or `Changed in part by`), and nothing else in it is rewritten.

The first two were made while backroom was a library inside Groundwork, a Clojure framework (groundwork.server.background-jobs, and the jobs of an older Groundwork before that); they name its parts (groundwork.sql, groundwork.schema) where they came from. The ones they build on stayed with Groundwork: "Groundwork's own tables are table maps, in their own folder", and the naming of the work: "The jobs library is named groundwork.background-jobs", "The background work is called chores", "The background work is called background jobs".

- [A job can give up for good](2026-10-12-a-job-can-give-up-for-good.md)
- [A job kind is a map, and the library makes its own tables](2026-10-22-a-job-kind-is-a-map-and-the-library-makes-its-own-tables.md), changed in part by Groundwork's "Groundwork's own tables are table maps, in their own folder" and "The background work is called background jobs", and by [backroom is its own library](2026-10-22-backroom-is-its-own-library.md)
- [backroom is its own library](2026-10-22-backroom-is-its-own-library.md)
