# Done background jobs are forgotten while the workers run, every server skipping the others' rows

> Changes in part [Every value that sets behaviour is a setting](2026-10-22-every-value-that-sets-behaviour-is-a-setting.md) (done background jobs were deleted only when the workers started) and [The app passes its log](2026-10-22-the-app-passes-its-log.md) (two lines more). Since v0.4.0, on cellar v0.3.0.

**Decision:** the workers delete the done background jobs older than `:forget-done-after-ms` when they start and then every `:forget-done-every-ms`, a new required setting (`[:int {:min 1}]`), until they stop. The deleting runs on its own virtual thread beside the dispatcher; `stop-workers!` wakes it and waits for it within the same `:stop-wait-ms`. Its first round always runs, so workers started and stopped at once still forget.

Every server does it. One statement a round, rows another server holds skipped:

```sql
DELETE FROM backroom.background_jobs
WHERE background_job__id IN (
  SELECT background_job__id FROM backroom.background_jobs
  WHERE background_job__state = 'done'
    AND background_job__finished_at < now() - CAST(:background-job/forget-done-after-ms AS bigint) * interval '1 millisecond'
  FOR UPDATE SKIP LOCKED)
```

Each round is a line on the workers' log:

| level | event | data |
|---|---|---|
| `:info` (any deleted) or `:debug` (none) | `:backroom/forgot-done` | `:background-job/forgotten` how many, `:background-job/forget-done-after-ms` |
| `:warn` | `:backroom/forgetting-failed` | `:error`: the round failed; the next tries again. Before, a failed delete at the start threw from `start-workers!` |

**The stuck background jobs, checked:** they did not wait for a start. The dispatcher queues again every background job left running past its time limit and `:stuck-after-time-limit-ms` on each round, every `:poll-ms`, on every server; that is unchanged.

**Why:** a server that runs for months never started its workers again, so done background jobs piled up for months, a table and its index growing without end. On its own thread, a delete over a large backlog (the first round after upgrading) holds up no claim. With `SKIP LOCKED` two servers never wait for each other: each deletes the rows it locked and skips the rest, which the other deletes; neither waits on a lock, so they can't deadlock (the tests: three servers every 10 ms over 5000 done rows; their counts sum to 5000, and no round fails).

**The price:** one more setting; every caller of v0.3 adds it. Every server runs the delete each round, though one would do. A round over a large backlog is one statement, as the start's was: a background job queued with a `:key` held by a done row being deleted waits for that statement.

**Instead of:**
- a plain `DELETE … WHERE` on every server: a server reaching a row another is deleting waits for that one's statement to end, and two deleting many rows in a different order can deadlock (Postgres ends one, a failed round)
- one server at a time, by an advisory lock: no doubled work, but one more mechanism, and a server holding the lock across a long delete keeps the others idle anyway
- in batches of n rows: shorter statements, but one more setting for a backlog only the first round meets
- the delete on the dispatcher's round, every `:poll-ms`: no new setting, but a delete each second on every server, and a long one holds up the claims
- a schedule (a cron line) that queues a forgetting background job: one server a time, but the app would have to add a kind and a schedule to keep the table from growing
