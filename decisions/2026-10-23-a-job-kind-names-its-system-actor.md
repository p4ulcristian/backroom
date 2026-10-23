# A job kind names its system actor, and its one transaction runs as it

> The user's decision, 2026-10-23, from Iron Rainbow's port (GROUNDWORK-GAPS.md, P6: every job kind and timed sweep). Built on tidy-up, not released.

**What was wrong:** a job runs later, with nobody asking (send order #1042's confirmation email). backroom ran its `:run` in a transaction that told Postgres no asker, so the gates showed it no gated row; Iron Rainbow wraps every job in `with-system`, which reaches everything.

**Decision:** every job kind names its system actor, a required key:

```clojure
{:name         :orders/send-confirmation-email
 :system-actor :order-emails          ; doorman's named actor: its sentences say what it reaches
 :params-shape [:map [:order/id :uuid]]
 :run          (fn [transaction params] …)
 :timeout-ms   30000
 :attempts     5}
```

backroom opens the job's one transaction through a function the app (the composer) hands it, which is doorman's `run-as-system!` for that actor: the work and the job's done-mark commit together. backroom itself knows no doorman: the opener is a required setting.

**Why:** a job reaches exactly what its actor's sentences give it, and `doorman/find-system-leaks :order-emails` lists every row it could reach. One transaction keeps "the work committed" and "the job is done" one fact.

**The price:** every job kind gains a key; the composer passes the opener; an app writes a system actor (and its sentences) per kind of background work.

**Instead of:**
- **the job runs as whoever queued it** (person, action and shop stored on the job): a sweep has nobody, and a member removed after queueing would still act
- **`:run` opens its own `run-as-system!` on the pool**: two transactions, so the work can commit and the job still retry

## Settled while building

Choices made while building, not asked:

1. **The opener is `:run-in-transaction-as`**, a required option of `start-workers!` beside `:wire`, `:log` and `:meter`: `(fn [pool system-actor f])`, which runs `(f transaction)` in one transaction of its own as the actor and returns what `f` returns. groundwork passes doorman's `run-as-system!` with `{:read-only? false}`.
2. **`:system-actor` is a keyword without a namespace** (`:order-emails`), as doorman names its actors; backroom checks only that, doorman refuses any other name when it opens the transaction.
3. **The done-mark moved into the job's transaction,** after `:run`: before, the work committed in one transaction and the job was marked done on the pool after it. The mark holds only while this run still holds the job (`running`, at this attempt); a run past its time limit, already marked failed or queued again, marks nothing, throws `:backroom/run-overtaken`, and its work is rolled back with it.
4. **The failure mark stays outside the transaction,** on the pool, as before: a transaction that doesn't commit (the work threw, the commit failed) leaves the job running, and the failure mark queues it again or makes it dead.
5. **A schedule's queueing stays on the pool,** in backroom's own transaction: it writes only backroom's tables, and asks as nobody.
