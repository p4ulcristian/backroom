# A job kind names its system actor, and its one transaction runs as it

> The user's decision, 2026-10-23, from Iron Rainbow's port (GROUNDWORK-GAPS.md, P6: every job kind and timed sweep). Not built yet.

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
