# From stress flakes to actors: notes for an engineering post

Draft source material (not a polished blog post). Captures the debugging path,
the concurrency bug, the shared-state smell, and the actor redesign on Volga’s
master control plane. Update facts/links as PRs merge.

**Rough timeframe:** Aug 2026  
**Primary PR:** [#195](https://github.com/volga-project/volga/pull/195) — ExecutionAttempt actor + StopSources Drain  
**Related:** [#193](https://github.com/volga-project/volga/pull/193) Worker mutex, [#194](https://github.com/volga-project/volga/pull/194) CheckpointCoordinator, [#191](https://github.com/volga-project/volga/issues/191) / [#192](https://github.com/volga-project/volga/issues/192) follow-ups  
**Signature test:** `test_local_multi_worker_window_checkpoint_restore`

---

## 1. One-line story

We built CI stress tooling that could burn thousands of in-process checkpoint/
restore runs. That surface found a rare hang: cooperative drain (`StopSources`)
racing the master’s checkpoint/poll loop. The first fix shared attempt phase
across gRPC and the run loop; the durable fix was to stop co-owning control
state via locks and move ownership into kameo actors (attempt → sessions, with
checkpoint/worker/master plane still stacking).

---

## 2. Background: what Volga’s master was doing

Volga is a Rust streaming runtime. The **master** schedules workers, runs an
execution attempt loop (poll worker state, interval checkpoints, failure
aggregation / recovery), and exposes gRPC including **StopSources** — cooperative
finish: stop sources, drain the graph, wait for `PipelineFinished`.

Rough control flow before the redesign:

```text
MasterLifecycle::run
  └─ ExecutionAttempt (stack-local struct)
        select! { failure | checkpoint tick | poll tick }
        WorkerClient map + heartbeats → failure mpsc

MasterState (Arc + mutex bag)
  checkpoints, workers registry, lifecycle journal, …
  ← also poked from Master gRPC handlers (acks, StopSources, …)
```

The attempt’s run loop lived in one async task. Checkpoint and poll were arms of
a `tokio::select!`. Lifecycle waited on that loop for `Finished` / `Recover`.

---

## 3. The tooling that made the bug findable

### 3.1 Unified test runner

`scripts/test` wraps nextest profiles (`unit`, `inprocess`, `default`, `docker`,
`kube`, `stress`) so host and CI share filters, jobs, and env setup.
Documented in `scripts/README.md`.

### 3.2 Stress profile

`scripts/test stress --env local|kube` repeats a test (or suite) across shards,
writes per-shard logs under `target/stress/`, fails fast on first failure.

### 3.3 CI: machines × shards × runs

GitHub Actions `workflow_dispatch` / schedule on `rust-tests.yml`:

| Knob | Meaning |
| --- | --- |
| `stress_machines` | Isolated GitHub runners |
| `stress_shards_per_machine` | Parallel stress processes per runner (prefer 1) |
| `stress_runs_per_shard` | Iterations per process |
| `stress_test` | Exact test name (inproc) |

Scheduled shape that mattered for this bug:

- **Inproc:** 15 × 1 × 100 of `test_local_multi_worker_window_checkpoint_restore`
- **Kube:** 15 × 1 × 10 of the kube suite (alternating schedule)

On-demand example:

```bash
gh workflow run rust-tests.yml --ref split/drain-run-modes \
  -f profile=inproc-stress \
  -f stress_test=test_local_multi_worker_window_checkpoint_restore \
  -f stress_machines=15 \
  -f stress_shards_per_machine=1 \
  -f stress_runs_per_shard=100
```

Artifacts upload `target/stress/` per machine — essential when only 1/1500 runs
fails.

### 3.4 Why this test

`test_local_multi_worker_window_checkpoint_restore`:

- Multi-worker window pipeline
- Interval checkpoints
- Abrupt worker kill after a completed checkpoint
- Restore on a new attempt
- Harness `StopSources` → wait `PipelineFinished`
- Sink/oracle correctness (key-set / aggregates)

It couples **recovery**, **in-flight checkpoints**, and **cooperative drain** —
exactly the race surface.

---

## 4. The bug: PipelineFinished hang (and friends)

### 4.1 Symptom

Under stress (and occasionally in default CI), the harness waited on
`PipelineFinished` (or checkpoint settle) until timeout. Not a clean assert
failure — a **stuck control plane**.

### 4.2 Mechanism (simplified)

Two interacting facts:

1. **`StopSources` needs to abort in-flight checkpoints and prevent new ones**
   before/while sources stop, or the drain can wait on a checkpoint that will
   never complete (killed worker, fenced attempt, etc.).

2. **The run loop’s `select!` + RPC placement mattered.**
   - Slow `get_worker_state` under `biased` select could starve the checkpoint
     arm if ordered wrong.
   - More importantly for drain: if “phase / abort” lived only inside the run
     loop, a gRPC `StopSources` handler on another task could not safely
     coordinate without shared mutable state — and if abort was delayed behind
     a long in-loop RPC, settle/finish timed out.

A common timeline:

```text
interval CP N started (barriers in flight)
test kills a worker  OR  harness calls StopSources
poll / barrier RPCs slow or stuck
CP never Completes/Fails in the lifecycle journal
harness: wait_until_checkpoints_idle → timeout
  or StopSources without abort → PipelineFinished never arrives
```

### 4.3 First fix direction: shared `AttemptPhase`

Introduce `Running | Checkpointing | Draining` visible to both:

- run loop (gate checkpoint arm; clear checkpointing when idle)
- `StopSources` (set `Draining`, abort in-flight CP **before** worker fan-out)

Also: keep poll/checkpoint RPCs outside cancellation races; gate checkpoint
starts when draining; unify “all tasks in status” helpers.

This fixed the hang class but left a design smell: **control state co-owned via
atomics/mutexes on `MasterState` while the attempt was still a stack-local
struct.**

### 4.4 Stack / merge chaos (meta-lesson)

While landing this work:

- An earlier drain PR was merged into the wrong base and could not be reopened.
- gRPC centralization (#181) had been merged into a WM branch **after** WM hit
  `master`, so a drain-on-master branch temporarily carried cherry-picked gRPC
  commits. Fix: land gRPC on `master` ([#196](https://github.com/volga-project/volga/pull/196)), rebase drain so the PR diff is only the feature.

Worth a sidebar in a post: stress + stacked PRs needs ruthless base hygiene.

---

## 5. The smell: shared state vs single owner

Recurring pattern across master and worker:

| Smell | Example |
| --- | --- |
| Mutex held across `.await` / actor `ask` | Worker `Mutex` around task actor asks (#193) |
| Phase/flags on a shared bag so “someone else” can poke | `AttemptPhase` on `MasterState` |
| Stack-local owner + remote mutator | `ExecutionAttempt` in lifecycle vs gRPC `StopSources` |
| Protocol + store behind `Mutex` with “pending persist” dance | Checkpoints before #194 |

**Actor rule of thumb we converged on:**

- If it owns durable **control state** → actor (or clear single-threaded owner).
- If it only wakes the owner (timer, channel) → task/`tell`, not a domain actor.
- Prefer **message + ownership** over **lock + poke**.

Half-measure rejected: tiny mpsc “mailbox” beside kameo for drain only.

---

## 6. Redesign path (what we actually built)

### 6.1 Target shape (master attempt plane)

```text
MasterLifecycle
  └─ ask Schedule / Run / Finish / Recover
        ExecutionAttempt (kameo)          ← owns AttemptPhase, run outcome
              ├─ WorkerSession(w1)       ← client, heartbeat, RPCs
              ├─ WorkerSession(w2)
              └─ run_loop task           ← timers + failure_rx → tell ticks

Master gRPC StopSources
  → MasterState.current_attempt: ActorRef
  → ask Drain  (phase=Draining, abort CP, session stop_sources)

MasterState
  → still a mutex bag for registry/journal/config (see #192)
  → checkpoints → CheckpointCoordinator actor on #194
```

### 6.2 ExecutionAttempt as actor (#195)

**Messages (public):** `Schedule`, `Run`, `Drain`, `Finish`, `Recover`  
**Internal:** `PollTick`, `CheckpointTick`, `FailureMsg`, `AggWindowEnd`,
`PollResult`, `CheckpointBarriersDone`

**Why `Run` is `DelegatedReply` + run_loop task (not a multi-minute `handle(Run)`):**

Kameo processes one message at a time. A long `select!` inside `handle(Run)`
would block `Drain` for the whole attempt. So:

1. `handle(Run)` stores `ReplySender`, spawns `run_loop`, returns `DelegatedReply`.
2. `run_loop` only turns wall-clock / failure channel into `tell`s.
3. Real work runs in short handlers; `complete_run` aborts the loop and sends
   the deferred reply (`Finished` / `Recover`).

**Phase lives only on the attempt actor.** `MasterState` holds
`Option<ActorRef<ExecutionAttempt>>`, not the phase enum.

### 6.3 WorkerSession actors (#195)

One session per worker for the attempt:

- Owns `WorkerClient` + heartbeat
- Messages: configure/start/run, `GetWorkerState`, `TriggerBarrier`,
  `StopSources`, reset/close/shutdown

Attempt orchestrates with `ask` / fan-out; **does not** hold clients directly.

### 6.4 Why spawn still wraps session fan-out

Even with sessions, this blocks the attempt mailbox:

```rust
// inside handle(PollTick) — BAD for Drain latency
join_all(sessions.ask(GetWorkerState)).await
```

So poll/barrier fan-out is still:

```text
handle(PollTick) → spawn { ask sessions; tell PollResult }
handle(CheckpointTick) → begin CP + set Checkpointing → spawn barriers
                       → tell CheckpointBarriersDone
```

Sessions own I/O concurrency; spawn keeps **awaiting** off the decision actor.
Same idea as run_loop: wake/collect off-mailbox, decide on-mailbox.

### 6.5 FIFO mailbox realism (do we need redesign?)

Default kameo mailbox is **bounded (64), FIFO**. `tell` does not mean
“runs immediately” — only “enqueued (or waits for capacity).”

For Drain correctness we concluded **no redesign required**:

- `Drain` sets `Draining` and aborts CP **before** awaiting `stop_sources`.
- Tick handlers are short or early-return when not `Running` / when `poll_in_flight`.
- Stale `PollResult` / `BarriersDone` guarded by `run.is_none()` / phase.

Optional hardening later (not topology change): ignore poll results when
`Draining`; coalesce ticks. Priority mailboxes only if stress shows Drain
latency problems.

### 6.6 Stacked follow-ups

| Work | Role |
| --- | --- |
| [#193](https://github.com/volga-project/volga/pull/193) | Worker: `Arc` + sync inner mutex; no tokio mutex across task `ask`s; finish RPC timeouts |
| [#194](https://github.com/volga-project/volga/pull/194) | `CheckpointCoordinator` actor; drop `PendingPersist` unlock dance |
| [#191](https://github.com/volga-project/volga/issues/191) | Full Worker-as-actor (beyond interim mutex) |
| [#192](https://github.com/volga-project/volga/issues/192) | Retire `MasterState` mutex bag as control plane |

Intended merge order: **195 → 193 → 194**, then 191/192 as larger arcs.

---

## 7. Debugging narrative (for the post’s middle act)

Suggested beat sheet:

1. **Green locally, red in stress** — 15×100 finds what 1×1 never will.
2. **Artifact archaeology** — shard logs: CP started, kill, `StatePollFailure`,
   missing `CheckpointFailed`, settle timeout / missing `PipelineFinished`.
3. **False leads** — blame gRPC message size / inmem limits (#181 territory);
   those were real but separate; hang reproduced as control-plane race.
4. **Shared phase fix** — works, smells like the Worker mutex story.
5. **Actorize attempt** — phase ownership; Drain as `ask`.
6. **Stress still flakes** — mailbox blocked on barrier/poll fan-out; spawn
   results; then sessions as the clean I/O boundary.
7. **FIFO question** — document the guarantee we do and don’t have; why abort-
   first Drain is enough.
8. **PR hygiene** — wrong-base merge, gRPC orphaned off master, rebase so the
   feature PR is readable.

---

## 8. Lessons (tweetable → expandable)

1. **Stress is a product feature**, not a luxury — same knobs in schedule and
   `workflow_dispatch`, logs as artifacts.
2. **`select!` order and RPC placement are load-bearing** under `biased`.
3. **If two tasks must mutate the same control flag, you don’t have an owner** —
   you have a race or a lock soup.
4. **Actors don’t remove concurrency; they move awaits.** Long work in
   `handle` is the new mutex-across-await.
5. **Timers should not be domain actors**; peers with I/O + identity should
   (WorkerSession).
6. **Stack PRs need a clean master base** or every review becomes archaeology.
7. **Interim shared state is OK if named and linked to the follow-up** (#191/#192).

---

## 9. Code pointers (as of draft)

| Area | Path |
| --- | --- |
| Attempt actor + phase + Drain | `src/runtime/master/attempt/mod.rs` |
| run_loop, poll/CP handlers | `src/runtime/master/attempt/execute.rs` |
| WorkerSession | `src/runtime/master/attempt/session.rs` |
| Lifecycle ask Schedule/Run | `src/runtime/master/lifecycle.rs` |
| StopSources → ask Drain | `src/runtime/master/mod.rs` |
| Attempt ref on state | `src/runtime/master/state.rs` |
| Stress runner | `scripts/stress-test`, `scripts/README.md` |
| CI stress jobs | `.github/workflows/rust-tests.yml` |
| Signature test | `src/tests/inprocess/checkpoint.rs` |
| Harness settle / finish | `src/tests/support/checkpoint/support.rs`, `kill_recovery.rs` |

---

## 10. Open items / honesty box for the post

- Confirm final stress verdict on #195 tip (15×100) before claiming “fixed in
  production CI.”
- Distinguish **hang** failures vs **oracle mismatch** (e.g. key-set overshoot)
  — not every stress red is the same bug.
- WorkerSession + spawn is the thin version of “I/O not on decision mailbox”;
  full Worker actor (#191) and master plane (#192) are still unfinished.
- kameo `DelegatedReply` / bounded mailbox details are version-sensitive
  (we used kameo 0.16).

---

## 11. Possible titles

- “1,500 checkpoint restores and a Drain race: from shared phase to actors”
- “StopSources couldn’t Stop: stress-driven actorization of a Rust control plane”
- “Your `select!` is a scheduler: finding a PipelineFinished hang with CI stress”

---

## 12. Diagram bank (mermaid for the post)

### Before: shared poke

```mermaid
sequenceDiagram
  participant GRPC as StopSources gRPC
  participant State as MasterState phase
  participant Loop as Attempt select loop
  GRPC->>State: set Draining + abort CP
  Loop->>State: read phase / start CP
  Note over GRPC,Loop: Co-ownership via atomics/mutex
```

### After: ask Drain

```mermaid
sequenceDiagram
  participant GRPC as StopSources gRPC
  participant Attempt as ExecutionAttempt
  participant Sess as WorkerSessions
  GRPC->>Attempt: ask Drain
  Attempt->>Attempt: phase=Draining, abort CP
  Attempt->>Sess: ask StopSources
  Attempt-->>GRPC: reply
  Note over Attempt: run_loop ticks cannot start CP while Draining
```

### Run interruptibility

```mermaid
flowchart LR
  run_loop[run_loop task] -->|tell PollTick / CheckpointTick / FailureMsg| mailbox[Attempt mailbox]
  Drain[ask Drain] --> mailbox
  mailbox --> handlers[Short handlers]
  handlers -->|spawn| fanout[Session asks]
  fanout -->|tell PollResult / BarriersDone| mailbox
```

---

*End of notes. Trim ruthlessly for the public post; keep failure logs and one
concrete timeline as the emotional center.*
