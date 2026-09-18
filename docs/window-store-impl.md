# Window store implementation plan

Stacked PRs to land the Scylla window backend. Architecture, schemas, MVCC,
consistency, GC, and rescale live in the store design — **do not duplicate
them here**.

- Design (Scylla + engine contract): [`STORE_DESIGN.md`](../src/runtime/operators/window/STORE_DESIGN.md)
- Operator semantics: [`window/README.md`](../src/runtime/operators/window/README.md)

**This protocol supersedes [#157](https://github.com/volga-project/volga/pull/157).**
Ignore older comments / commits that specify `window_kg_lease`, `prev_E`,
`high_E`, `window_recovery_bases`, HeadClaim / DashMaps, ingest LWT, epoch
restart from pins, or publish-on-checkpoint. The normative cut is
`STORE_DESIGN.md` → *Protocol (normative)*.

Engine contract + InMem isolation already landed
([#285](https://github.com/volga-project/volga/pull/285)).

---

## Protocol (stack must match this)

**Data (always):** raw / tiles / `key_state` / triggers cluster `(attempt, E)`.
`E` is per `key_group`. Seed `next_E[g] = cp_E_g` on first write. Ingest:
unlogged batches, **no LWT**.

**Streaming WO overlay (no pins).** Until restore: `me` only. After restore:

```text
attempt == me
OR (attempt == cp_g.attempt AND E <= cp_E_g)
```

`cp_g` is the restore slice containing the group. Checkpoint blob stores
**per-owned-group** `cp_E` (not one task-wide epoch). `load_triggers` uses
the same overlay. Zombies: old attempt, `E > cp_E` → ignored.

**Request (only if WRO is on)** — `window_pins` per `key_group`:

```text
owner, serving_wm              static
serving_attempt, serving_E     per business_key
```

Idle keys keep their pin until that key is written and published.

- Steal (restore, per owned group): `SET owner = me IF owner = previous`.
  Serving unchanged. Fail the task on miss / timeout.
- Publish (OnCommit / periodic, **not** on CP): if `publish_ok`,
  `SET serving_wm` and dirty keys' `serving_*` `IF owner = me`. Fail the
  task on miss / timeout.

```text
publish_ok ⇔
    current_wm >= serving_wm
    AND current_wm > cp_wm
    AND dirty ≠ ∅
```

- WRO: pin **once** (missing row ⇒ empty), then

```text
attempt == serving_attempt AND E <= serving_E
```

**Not doing:** publish on CP, `prev_E`, `high_E`, ingest CAS, recovery_bases,
any-attempt overlay, WO-only owner suicide (follow-up).

---

## Rules

1. Merge **this design PR** first. Stack impl PRs on `master` after that.
2. Engine contract / InMem isolation is already on master. Later PRs
   implement the Scylla protocol above.
3. Rescale is in the **protocol** (per-group `cp_E`, planner slices). The
   `RestorePlanner` code change can follow; do not design as
   same-assignment-only.

---

## Stack

Each PR stacks on the previous. Do not merge adjacent PRs.

| # | PR | Branch | Scope |
|---|---|---|---|
| 0 | Design | `docs/window-store-protocol` | This PR. Spec only. Protocol above. Replaces #157. |
| 1 | Due paging | `feat/wo-load-triggers` ([#296](https://github.com/volga-project/volga/pull/296)) | Store `load_triggers`; operator pages. Prefetch later: [#297](https://github.com/volga-project/volga/issues/297). |
| 2 | InMemoryGrpc + request-mode runner | `feat/request-inmem-grpc` | [#162](https://github.com/volga-project/volga/issues/162). |
| 3 | Scylla write path | `feat/scylla-wo-write` ([#287](https://github.com/volga-project/volga/pull/287)) | Session, DDL, unlogged versioned writes, `load_triggers`. Overlay **`me` only**. **No pins, no ingest LWT.** Rewrite schema vs any `window_head` leftover. |
| 4 | Checkpoint / restore + pins | `feat/scylla-wo-checkpoint` ([#288](https://github.com/volga-project/volga/pull/288)) | Per-group `cp_E` in `Versioned`. Overlay **`me \| (cp_g.attempt ∧ E ≤ cp_E_g)`**. Request: `window_pins`; steal owner; `publish_ok`. **Rewrite:** drop `prev_E`, `recovery_bases`, HeadClaim, pin=CP. |
| 5 | `maintain` / GC | `feat/scylla-wo-maintain` ([#289](https://github.com/volga-project/volga/pull/289)) | `window_kg_buckets`, `floor_bucket`. Keep overlay ∪ serving pin. |
| 6 | Scylla request store | `feat/scylla-request-store` ([#290](https://github.com/volga-project/volga/pull/290)) | Pin `window_pins` once. Client-side `attempt == serving_attempt ∧ E ≤ serving_E`. |

#287–#290 were stacked on the #157 protocol. After this PR merges, restack
those branches onto the pins / `publish_ok` / per-group CP cut.

---

## Out of this stack

Foyer / cache quotas, `USING TIMESTAMP` instead of owner CAS, ingest CAS,
CDC / late events, WO-only periodic owner suicide.

[#162](https://github.com/volga-project/volga/issues/162) is **PR 2**. **PR 6**
only adds Scylla as another `backend` on that runner.

---

## Testing

Request mode is a **generic graph split**: write path (ingest / maintain
state) and read path (point lookup against published state). Windows (WO/WRO)
are the first SQL shape, not the harness.

Three jobs. Do not fold them into one suite.

| Layer | What it proves | Where | Backend |
|---|---|---|---|
| **A. Operator / store contract** | Window eval, tiles, watermarks, key-group isolation | `window/tests/*` | Process-local `InMemWindowStore`. PR 3 adds a Scylla testcontainer for write-path store ops. |
| **B. Request HTTP plumbing** | Request source ↔ sink, concurrency, echo | `tests/inprocess/request_source.rs` | No operator store. |
| **C. Request-mode correctness** | Write path, read path, and storage on separate processes; exact oracle vs published frontier | One runner on `VolgaCluster` ([#162](https://github.com/volga-project/volga/issues/162)) | `InMemoryGrpc` first, then Scylla. |

### Oracle (Layer C)

| | InMemoryGrpc | Scylla |
|---|---|---|
| Happy path | Last successful `commit` | Last **serving** publish: `attempt == serving_attempt ∧ E ≤ serving_E` |
| Write-worker kill | Storage stays up. After the new write worker attaches, reads = that store’s current contents. No fencing/MVCC. | WRO keeps last **serving** pin while writer recovers. Grey window until steal is accepted. After steal, CAS blocks publish. After `publish_ok`, reads = new serving. |
| Read-worker kill | Requests fail until replacement; then same store. | Same lifecycle; answers still pinned to serving. |
| Storage kill | State gone. Do not test as correctness. | Out of v1 unless we add a dedicated Scylla HA case. |

PR 6 adds `backend = Scylla` to this runner. Failure cases use the same
runner + `FaultAction` already on `VolgaCluster`.
