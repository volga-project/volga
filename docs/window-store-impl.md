# Window store implementation plan

How to land the Scylla window backend. Architecture, schemas, MVCC,
consistency, GC and rescale live in the design — **do not duplicate them
here**.

- Design: [`STORE_DESIGN.md`](../src/runtime/operators/window/STORE_DESIGN.md) ([#157](https://github.com/volga-project/volga/pull/157))
- Operator semantics: [`window/README.md`](../src/runtime/operators/window/README.md)
- Impl backlog / umbrella: [#291](https://github.com/volga-project/volga/issues/291)

`STORE_DESIGN.md` only exists on `docs/window-store-design` until #157 merges.

**The protocol changed after the Scylla stack was written.** PRs #287–#293
implement per-key `window_head`, `window_recovery_bases`, an ownership lease,
a `serving_publish` cadence and `prev_E`. None of that survives. Ignore every
older comment and commit that specifies them; #157 is normative.

---

## What the new protocol is, in one screen

`Version = (attempt, epoch)`, totally ordered. Visibility is a per-key-group
`CutHistory` — at most one `(attempt, epoch)` prefix per attempt, sorted.

```text
WO:        visible ⇔ attempt == my_attempt OR cp_cut[g].allows(v)
WRO cmtd:  visible ⇔ cut.allows(v)
WRO fresh: [lo, committed_wm] as above; (committed_wm, hi] raw only,
           visible ⇔ cut.allows(v) OR v.attempt == cur_attempt
```

Publication **is** the checkpoint. On checkpoint *completion* the WO writes
`cut`, `prev_cut`, `committed_wm` and `retention_floor` into one
`window_kg_meta` row per owned key group. That is the entire streaming →
serving interface. There is no publish cadence, no ownership lease, no
per-key pin and no clamp, because the published cut is exactly the cut a
successor restores from.

Gone: `window_head`, `window_recovery_bases`, `window_kg_lease`, `prev_E`,
`high_E`, steal, promote, `serving_publish`, per-key catch-up freeze,
HeadClaim/DashMaps, epoch seeding from a checkpoint or pin.

---

## Prerequisites (master)

Both block the Scylla backend from opening. Neither is window work.

| | What | Why | Tracking |
|---|---|---|---|
| P1 | Durable, never-reused `attempt` allocated by the master before workers are configured | `attempt_id` resets to `0` on pipeline start, and `max(restored) + 1` collides when two attempts restore from the same checkpoint | [#156](https://github.com/volga-project/volga/issues/156) |
| P2 | `notify_checkpoint_complete(checkpoint_id)` delivered to tasks | The cut may only be published after its checkpoint completes globally; publishing at the barrier reintroduces the clamp | new issue |

P2's fallback, if the notification is too invasive: piggyback
`last_completed_checkpoint_id` on the next barrier message and publish one
checkpoint behind. Costs one extra interval of serving lag. The master
already tracks `latest_complete_checkpoint` and emits
`LifecycleEvent::CheckpointCompleted` (`src/runtime/master/state.rs`); what is
missing is only the path back to the tasks.

---

## Work items

### Contract and operator (backend-independent)

| | Item | Where |
|---|---|---|
| C1 | `Version` / `CutHistory` types, encode/decode, `allows` / `advance` with the asserts from the design | `window/store/` |
| C2 | `ReadOptions { Committed, Fresh }` on `load_window_data`; return `committed_wm` + `checkpoint_id` alongside the data | `store/backend/mod.rs`, InMem, Scylla |
| C3 | Coverage guard: refuse a request whose `lo < retention_floor`. Replaces the `// No lateness filter. Answer from whatever state the backend still retains.` comment | `window/request.rs` |
| C4 | Request-mode admission on `ts > retention_floor` instead of `ts > watermark`, for `StateOnly` operators only | `window/state.rs`, `window/operator.rs` |
| C5 | Derive retention from the **committed** watermark and publish the floor; GC gated on the published floor, never the live one | `window/state.rs`, backend `maintain` |
| C6 | Fresh mode: head-scoped read as specified | InMem + Scylla read paths |
| C7 | Deduplicate **after** version filtering, not before | `window/store/data.rs` |

Tests to add with these:

- WO emitted rows for `[lo, hi]` == WRO `Committed` read of `[lo, hi]`. This
  equivalence is the split-path contract and is currently untested
  (`window/tests/semantics.rs`).
- Coverage guard refuses below the floor and answers at the floor. Removes
  the `lateness: 300_000` workaround and the *WRO query horizon is not
  modeled yet* comment in `window/tests/matrix.rs`.
- Fresh sees a post-checkpoint row that `Committed` does not; both agree
  again after the next completed checkpoint.
- A late row below `committed_wm` is invisible to Fresh and visible after the
  next completed checkpoint (the documented gap, asserted rather than
  discovered).

### Scylla stack rework

Each row is a rewrite of an open PR, not a new PR on top.

| | PR | What changes |
|---|---|---|
| S1 | [#287](https://github.com/volga-project/volga/pull/287) write path | Add `attempt` to the clustering key of all four data tables, version clustering `DESC`. Per-group atomic `next_epoch` from 0. Acked-prefix low-water tracking. Per-key in-flight rule (no read or second commit for a key with a commit in flight). Drop `window_head` and any ingest LWT. |
| S2 | [#288](https://github.com/volga-project/volga/pull/288) checkpoint/restore | `Versioned { attempt, cuts }` replaces `Versioned { version }`; delete `window_recovery_bases`. Restore takes the master attempt, asserts dominance, `next_epoch = 0`. **Delete** steal, promote, `serving_publish`, catch-up freeze. Add `window_kg_meta` and the completion-triggered publish. |
| S3 | [#289](https://github.com/volga-project/volga/pull/289) maintain/GC | Per-cell version retention, three slots (`cur_attempt`, `cut`, `prev_cut`). Gate every delete on the published floor. No wall-clock grace anywhere. |
| S4 | [#290](https://github.com/volga-project/volga/pull/290) request store | Read `window_kg_meta` per request (not cached in v1) instead of a pin; `ReadOptions`; coverage guard; wire WRO to the worker `StateSessionHandle` instead of opening a second driver pool. |
| S5 | [#292](https://github.com/volga-project/volga/pull/292) retry profile | Mostly stands. LWT now appears only on `window_kg_meta` writes. Keep: timeout fails the task, no epoch bump, no republish. |
| S6 | [#293](https://github.com/volga-project/volga/pull/293) kube schema | Drop `serving_publish` from `ScyllaConfig` — there is no cadence. Rest stands. |
| S7 | [#296](https://github.com/volga-project/volga/pull/296) due paging | Independent of the protocol change; `load_triggers` uses the same filter. Land it on its own schedule. |

### Deferred, tracked, not in this pass

- Skinny unversioned due-bucket index to replace the 1:1 `window_due` shadow,
  and consumed-due GC, which needs that index to find partitions.
- Cut-history retirement / forced version compaction. v1 caps the list and
  fails loudly.
- `RestorePlanner` range intersection for rescale ([#121](https://github.com/volga-project/volga/issues/121)); v1 is same assignment.
- Foyer WO cache, namespaced quota, `StateResourceTracker` backpressure.
- WRO-side caching of the `window_kg_meta` row. One slot of GC grace covers
  readers one generation stale, so caching needs an explicit
  `TTL + max request duration < checkpoint interval` bound or a second
  retention slot. Measure the hop before adding the knob.
- Request workers ([#247](https://github.com/volga-project/volga/issues/247)); Layer C uses `Pipelined` placement until they exist.
- Keyspace DDL is hardcoded `SimpleStrategy` RF=1 in `StateSessionHandle::connect`; production needs `NetworkTopologyStrategy`, or assume the keyspace exists.
- No auth/TLS on `ScyllaConfig`.
- Checkpoint store is still `in_memory`; operator state can be Scylla while job checkpoints live in the master.
- CDC / late-event correction ([#122](https://github.com/volga-project/volga/issues/122), [#174](https://github.com/volga-project/volga/issues/174)).

---

## Rules

1. Merge **#157** first.
2. **No Scylla until P1 and P2 exist.** The backend refuses to open without a
   durable attempt and a completion signal.
3. Same `StateNamespace` meaning on every backend. It encodes
   `(pipeline, operator)` and is fixed at compile time, so WO and WRO agree
   without sharing runtime context — that is what makes serving deployable
   independently ([#285](https://github.com/volga-project/volga/pull/285), merged).
4. Shared-worker restore and maintain must not clobber sibling tasks.
5. v1 restore is **same assignment**.

---

## Testing

Request mode is a **generic graph split**: a write path (ingest / maintain
state) and a read path (point lookup against published state). Windows are
the first SQL shape, not the harness. Name Layer C APIs in those terms
(`request_store`, write/read workers) — not `run_window_*`.

Three jobs. Do not fold them into one suite.

| Layer | What it proves | Where | Backend |
|---|---|---|---|
| **A. Operator / store contract** | Window eval, tiles, watermarks, key-group isolation, the `Committed`/`Fresh`/coverage semantics above | `window/tests/*`, `test_utils/window/harness.rs` | Process-local `InMemWindowStore`; Scylla testcontainer for write-path store ops. **Window-specific — stays here.** |
| **B. Request HTTP plumbing** | Request source ↔ sink, concurrency, echo | `tests/inprocess/request_source.rs` | No operator store. Keep as-is. |
| **C. Request-mode correctness** | Write path, read path and storage on separate processes; exact oracle vs the published cut | One runner on `VolgaCluster` | Scylla (`InMemoryGrpc` was dropped from the stack; [#162](https://github.com/volga-project/volga/issues/162) is still the spec if it is revived) |

- **Keep** Layer A. `WoWroHarness` is operator semantics and tiling; do not
  re-run the matrix on the cluster.
- **Keep** Layer B. It uses a window exec only to steal KeyBy keys.
- **Do not** revive the old one-worker shared-memory request-mode test, and
  do not treat request-mode benchmarks as correctness.

### Environments (Layer C)

Same pattern as the existing suite (`src/tests/README.md`); the runner takes
`RuntimeEnv`, entrypoints live under `inprocess/` / `docker/` / `kube/`.
Docker and kube stay `#[ignore]` and run via `scripts/test docker` / `kube`,
which are already on PR/push CI. Scylla store unit tests are testcontainer;
cluster Scylla is one env plus stress, nightly. Process-local `InMemory`
stays streaming-only and illegal in request mode.

### Oracle

`run_request_correctness(env, backend, profile)` — request-mode, not window.
Profiles: smoke (bounded, on CI), stress (nightly), failure (same runner plus
a `FaultAction` already on `VolgaCluster`).

| Case | Expected |
|---|---|
| Happy path, `Committed` | Exactly the last completed checkpoint's cut. Never a partial commit, never two payloads for one cell. |
| Happy path, `Fresh` | A superset of `Committed` restricted to `(committed_wm, hi]`; converges to `Committed` after the next completed checkpoint. |
| Write-worker kill | `Committed` keeps answering from the last completed checkpoint throughout recovery, then advances when the new attempt's first checkpoint completes. It must never regress and never mix attempts. `Fresh` may undercount during replay. |
| Read-worker kill | Requests fail until the replacement attaches; answers unchanged. |
| Uncovered range | Refused, not silently undercounted. |
| Storage kill | Out of v1 unless we add a dedicated Scylla HA case. |

The write-worker-kill case is the one that distinguishes this protocol from
the previous one: there is no grey window, no serving regression and no
attempt mixing to assert around, because the published cut only grows.

### What not to duplicate

- Tile / coverage geometry → Layer A only.
- HTTP echo / pending-request limits → Layer B only.
- Cross-key leak, write≠read workers, published-cut visibility → Layer C only.
