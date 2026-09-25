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
| P2 | `notify_checkpoint_complete(checkpoint_id)` delivered to tasks | The cut may only be published after its checkpoint completes globally; publishing at the barrier reintroduces the clamp | [#300](https://github.com/volga-project/volga/issues/300) |

The master already tracks `latest_complete_checkpoint` and emits
`LifecycleEvent::CheckpointCompleted` (`src/runtime/master/state.rs`); what is
missing is only the path back to the tasks. Delivery may be at-most-once — a
missed notification is repaired by the heal at open, not by waiting for the
next checkpoint.

---

## Work items

### Contract and operator (backend-independent)

| | Item | Where |
|---|---|---|
| C1 | `Version` / `CutHistory` types, encode/decode, `allows` / `advance` with the asserts from the design | `window/store/` |
| C2 | `ReadOptions { Committed, Fresh }` on `load_window_data`; return `committed_wm` + `checkpoint_id` alongside the data | `store/backend/mod.rs`, InMem, Scylla |
| C3 | Coverage guard: refuse a request whose `lo < retention_floor`. Replaces the `// No lateness filter. Answer from whatever state the backend still retains.` comment | `window/request.rs` |
| C4 | Request-mode admission on `ts > retention_floor` instead of `ts > watermark`, for `StateOnly` operators only | `window/state.rs`, `window/operator.rs` |
| C5 | Gate every delete on the **committed** watermark in **both** modes, never the live one. Raw minutes and tiles use the data floor (`committed_wm − window − lateness`). Triggers drop at `fire_at.ts <= committed_wm`. Streaming sources the watermark locally from its last completed checkpoint; request publishes the same value for readers | `window/state.rs`, backend `maintain` |
| C8 | Reader staleness check: re-read the metadata row on requests that outlive a threshold and fail on `pinned_id < row.prev_checkpoint_id`. Exact, and independent of wall clock — publication cadence is not the configured interval. A request deadline is operational hygiene, not the mechanism | request path |
| C9 | `load_key_state` is one unpaged read of the key partition. The first visible row wins. A `LIMIT` can stop on unswept zombie versions and return `KeyState::default()`, which resets `next_seq` and loses `evaluation.through` | backend read path |
| C6 | Fresh mode: head-scoped read as specified | InMem + Scylla read paths |
| C7 | Deduplicate **after** version filtering, not before | `window/store/data.rs` |

Tests to add with these:

- Split-path equivalence, **quiesced**: drain the input, let one checkpoint
  complete, then assert WO emitted rows for `[lo, hi]` == WRO `Committed`
  read of `[lo, hi]` (`window/tests/semantics.rs`). Do not assert this
  against a live emit path — a running `Emit` WO computes from
  `my_attempt | cp_cut` and is legitimately ahead of the published cut, so
  the live form asserts zero publication lag, which the design does not
  promise.
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
| S1 | [#287](https://github.com/volga-project/volga/pull/287) write path | Add `attempt` to the clustering key, version clustering `DESC`. Per-group atomic `next_epoch` from 0. Acked-prefix low-water tracking. Per-key in-flight rule. Drop `window_head` and any ingest LWT. Raw stays a 60-second partition plus `window_kg_buckets`. Tiles drop the minute column: one partition per key per granularity, one `tile_start` range per run. `window_triggers` is `(namespace, kg_shard)` with one unpaged `(fire_ts, fire_seq)` range per touched shard and no paging. `key_state` is one unpaged read. Overlap that read with the tile loads. |
| S2 | [#288](https://github.com/volga-project/volga/pull/288) checkpoint/restore | `Versioned { attempt, cuts }` replaces `Versioned { version }`; delete `window_recovery_bases`. Restore takes the master attempt, asserts dominance, `next_epoch = 0`. **Delete** steal, promote, `serving_publish`, catch-up freeze. Add `window_kg_meta` with **two** statements: publish (`prev_cut = row.cut`, `prev_checkpoint_id = row.checkpoint_id`, `IF cur_attempt <= me AND checkpoint_id = observed`) and take-attempt (`SET cur_attempt = me IF cur_attempt <= me`). Triggers: checkpoint completion publishes; open publishes the restored cut if the row is behind it, else takes the attempt only, and must finish before ingest starts or `Fresh` keeps seeing the dead attempt. Do not publish with an unchanged cut — that sets `prev_cut = cut` and collapses the two GC retention slots. Open does not create the row on a fresh job. |
| S3 | [#289](https://github.com/volga-project/volga/pull/289) maintain/GC | Per-cell version retention, three slots (`cur_attempt`, `cut`, `prev_cut`). Raw: partition-delete minutes below the data floor via `window_kg_buckets`. Tiles: one range delete per key per granularity for tiles that end at or below the floor, then the version trim. Triggers: one `fire_ts <= committed_wm` range delete per shard the task fully owns. In-memory triggers use that same watermark. No wall-clock grace. |
| S4 | [#290](https://github.com/volga-project/volga/pull/290) request store | Read `window_kg_meta` per request (not cached in v1) instead of a pin; `ReadOptions`; coverage guard; staleness re-read (C8); the store is **given** a session rather than calling `connect()` itself, so it fails at configure instead of on the first HTTP request. Collocated, that session is the worker's `StateSessionHandle`; standalone it is the request worker's own — the signature says "a session" and does not name either. The request store takes **no** key-group range and no lifecycle methods; its whole input is namespace, `max_parallelism`, session, read policy. |
| S5 | [#292](https://github.com/volga-project/volga/pull/292) retry profile | Mostly stands. LWT now appears only on `window_kg_meta` writes. Keep: timeout fails the task, no epoch bump, no republish. |
| S6 | [#293](https://github.com/volga-project/volga/pull/293) kube schema | Drop `serving_publish` from `ScyllaConfig` — there is no cadence. Rest stands. |
| S7 | [#296](https://github.com/volga-project/volga/pull/296) due paging | Merged, then removed from the store. The operator still calls `load_triggers(after, through)`. The Scylla read is one unpaged range per shard, not a page loop. |

### Deferred, tracked, not in this pass

- Skinny unversioned trigger index to replace the 1:1 `window_triggers` rows.
  Consumed-trigger GC does not wait on it: it is the per-shard `fire_ts` range
  delete in S3.
- Cut-history retirement / forced version compaction. v1 caps the list and
  fails loudly.
- `RestorePlanner` range intersection for rescale ([#121](https://github.com/volga-project/volga/issues/121)); v1 is same assignment.
- Foyer WO cache, namespaced quota, `StateResourceTracker` backpressure.
- WRO-side caching of the `window_kg_meta` row. The pin-generation re-read
  (C8) already makes a cache safe rather than unsound, so this is a cost
  question: a cache trades the extra hop for a visible failure rate unless
  retention widens beyond one previous cut. Measure the hop first.
- Request workers ([#247](https://github.com/volga-project/volga/issues/247)). The protocol is already compatible — rule 4 is exactly the interface that split needs — so what remains is deployment, not design: request-mode placement is pinned to `Pipelined` so the HTTP source and sink share a process, and Layer C therefore cannot yet put write and read paths on separate workers. Neither shows up in the store contract.
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
4. **Ownership is write-side only.** WO tasks own key-group ranges; WRO owns
   nothing and is stateless. Its whole input is `StateNamespace`,
   `max_parallelism`, a store session and a read policy — no `task_index`,
   no attempt, no assignment, no lifecycle methods, no master handshake. Any
   WRO task can answer any key; routing is locality. Do not add a read-side
   range bound as an optimization: that makes serving parallelism a function
   of the streaming topology again.
5. Shared-worker restore and maintain must not clobber sibling tasks.
6. v1 restore is **same assignment**.

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
| **C. Request-mode correctness** | Write path and read path against shared remote state; exact oracle vs the published cut | One runner on `VolgaCluster` | Scylla (`InMemoryGrpc` was dropped from the stack; [#162](https://github.com/volga-project/volga/issues/162) is still the spec if it is revived) |

**Layer C v1 is collocated WO and WRO with remote Scylla**, because
request-mode placement is pinned to `Pipelined` so the HTTP source and sink
can share a process. The storage boundary is real and that is what the layer
exists to exercise; the process boundary between write and read paths is not
yet, and waits on [#247](https://github.com/volga-project/volga/issues/247).
Do not describe Layer C as proving independent serving until it does — the
protocol permits it, the deployment does not yet.

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
| Worker kill | Requests fail while the attempt is down. On resume, `Committed` returns the last completed checkpoint's cut — never empty, never regressed, never mixing attempts — and advances when the new attempt's first checkpoint completes. `Fresh` may undercount during replay. |
| Uncovered range | Refused, not silently undercounted. |
| Storage kill | Out of v1 unless we add a dedicated Scylla HA case. |

There is **one** kill case in v1, not a write-side and a read-side one.
`PipelinedStrategy` places every vertex by `task_index / slots_per_node`, so
WO and WRO of a slice share a worker, and recovery replaces the whole
execution attempt anyway.

That also bounds what the kill case can prove. The protocol's real claim is
that a reader keeps serving the last completed cut *while* the writer
recovers — no grey window, no serving regression, no attempt mixing, because
the published cut only grows. Observing the *continuity* needs a reader that
outlives the writer's failure, which is
[#247](https://github.com/volga-project/volga/issues/247). What v1 can
assert is the state on either side of the gap, which still catches a
regression, a clamp or a lost cut; it just cannot catch a transient one.
Extend this case when request workers land.

### What not to duplicate

- Tile / coverage geometry → Layer A only.
- HTTP echo / pending-request limits → Layer B only.
- Cross-key leak, published-cut visibility across a restart, remote-state
  round trip → Layer C only.
