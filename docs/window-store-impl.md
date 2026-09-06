# Window store implementation plan

Stacked PRs to land the Scylla window backend. Architecture, schemas, MVCC,
consistency, GC, and rescale live in the store design — **do not duplicate
them here**.

- Design (Scylla + engine contract): [`STORE_DESIGN.md`](../src/runtime/operators/window/STORE_DESIGN.md) ([#157](https://github.com/volga-project/volga/pull/157))
- Operator semantics: [`window/README.md`](../src/runtime/operators/window/README.md)

`STORE_DESIGN.md` may only exist on `docs/window-store-design` until #157 merges.

---

## Rules

1. Merge **#157** first. Stack impl PRs on `master` after that.
2. **No Scylla until the engine contract and InMem isolation are real.** Same
   `StateNamespace` meaning on every backend; shared-worker restore must not
   clobber sibling tasks.
3. Only **PR 1** changes InMem and the store traits. Later PRs implement that
   client contract. **PR 2** is the InMemoryGrpc harness ([#162](https://github.com/volga-project/volga/issues/162)), not Scylla.
4. v1 restore is **same assignment**. `RestorePlanner` remap is out of this
   stack (see design *Rescaling*).

---

## Stack

This is the cut. Each PR stacks on the previous. Do not merge adjacent PRs.

| # | PR | Branch | Scope |
|---|---|---|---|
| 0 | Design | `docs/window-store-design` | #157. Spec only. |
| 1 | Engine contract + InMem | `feat/window-store-contract` | Operator-scoped `StateNamespace`; per-task store client (`max_p`, key-group range); traits drop `namespace` on `stream_due` / `checkpoint` / `restore`; `OperatorTaskState` exposes owned range for `maintain`. **Test:** two WO tasks, one worker — restore/maintain of task 0 do not clobber task 1. Read-path client uses operator ns. |
| 2 | InMemoryGrpc + request-mode runner | `feat/request-inmem-grpc` | [#162](https://github.com/volga-project/volga/issues/162): shared state over gRPC; generic request-mode cluster runner (`env × backend × profile`). First scenario is windows; APIs are write/read/state, not WO/WRO. |
| 3 | Scylla write path | `feat/scylla-wo-write` | Session, DDL, head LWT, raw / tiles / `key_states`, `commit_events` (same-partition UNLOGGED BATCH), overlay filter, triggers, `stream_due`. Derive `key_group`. Testcontainer ingest + load + emit. |
| 4 | Checkpoint / restore v1 | `feat/scylla-wo-checkpoint` | `Versioned { version }`, `window_recovery_bases`, writer vs serving. Identity `RestorePlanner`. Existing checkpoint e2e + Scylla. |
| 5 | `maintain` / GC | `feat/scylla-wo-maintain` | `window_kg_buckets`, `floor_bucket`, raw + all tile granularities, MVCC GC from live index keys, serving-pin grace. |
| 6 | Scylla request store | `feat/scylla-request-store` | Scylla read-path store (first: `WindowRequestStore`). Register `backend = Scylla` on the PR 2 runner. |

---

## Out of this stack

In the design as later work: `RestorePlanner` range intersection, Foyer / cache
quotas, `USING TIMESTAMP` instead of LWT, CDC / late events.

[#162](https://github.com/volga-project/volga/issues/162) is **PR 2**. **PR 6**
only adds Scylla as another `backend` on that runner.

---

## Testing

Request mode is a **generic graph split**: write path (ingest / maintain
state) and read path (point lookup against published state). Windows (WO/WRO)
are the first SQL shape, not the harness. Joins and other stateful ops plug
into the same runner later.

Name Layer C and #162 APIs in those terms (`request_store`, write/read
workers, serving snapshot). Do not call the runner `run_window_*` or put it
under `window/tests`.

Three jobs. Do not fold them into one suite.

| Layer | What it proves | Where | Backend |
|---|---|---|---|
| **A. Operator / store contract** | Window eval, tiles, watermarks, key-group isolation | `window/tests/*`, `test_utils/window/harness.rs` | Process-local `InMemWindowStore`. PR 3 adds a Scylla testcontainer for write-path store ops. **Window-specific — stays here.** |
| **B. Request HTTP plumbing** | Request source ↔ sink, concurrency, echo | `tests/inprocess/request_source.rs` | No operator store. Keep as-is. |
| **C. Request-mode correctness** | Write path, read path, and storage on separate processes; exact oracle vs published frontier | One runner on `VolgaCluster` ([#162](https://github.com/volga-project/volga/issues/162)) | `InMemoryGrpc` first, then Scylla. **Not** process-local `InMemory`. First scenario: window SQL. |

### Keep vs discard

- **Keep** Layer A. `WoWroHarness` (shared `Arc<InMem>`) is window-operator
  semantics / matrix / tiling. Not the request-mode topology. Do not re-run
  the matrix on the cluster.
- **Keep** Layer B. It uses a window exec only to steal KeyBy keys, then
  `IdentityMapFunction`. It is not a read-path store test.
- **Discard** (already gone) the old one-worker shared-memory request-mode
  test. Do not revive it.
- **Do not** treat request-mode **benchmarks** as correctness.

After PR 1, add the two-task / one-worker isolation test in Layer A (same
physical InMem, range-isolated clients). That is not Layer C.

### Environments (Layer C)

Same pattern as existing suite tests (`src/tests/README.md`). The runner takes
`RuntimeEnv`. Entrypoints live under `inprocess/` / `docker/` / `kube/`.
Docker and kube stay `#[ignore]` and run via `scripts/test docker` / `kube`
— those jobs already run on PR/push CI. **Do not skip request mode there.**

| Env | CI | Role |
|---|---|---|
| **Local** | `default-tests` (inprocess) | Smoke + InMemoryGrpc. Separate storage task, not a shared `Arc`. |
| **Docker** | `docker-tests` (already on PR) | Same runner, `RuntimeEnv::Docker`. |
| **Kube** | `kube-tests` (already on PR) | Same runner, `RuntimeEnv::Kube`. |

What we do **not** put on every PR: Scylla × all envs, or stress profiles.
Scylla store unit tests = testcontainer. Cluster Scylla = one env + stress
(nightly / `kube-stress`), later.

Process-local `InMemory` stays streaming-only and illegal in request mode.

### Profiles, backends, failures

`run_request_correctness(env, backend, profile)` — request-mode, not window.

- Smoke: bounded, all three envs, `InMemoryGrpc` (then Scylla where configured).
- Stress: scheduled/nightly, concurrency / mixed versions.
- Failure: same runner, extra fault (kill write worker / read worker). Storage
  process kill is out of scope for InMemoryGrpc ([#162](https://github.com/volga-project/volga/issues/162) non-goal).
- Workload/oracle for windows is a **scenario** parameter (later: joins).

**Oracle is backend-specific.** Do not assert Scylla MVCC on InMemoryGrpc.

| | InMemoryGrpc | Scylla |
|---|---|---|
| Happy path | Last successful `commit` | Last **serving** publish |
| Write-worker kill | Storage stays up. Successful reads = last completed commit. No frozen snapshot while the replacement replays. After the new write worker attaches to the **same** store, reads = that store’s current contents. No fencing/MVCC. | WRO keeps the last **serving** version while writer recovers (design). Successful reads during outage/recovery must not mix writer+serving or go empty. After serve-promote, reads = new serving. |
| Read-worker kill | Requests fail until replacement; then same store, same answers. | Same lifecycle; answers still pinned to serving. |
| Storage kill | State gone. Do not test as correctness. | Out of v1 unless we add a dedicated Scylla HA case. |

PR 6 adds `backend = Scylla` to this runner. It does not invent a second
request-mode test. Failure cases use the same runner + `FaultAction` already
on `VolgaCluster`.

### What not to duplicate

- Tile / coverage geometry → Layer A only (window).
- HTTP echo / pending-request limits → Layer B only.
- Mixed-version reads, cross-key leak, write≠read workers, published serving
  snapshot → Layer C only (any stateful request-mode SQL).
