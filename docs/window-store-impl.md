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
   client contract. There is **no** InMemoryGrpc / fake remote store.
4. v1 restore is **same assignment**. `RestorePlanner` remap is out of this
   stack (see design *Rescaling*).

---

## Stack

This is the cut. Each PR stacks on the previous. Do not merge adjacent PRs.

| # | PR | Branch | Scope |
|---|---|---|---|
| 0 | Design | `docs/window-store-design` | #157. Spec only. |
| 1 | Engine contract + InMem | `feat/window-store-contract` | Operator-scoped `StateNamespace`; per-task store client (`max_p`, key-group range); traits drop `namespace` on `stream_due` / `checkpoint` / `restore`; `OperatorTaskState` exposes owned range for `maintain`. Request-mode engine: HTTP processor only on the request-source worker, request source/sink colocation, WRO forwards request extras. **Test:** two WO tasks, one worker — restore/maintain of task 0 do not clobber task 1. |
| 2 | Scylla write path | `feat/scylla-wo-write` | Session, DDL, head LWT, raw / tiles / `key_states`, `commit_events` (same-partition UNLOGGED BATCH), overlay filter, triggers, `stream_due`. Derive `key_group`. Testcontainer ingest + load + emit. |
| 3 | Checkpoint / restore v1 | `feat/scylla-wo-checkpoint` | `Versioned { version }`, `window_recovery_bases`, writer vs serving. Identity `RestorePlanner`. Existing checkpoint e2e + Scylla. |
| 4 | `maintain` / GC | `feat/scylla-wo-maintain` | `window_kg_buckets`, `floor_bucket`, raw + all tile granularities, MVCC GC from live index keys, serving-pin grace. |
| 5 | Scylla request store + Layer C | `feat/scylla-request-store` | Scylla `WindowRequestStore` (serving pin). Cluster runner ([#162](https://github.com/volga-project/volga/issues/162)): `env × profile`, backend is Scylla. First scenario is windows; APIs are write/read/state, not WO/WRO. |

---

## Out of this stack

In the design as later work: `RestorePlanner` range intersection, Foyer / cache
quotas, `USING TIMESTAMP` instead of LWT, CDC / late events.

A process-local InMem wrapped in gRPC is **not** a backend. Request mode uses
Scylla (or another real shared store). [#162](https://github.com/volga-project/volga/issues/162)
is the Layer C runner on **PR 5**, not a second store implementation.

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
| **A. Operator / store contract** | Window eval, tiles, watermarks, key-group isolation | `window/tests/*`, `test_utils/window/harness.rs` | Process-local `InMemWindowStore`. PR 2 adds a Scylla testcontainer for write-path store ops. **Window-specific — stays here.** |
| **B. Request HTTP plumbing** | Request source ↔ sink, concurrency, echo | `tests/inprocess/request_source.rs` | No operator store. Keep as-is. |
| **C. Request-mode correctness** | Write path, read path, and storage on separate processes; exact oracle vs published serving | One runner on `VolgaCluster` ([#162](https://github.com/volga-project/volga/issues/162)) | **Scylla only.** Not process-local `InMemory`. First scenario: window SQL. |

### Keep vs discard

- **Keep** Layer A. `WoWroHarness` (shared `Arc<InMem>`) is window-operator
  semantics / matrix / tiling. Not the request-mode topology. Do not re-run
  the matrix on the cluster.
- **Keep** Layer B. It uses a window exec only to steal KeyBy keys, then
  `IdentityMapFunction`. It is not a read-path store test.
- **Discard** (already gone) the old one-worker shared-memory request-mode
  test. Do not revive it.
- **Do not** add an InMemoryGrpc / networked-InMem store.
- **Do not** treat request-mode **benchmarks** as correctness.

After PR 1, add the two-task / one-worker isolation test in Layer A (same
physical InMem, range-isolated clients). That is not Layer C.

### Environments (Layer C)

Same pattern as existing suite tests (`src/tests/README.md`). The runner takes
`RuntimeEnv`. Entrypoints live under `inprocess/` / `docker/` / `kube/`.
Docker and kube stay `#[ignore]` and run via `scripts/test docker` / `kube`.

Layer C needs Scylla. **Do not** put Scylla × all envs on every PR.

| Env | CI | Role |
|---|---|---|
| **Local** | not `default-tests` | `#[ignore]` / testcontainer or `VOLGA_SCYLLA_*`. Smoke when Scylla is there. |
| **Docker** | later / nightly | Same runner, `RuntimeEnv::Docker`, Scylla in compose. |
| **Kube** | later / `kube-stress` | Same runner, `RuntimeEnv::Kube`. |

Process-local `InMemory` stays streaming-only and illegal in request mode.

### Profiles and failures

`run_request_correctness(env, profile)` — request-mode, Scylla, not window.

- Smoke: bounded; run where Scylla is configured.
- Stress: scheduled/nightly, concurrency / mixed versions.
- Failure: same runner, extra fault (kill write worker / read worker).
- Workload/oracle for windows is a **scenario** parameter (later: joins).

Oracle is **serving** (Scylla MVCC): last serving publish on the happy path.
Write-worker kill: WRO keeps the last serving version while the writer
recovers. Successful reads must not mix writer+serving or go empty. After
serve-promote, reads = new serving. Read-worker kill: requests fail until
replacement; answers still pinned to serving. Storage kill is out of v1
unless we add a dedicated Scylla HA case.

Failure cases use the same runner + `FaultAction` already on `VolgaCluster`.

### What not to duplicate

- Tile / coverage geometry → Layer A only (window).
- HTTP echo / pending-request limits → Layer B only.
- Mixed-version reads, cross-key leak, write≠read workers, published serving
  snapshot → Layer C only (any stateful request-mode SQL).
