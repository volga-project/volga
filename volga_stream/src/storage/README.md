## Storage internals

This document describes the **storage execution flow and invariants** used by the runtime.

### Operator-facing API

Operators must only use the storage facade on `WorkerStorageRuntime`:
- `storage.reader()` → `StorageReader` (read path)
- `storage.writer()` → `StorageWriter` (write path)
- `storage.state_handle()` → `StateHandle` (state cache)

No operator should touch backend, pins, or write-buffer internals directly.

### Core components (per worker, per task)

Each worker hosts a shared **`WorkerMemoryPool`** with accounting keyed by `(task_id, tenant_type)`:

- **`WriteBuffer`**: per-task, per-key delta buffer for new data.
- **`BaseRuns`**: per-task, per-key compacted base runs for read performance.
- **`StateCache`**: per-task operator state (serialized blobs).

Persistence is provided by the **`StorageBackend`** (in-mem or remote).

### Moving parts and ownership

Worker-level storage owns the moving parts below; operators only use read/write/state handles.

- **`WorkerStorageRuntime`**: composition root; holds backend, pools, and maintenance supervisor.
- **`WriteBuffer`**: in-memory deltas (per key).
- **`BaseRuns`**: in-memory compacted runs (per key).
- **`BucketIndex`**: per-key metadata about buckets and runs.
- **`StorageReader`**: read path (plan + execute).
- **`StorageWriter`**: write path (append, drop, stats).
- **`MaintenanceSupervisor`**: owns batch-level background maintenance with generic access to `BucketIndexState`.
- **`BatchPins` / `BatchRetirementQueue`**: safe deletion of stored batches.

### Backpressure model: `BacklogBudget`

Backlog tracks **unconsumed bytes** in the pipeline. Dumping/evicting changes *where* bytes live but does not reduce backlog.

Rules:
- **Acquire on ingest** before accepting new data.
- **Release only on progress/pruning** when data becomes unreachable.
- **Never release on dump/persist**.

This guarantees slow/stopped consumers backpressure sources instead of hiding behind infinite spill.

### Memory model: `WorkerMemoryPool`

We track cache-owned bytes, not exact RSS:
- accounting key: `(task_id, tenant_type)` with `tenant_type ∈ {WriteBuffer, BaseRuns, StateCache}`
- each partition has `reserve_bytes` and `soft_max_bytes`

Transient scratch usage (compaction + store IO) is bounded by `ScratchLimiter` (concurrency-only).

### Maintenance ownership

Storage owns background maintenance. Operators only start/stop it for a task.

Maintenance does **not** depend on window state directly; it operates on a generic
`BucketIndexState` (anything that exposes a `BucketIndex`).
Window state implements that trait, so window tasks plug into the same storage machinery.

### Execution flow (high level)

#### 1) Ingest (write path)

1. `StorageWriter::append(...)` acquires `BacklogBudget(bytes_estimate)` and appends deltas.
2. Operator updates `BucketIndex` using the returned `AppendBatch` metadata.
3. If the write buffer is over limit, storage maintenance selects buckets to evict:
   - compact deltas into a base run
   - persist to store (stored runs)
   - drop corresponding deltas from `WriteBuffer`

#### 2) Request read (read path)

1. Plan bucket ranges from `BucketIndex` metadata.
2. Build views from persisted base + `BaseRuns` + `WriteBuffer` deltas.
3. Load missing stored runs from backend with bounded IO and scratch concurrency.
4. Return `SortedRangeView`s with batch pins and scratch permits held.

#### 3) Progress/pruning

When progress advances (e.g. watermark/processed_until):
- prune unreachable deltas/bases
- retire stored runs via retirement queue
- **release `BacklogBudget`** for pruned bytes

### Execution flow (low level)

#### Read path details
1. **Plan**: build `RangesLoadPlan` from `BucketIndex` (no IO).
2. **Execute**:
   - pin stored batch ids (`BatchPins`)
   - estimate working set (for scratch admission)
   - load stored batches from backend
   - snapshot in-mem batches from `WriteBuffer`
3. **Build views**: materialize `SortedRangeView` segments with stable references.

#### Maintenance details
- **Compact**: merge `hot_base + hot_deltas` into new in-mem base runs.
- **Dump**: persist selected buckets to backend and publish stored runs in `BucketIndex`.
- **Rehydrate**: read stored runs and rebuild hot base runs in memory (on demand).
- **Pressure**: select eviction candidates (cold + LRU) and dump to meet target.
  - **Eviction granularity**: evict at **per-key** granularity. When relieving pressure, select an
    eviction candidate key and evict all hot batches/state for that key together.
    TODO: current implementation evicts per-bucket; update pressure relief to evict per-key.

### Deleting (batch retirement)

Deleting means **physically deleting** stored batches that were already removed from metadata
and are no longer pinned by any readers. The retirement queue tracks these ids and the deleter
periodically deletes them once pin counts drop to zero.

### Window state wiring

Window state is just one implementation of `BucketIndexState`:
- `WindowsState` owns a `BucketIndex` and optional `Tiles`.
- Window operator updates `BucketIndex` on ingest and pruning.
- Maintenance uses the same storage machinery through the `BucketIndexState` trait.

This lets other operators (e.g. time-based joins) reuse the same storage maintenance
without depending on window-specific state or policies.

### Integration with master / worker / window operator

- **Master**: owns orchestration only (pipelines, attempts, checkpoints). It does **not** touch
  storage internals; storage state is checkpointed by workers and treated as opaque bytes.
- **Worker**: builds a single `WorkerStorageRuntime` per worker (backend + budgets + maintenance)
  and injects it into each task `RuntimeContext`.
- **Window operator**: pulls the storage runtime from `RuntimeContext`, creates
  `WindowOperatorState`, and only uses the storage facade. Checkpoint/restore calls go through
  `StorageBackend` (`checkpoint` / `apply_checkpoint`), while logical window state is
  persisted via `StateHandle`.

In short: master coordinates, worker owns the backend and runtime, operator uses the facade.
