# Window store with Scylla design

`WindowOperatorStore` serves the sole WO owner of a key.
`WindowRequestStore` serves coherent WRO point lookups. Physical layout, MVCC,
serialization, and caching stay inside the backend.

For detailed window-operator semantics, evaluation flow, and module structure,
see the [window operator README](README.md). This document focuses on store
contracts and the proposed Scylla backend.

The current data contract is append-only. CDC and late-event correction will
require explicit mutation semantics rather than implicit delete markers — this
is future work.

## Contract types

These are the logical models; backend serialization may differ. They are an
**engine contract** (WO, WRO, InMem, Scylla). Physical layout may add columns
such as `key_group`; it must not change what `StateNamespace` means.

```rust
/// Operator state space. Shared by every WO task and by WRO.
/// Bytes encode (pipeline, owner operator) only — not task_index.
pub struct StateNamespace {
    pub bytes: Vec<u8>,
}

/// Collision-safe logical identity. Backends derive `key_group` from the
/// business-key hash and job `max_parallelism`; they do not hash-mod `p`.
pub struct PartitionKey {
    pub namespace: Vec<u8>,
    pub business_key: Vec<u8>,
}

/// Contiguous Flink-style assignment: `subtask = key_group * p / max_p`.
/// `start` inclusive, `end` exclusive. `max_parallelism` is immutable for a
/// pipeline incarnation.
pub struct KeyGroupRange {
    pub start: usize,
    pub end: usize,
}

pub struct Cursor {
    pub ts: i64,
    pub seq_no: u64,
}
pub struct RawRun {
    pub from: Cursor,
    pub to: Cursor,
}
pub struct TileRun {
    pub granularity: TimeGranularity,
    pub start_ts: i64,
    pub end_ts_exclusive: i64,
}
pub enum TimeGranularity {
    Milliseconds(u32),
    Seconds(u32),
    Minutes(u32),
    Hours(u32),
    Days(u32),
    Months(u32),
}
pub type WindowId = usize;
pub type AccumulatorState = Vec<ScalarValue>;
pub struct KeyEvaluationState {
    pub through: Cursor,
    pub accumulators: BTreeMap<WindowId, AccumulatorState>,
}
pub struct KeyState {
    pub next_seq: u64,
    pub evaluation: Option<KeyEvaluationState>,
}
pub enum WindowTriggerKind {
    RowEmit,
    WindowEnd { window_id: WindowId },
}
pub struct WindowTrigger {
    pub fire_at: Cursor,
    pub partition: PartitionKey,
    pub kind: WindowTriggerKind,
}
pub struct DueWindowWork {
    pub partition: PartitionKey,
    pub key_state: KeyState,
    pub triggers: Vec<WindowTrigger>,
}
/// Opaque to the operator. Only the backend that produced it should pass it back.
pub struct TriggerResume {
    pub last: WindowTrigger,
    /// Last raw clustering `(attempt, epoch)` when the store distinguishes
    /// overlay-hidden rows from visible ones. Empty attempt = last visible.
    pub raw_attempt: Vec<u8>,
    pub raw_epoch: i64,
    /// Scylla `(bucket, shard)` walk index. InMem ignores this.
    pub part_idx: usize,
}
pub struct TileState {
    pub accumulator_state: Option<AccumulatorState>,
}
pub struct WindowTiles {
    pub windows: BTreeMap<WindowId, TileState>,
}
pub type TileMap =
    BTreeMap<(TimeGranularity, i64 /* tile_start */), WindowTiles>;
pub struct WindowData {
    raw_batches: Vec<RecordBatch>,
    tile_map: TileMap,
}
pub struct WindowStateSnapshot {
    pub namespace: Vec<u8>,
    pub watermark_frontier: Option<i64>,
    pub backend: WindowBackendSnapshot,
}
```

Routing (already landed):

```text
key_group = hash % max_parallelism
subtask   = key_group * p / max_parallelism
```

Each WO/WRO task owns a contiguous `KeyGroupRange`. `max_parallelism` is
job-wide and must not change for a pipeline incarnation.

- `StateNamespace` is the operator state space, not a task. Task isolation is
  the owned key-group range, bound on the per-task store client at open (see
  below). Changing only Scylla to strip `task_index` while InMem still keys on
  `for_operator_task(..., task_index)` would give the same type two meanings;
  both backends follow this contract.
- `PartitionKey` must remain collision-safe; hashes alone are insufficient.
  Persist the serialized business-key bytes. Derive `key_group` from `Key.hash`
  (first 8 bytes of `Key::to_bytes`) and `max_parallelism`. Scylla hashes the
  complete partition key for distribution.
- `Cursor` is raw event identity and total order within a partition.
- `RawRun` and `TileRun` are half-open. Calls contain merged logical runs, and
  backends must return exactly their union.
- `next_seq` allocates per-key sequence IDs. Optional `evaluation` keeps the
  last fired trigger together with retractable accumulator state. State-only WO
  does not keep evaluation state.
- `watermark_frontier` is the **task-level** late-data boundary (each task
  advances its own watermark over the keys it owns). Logical retention is
  derived from it, the largest window, and configured lateness.
- `WindowTrigger` is durable event-time work. Current RANGE windows create one
  `RowEmit` trigger per accepted row; `WindowEnd` is reserved for scheduled
  windows.
- `TriggerResume` is an opaque `load_triggers` cursor. Scylla seeks from the
  last **raw** clustering row (even if overlay-hidden), not the last visible
  trigger. InMem resumes from the last visible trigger.
- Tiles for all windows share `(granularity, tile_start)`, so persisted tile and
  accumulator state retain `WindowId`.
- `WindowData` is one materialized WRO snapshot. Evaluation filters its rows by
  cursor; batches need not be mapped back to individual runs.

## Worker state topology

Two layers:

```text
physical backend     worker + OperatorKind (one InMem map / Scylla session)
WindowOperatorStore  per-task client wrapping that backend
```

The client is created at WO `open` from the task's assignment and is stable
for the attempt:

```text
StateNamespace  = (pipeline, owner operator)
max_parallelism = job-wide, bound on the client
KeyGroupRange   = groups for this task_index at (p, max_p)
WriterId        = this task execution
AttemptToken    = job-level execution attempt
```

The client derives `key_group` from `Key.hash` and bound `max_p`. It never
trusts a caller-supplied group. `load_triggers` / `checkpoint` / `restore` use
the bound range and do not take `owned` or `namespace`. Per-key calls must
land in the bound range (routing bug otherwise). The operator helper
`stream_due` pages `load_triggers` and is not a store method.

WRO reuses the WO owner namespace. It addresses rows by `PartitionKey`
(namespace + business key). Request routing still sends a lookup to the WRO
task that owns that key (locality); the namespace no longer encodes
`task_index`.

`OperatorStore` is the generic maintenance port on the **shared** physical
backend (registry: one store per kind). The worker cleaner runs one tick:

1. for each registered kind in parallel,
2. `try_join_all` over that kind's task states,
3. `store.maintain(ns, task_state)`.

Window eligibility is read from `OperatorTaskState` (watermark →
`retention_cutoff`, plus the same key-group range the WO client was bound to).
There is no separate cut/meta publish on the data-plane trait. Pipeline
`StateSpec.maintenance_*` (default on) enables the cleaner.

See the [window operator README](README.md) for the runtime retention contract
and how InMem implements `maintain` today. Scylla's `maintain` body is still
proposed below.

## Store traits

```rust
#[async_trait]
pub trait WindowOperatorStore: Send + Sync + Debug {
    async fn load_key_state(&self, partition: &PartitionKey) -> Result<KeyState>;
    async fn load_raw(
        &self,
        partition: &PartitionKey,
        runs: &[RawRun],
    ) -> Result<Vec<RecordBatch>>;
    async fn load_tiles(
        &self,
        partition: &PartitionKey,
        runs: &[TileRun],
    ) -> Result<TileMap>;
    async fn commit_events(
        &self,
        partition: &PartitionKey,
        ts_column_index: usize,
        events: &RecordBatch,
        tiles: &TileMap,
        state: &KeyState,
        triggers: &[WindowTrigger],
    ) -> Result<()>;
    async fn load_triggers(
        &self,
        after: Option<Cursor>,
        through: Cursor,
        resume: Option<&TriggerResume>,
        limit: usize,
    ) -> Result<(Vec<WindowTrigger>, Option<TriggerResume>)>;
    async fn store_key_state(
        &self,
        partition: &PartitionKey,
        state: &KeyState,
    ) -> Result<()>;
    /// Complete pending writes before capturing the returned snapshot.
    async fn checkpoint(&self) -> Result<WindowBackendSnapshot>;
    async fn restore(&self, snapshot: &WindowBackendSnapshot) -> Result<()>;
}
#[async_trait]
pub trait WindowRequestStore: Send + Sync + Debug {
    async fn load_window_data(
        &self,
        partition: &PartitionKey,
        raw_runs: &[RawRun],
        tile_runs: &[TileRun],
    ) -> Result<WindowData>;
}
```

### Required behavior

- Missing partitions and empty runs return empty/default results.
- Raw rows are restricted to the requested runs, deduplicated, and globally
  ordered by `Cursor`. Their Arrow schema includes `__seq_no: UInt64`. The
  Scylla backend stores **one CQL row per event**, clustered by time; it does
  not pack events into a serialized batch blob.
- Tiles are unique by `(granularity, tile_start)` and restricted to requested
  runs. Missing tiles mean empty intervals.
- `commit_events` atomically publishes raw rows, replacement tiles, key state,
  and triggers. `ts_column_index` plus `__seq_no` identifies each raw cursor.
  Retries are idempotent. The backend derives `key_group` from the partition's
  business key; it does not trust a caller-supplied group.
- `load_triggers` returns a bounded page of **visible** triggers in one
  stable `(after, through]` plus an opaque resume, or none when the range is
  exhausted. It only returns work for the client's bound key-group range.
  The store does not group by key or load `key_state`. The operator helper
  `stream_due` pages this call, groups by key, and attaches `key_state`.
  Fetch size is `window.process_page_size` (4096), floored at
  `process_key_concurrency`. Paginate only when a hop exceeds the cap.
  Do not derive fetch size from key concurrency. Prefetch is later
  ([#297](https://github.com/volga-project/volga/issues/297)).
- `store_key_state` publishes only sequence/evaluation state.
- `checkpoint` completes pending writes and returns a backend-specific
  snapshot of the client's bound range only. Scylla returns this writer's
  `StateVersion`; InMem serializes partitions in that range.
- `restore` starts store access from the supplied checkpoint base and must
  not clobber keys outside the bound range (the physical InMem/Scylla backend
  is shared by all tasks of the kind).
- WO reads observe their latest publication. One fenced WO owns a partition, so
  its historical raw and tile reads need not share a snapshot and can run
  concurrently.
- WRO: Each `load_window_data` is one coherent published snapshot across all
  physical buckets and pages.
- Methods must not broaden ranges or return partial results.

`InMemWindowStore` is the reference physical backend. One lock provides the
WRO snapshot boundary. Each WO task holds a client over that shared map,
bound to namespace + key-group range; checkpoint embeds serialized partition
state and triggers for that range. The shared backend implements
`OperatorStore::maintain` and prunes eagerly under the same retention contract
as Scylla (control flow identical; lag ~0), reading the range from
`OperatorTaskState` rather than a task-suffixed namespace blob.

**Land the engine contract and InMem before any Scylla backend.** Today
`StateNamespace` still includes `task_index`, and restore/maintain key on that
blob. Operator-scoped namespace plus range isolation must ship on the traits
and InMem first (two tasks, one worker: restore/maintain of task 0 must not
clobber task 1). Otherwise the same type has two meanings and shared-worker
restore wipes sibling keys. Scylla implements that already-landed contract.

## Runtime flows

WO ingest:

1. Load key state, drop rows at or behind the task watermark, and assign
   sequence IDs.
2. Load and update affected tiles.
3. For emitting WO, create one `RowEmit` trigger per accepted row.
4. Atomically `commit_events(..., events, tiles, state, triggers)`. State-only
   WO publishes raw rows and tiles without row triggers.

WO advance:

1. Stream backend-sized trigger pages for
   `(watermark_frontier, incoming_watermark]` (client already scoped to this
   task's key groups).
2. Group each page by key and use every trigger cursor as an exact emit point.
3. Build per-cursor plans. Sliding aggregates plan leave bands; rebuild
   aggregates plan raw edges plus interior tiles.
4. Merge emit-row and historical coverage, then load raw rows and tiles
   concurrently.
   Small ranges remain all-raw. Large slide leave bands may also use tiles.
5. Evaluate using those same plans. New rows are always raw; rebuild interiors
   do not overfetch raw rows.
6. Publish updated `KeyEvaluationState` with `store_key_state`.
7. Advance and forward the task watermark only after all due work succeeds.
   Physical prune is not awaited here; the worker cleaner applies it via
   `maintain`.

Tile-based slide retraction is limited to aggregates whose states can be
subtracted safely (`SUM`, `COUNT`, and `AVG`). Other sliding aggregates retract
raw leave-band rows. Rebuilds may use mergeable tile states for any aggregate.

WRO lookup:

1. Both plain and retractable aggregates use rebuild plans, since WRO has no
   prior accumulator.
2. Build and merge exact raw-edge and tile plans for all points and windows.
3. Call `load_window_data` once for one coherent snapshot.
4. Evaluate per-window request arguments and rebuild every answer.

WRO is read-only.

## Scylla backend

Scylla does not provide one snapshot spanning multiple partitions, CQL queries,
or result pages. A logical window read may cross several physical ranges, so
concurrent writes could otherwise make those pieces represent different
moments. We use application-level MVCC to pin every piece to one published
version and provide a coherent logical snapshot. We also use time buckets to
keep physical partitions and individual range reads bounded instead of storing
a key's entire history in one ever-growing partition.

v1 restore still assumes **same task assignment** (`RestorePlanner` identity
map). The physical model is key-group-native from the start so later rescaling
is a remap of ranges between checkpoints, not a data rewrite. `task_index`
does not appear in PRIMARY KEYs.

### Versions and checkpoints

```rust
/// Job-level recovery branch. Not a task id.
/// Example: pipeline incarnation + execution_attempt_id.
pub struct AttemptToken(Vec<u8>);

/// Unique WO task execution. Head fence only, not part of data PKs.
/// Example: AttemptToken + vertex/task identity.
pub struct WriterId(Vec<u8>);

pub struct StateVersion {
    pub attempt: AttemptToken,
    pub epoch: u64,
}

/// Restore/remap only. Not stored in the checkpoint blob.
/// Planner output, then rows in `window_recovery_bases`.
pub struct VersionedRange {
    pub range: KeyGroupRange,
    pub version: StateVersion,
}

pub enum WindowBackendSnapshot {
    InMemory { snapshot: Vec<u8> },
    /// This writer's cutoff. Range is not stored: it is the client's bound
    /// assignment, and on the master the payload is already keyed by task.
    Versioned { version: StateVersion },
}
```

There is **one epoch**: a per-writer monotonic publish id. Each successful
`commit_events` / `store_key_state` does `E += 1` and stamps that `E` on the
write. It is not a per-key or per-key-group clock.

| Place | Role |
| --- | --- |
| Writer allocator | produces `E` |
| Data rows | MVCC: this cell belongs to publish `E` |
| Head `writer_(attempt, epoch)` | optional watermark of this key's latest **writer** publish |
| Head `serving_(attempt, epoch)` | WRO/GC pin — the only cut request reads. Not updated on ingest unless the publish policy says so |
| Checkpoint cutoff | include versions with `epoch <= E` for keys in this range |

Head `writer_epoch=5` on key A and `7` on key B means A and B were last
published at those points on the **same** writer counter. Checkpoint `E=7`
means “every owned key, newest row with `epoch <= 7`.”

`AttemptToken` in table keys and in `StateVersion` is job-level. All WO tasks
of one execution share it and write different key groups. Numeric epochs may
repeat across writers (`E=3` on task 0 and task 1); that is fine because they
are different keys. A cutoff is applied only to the groups that writer owned —
the master already stores one checkpoint payload per task, so the blob does
not need to repeat the range.

`WriterId` is stored only on the head (`owner_writer`). It is the lock holder
for zombie fencing, not a data address.

A new execution attempt may restart writers at epoch zero because
`(attempt, epoch)` is unique. Scylla stores the two fields separately.

`VersionedRange` is not a checkpoint record. Checkpoint persists one
`StateVersion` for this writer. The owned `KeyGroupRange` is already known
from the client's assignment (and, on the master, from `TaskKey` + graph
`p` / `max_p`). After a future rescale, `RestorePlanner` intersects old task
ranges with the new assignment and produces a `Vec<VersionedRange>` as the
**restore instruction**. `restore()` writes those slices into
`window_recovery_bases`. v1 same-assignment is the trivial case: one slice =
bound range + the checkpoint's `StateVersion`.

Scylla uses `WindowBackendSnapshot::Versioned`. InMem uses `InMemory` with a
serialized snapshot of the owned range. Namespace and the last fully processed
watermark remain in the operator's `WindowStateSnapshot` envelope.

### Tables

`attempt` is **not** in any data partition key. It is clustering (or a
column) so recovery overlay is a visibility filter on one `LOCAL_QUORUM`
read, not a second partition hop. Restore still does not copy rows.
`maintain` drops unreachable generations so the filter stays one or two
attempt clauses.

**GC default: skinny index + per-key data PK. Do not cluster `business_key`
under `(namespace, key_group, bucket)` on the data tables.** `load_raw` is
per-key; a key group is unbounded, so that clustering would make one data
partition (compaction, repair, hot token) hold every key in the group+bucket.
Keep `business_key` in the **data** partition key so each key+bucket stays
small.

That data layout cannot list expired buckets, so `commit_events` also writes
one payload-free index. **`bucket_start` is clustering on the index, not a
partition-key component** — otherwise `maintain` cannot range-scan buckets
and would have to probe historical bucket ids (unbounded). Do not add a
second `live_buckets` table. Bind `floor_bucket` = first bucket that still
overlaps `data_floor` (`align_down(data_floor, width)`). Only drop buckets
whose entire range is below the floor: `bucket_start + width <= data_floor`,
i.e. `bucket_start < floor_bucket`. Do not bind `data_floor` as if it were a
`bucket_start`.

After prune, each index partition is one key group and holds only live
buckets: `keys_in_group × (retention / bucket_width)`, skinny cells. Shard
with `index_shard` on the PK later if a hot group is too wide — not v1.
TWCS/TTL on write-time cannot replace this: event-time buckets ≠ write time,
and replay rewrites old buckets.

```sql
CREATE TABLE window_head (
    namespace blob,
    key_group int,
    business_key blob,
    owner_writer blob,
    writer_attempt blob,
    writer_epoch bigint,
    serving_attempt blob,
    serving_epoch bigint,
    PRIMARY KEY ((namespace, key_group, business_key))
);

-- One row per inherited range (usually one at same assignment).
CREATE TABLE window_recovery_bases (
    namespace blob,
    recovery_attempt blob,
    range_start int,
    range_end int,
    base_attempt blob,
    base_epoch bigint,
    PRIMARY KEY ((namespace, recovery_attempt), range_start)
);

-- Skinny GC index only: no payloads, no attempt/epoch.
-- bucket_start is clustering so maintain can range `< data_floor`.
-- Do not add a second live_buckets table. Shard with index_shard later
-- if a hot group is too wide — not v1.
CREATE TABLE window_kg_buckets (
    namespace blob,
    key_group int,
    bucket_start bigint,
    business_key blob,
    PRIMARY KEY ((namespace, key_group), bucket_start, business_key)
) WITH CLUSTERING ORDER BY (bucket_start ASC, business_key ASC);

CREATE TABLE window_raw (
    namespace blob,
    key_group int,
    business_key blob,
    bucket_start bigint,
    event_ts bigint,
    seq_no bigint,
    attempt blob,
    epoch bigint,
    payload blob,
    PRIMARY KEY (
        (namespace, key_group, business_key, bucket_start),
        event_ts, seq_no, attempt, epoch
    )
) WITH CLUSTERING ORDER BY (
    event_ts ASC, seq_no ASC, attempt ASC, epoch DESC
);

CREATE TABLE window_tiles (
    namespace blob,
    key_group int,
    business_key blob,
    granularity_ms bigint,
    bucket_start bigint,
    tile_start bigint,
    attempt blob,
    epoch bigint,
    payload blob,
    PRIMARY KEY (
        (
            namespace,
            key_group,
            business_key,
            granularity_ms,
            bucket_start
        ),
        tile_start, attempt, epoch
    )
) WITH CLUSTERING ORDER BY (tile_start ASC, attempt ASC, epoch DESC);

CREATE TABLE window_key_states (
    namespace blob,
    key_group int,
    business_key blob,
    attempt blob,
    epoch bigint,
    key_state blob,
    PRIMARY KEY (
        (namespace, key_group, business_key),
        attempt, epoch
    )
) WITH CLUSTERING ORDER BY (attempt ASC, epoch DESC);

CREATE TABLE window_triggers (
    namespace blob,
    bucket_start bigint,
    kg_shard int,
    fire_ts bigint,
    fire_seq bigint,
    business_key blob,
    trigger_kind tinyint,
    window_id bigint,
    key_group int,
    attempt blob,
    epoch bigint,
    PRIMARY KEY (
        (namespace, bucket_start, kg_shard),
        fire_ts,
        fire_seq,
        business_key,
        trigger_kind,
        window_id,
        attempt,
        epoch
    )
) WITH CLUSTERING ORDER BY (
    fire_ts ASC,
    fire_seq ASC,
    business_key ASC,
    trigger_kind ASC,
    window_id ASC,
    attempt ASC,
    epoch DESC
);
```

- `window_head`: one row per key (not versioned by attempt). `owner_writer` is
  the fence. `serving_*` is the published snapshot WRO always pins.
  `writer_*` may lag serving during recovery. Point get by
  `(namespace, key_group, business_key)`. Do not put `attempt` in this PK —
  that stores many names and the zombie race moves to the reader.
- `window_recovery_bases`: immutable rows mapping a new job `recovery_attempt`
  to restored checkpoint cutoffs at **key-group range** granularity. Lookup for
  a key: find the row with `range_start <= key_group < range_end`. This lets a
  new execution inherit unchanged state without copying raw rows, tiles, or
  `KeyState`, and lets a later rescale inherit different cutoffs from different
  source tasks.
- `window_kg_buckets`: skinny GC index, not a source of truth for reads.
  One table. `commit_events` does an unversioned upsert of `business_key`
  (no `attempt`/`epoch`, no payload). A failed serving promote may leave a stale
  index row; `maintain` deletes it. `maintain` per owned `key_group`:

  ```cql
  SELECT * FROM window_kg_buckets
   WHERE namespace = ? AND key_group = ?
     AND bucket_start < ?   -- floor_bucket, not data_floor
  ```

  For each hit, delete that key's `window_raw` partition for the bucket
  **and** every `window_tiles` partition for the same key+bucket (all
  `granularity_ms` from task state). Then delete the index row. After prune,
  the partition holds only live buckets. `index_shard` on the PK is later,
  not v1.
- `window_raw`: **one CQL row per event**, clustered by event time. `payload`
  is that event only (Arrow-IPC including `__seq_no`). Do not pack a batch
  into one blob — that breaks time clustering, overlay, and per-event GC.
- `window_tiles`: versioned aggregate tiles, partitioned by key, granularity,
  and time bucket. Overlay filters `attempt` / `epoch` in the same partition.
- `window_key_states`: versioned `KeyState` in one partition per key. Overlay
  is a clustering filter, not another hop.
- `window_triggers`: immutable due work. Locality is
  `(namespace, bucket_start, kg_shard)`. Cluster by `fire_ts` first so
  `load_triggers` can slice `(after, through]` without `ALLOW FILTERING`.
  `key_group` is a regular column; the client drops rows outside the bound
  range (`kg_shard` already limits how many groups share a partition).
  Shard with the **same range formula as routing**, not
  `hash(business_key) % N`:

  ```text
  kg_shard = key_group * SHARD_COUNT / max_parallelism
  ```

  `SHARD_COUNT` is a store constant (e.g. 32 or 64), not `p` (changes on
  rescale) and not `max_parallelism` (too fine: `p=4`, `max_p=32768` would
  otherwise mean 8192 queries per time bucket). `SHARD_COUNT` and
  `max_parallelism` are immutable for the namespace. Row triggers use a
  sentinel `window_id`; scheduled windows retain their actual `WindowId`.

### Head semantics

`window_head` is a singleton lease, not an MVCC table. Data rows are versioned
by `(attempt, epoch)` in their primary keys; overlay fences zombie **data**.
Head is the **serving name**. A last-write-wins upsert lets dying attempt N
overwrite `serving_*` after N+1 is live; WRO follows that pin. Owner LWT is
the fence for that cell, not a workaround for a missing clustering key.

Three fields, three jobs — never one LWT that does all three on ingest:

| Column | Role |
| --- | --- |
| `owner_writer` | Who may change serving. Fence. |
| `writer_(attempt, epoch)` | Optional writer watermark. |
| `serving_(attempt, epoch)` | Snapshot WRO always `SELECT`s. |

`owner_writer` is length-prefixed `attempt || vertex` (`WriterId` on the
scope stays vertex). Claiming is **only** `SET owner_writer`:

- empty key: `INSERT … IF NOT EXISTS` (may also set serving — there is no
  prior cut);
- restore, first touch: `UPDATE … SET owner_writer = me IF owner_writer =
  previous`. **Do not** change `serving_*`.

CAS not applied fences; do not fall through to a second LWT. Restore is a
key-group range, head is per key, so steal is **lazy on first touch**, not a
range scan at restore.

WRO always pins `serving_*` (one hop + payload). It does not discover epoch
from `key_state` and does not read writer overlay.

**Publish policy** (`ScyllaConfig.serving_publish`, same `promote_serving`
LWT: `SET serving_* IF owner = me`). Cadence is the freshness / ingest-p99
knob; it is not fencing. Catch-up freeze still wins: do not promote serving
while N+1 is behind the pinned cut.

| Policy | Ingest wait path | WRO freshness |
| --- | --- | --- |
| `on_commit` (default) | data + promote LWT after catch-up | last ingest |
| `interval: { interval_ms }` | data only; promote this key when the interval elapses | last promote, at most `interval_ms` lag (continuous ingest) |
| `checkpoint` | data only | last checkpoint flush |

Checkpoint always flushes serving for live keys (so a barrier is WRO-visible
even under `interval` / `checkpoint`). Empty-key `INSERT IF NOT EXISTS` still
names the first snapshot so WRO is not empty.

A background timer is not required: interval is checked on the next ingest
of that key, and checkpoint covers idle keys. Do not spawn a publisher
during restore freeze.

### WO write

For one key:

1. Load writer `KeyState` (overlay). On first access after restore, steal
   `owner_writer` without moving serving.
2. Allocate writer epoch `E`.
3. Derive `key_group` from the key hash and the client's `max_p`.
4. Write changed raw rows, tiles, `KeyState`, and triggers under
   `(job attempt, E)`. Upsert `window_kg_buckets` unversioned (not under
   `E`; a later failed promote may leave a stale index row for `maintain`).
5. **Empty key:** after data is durable, `INSERT IF NOT EXISTS` with serving
   `E` (first snapshot).
6. **Owned, not recovering (or catch-up complete):** `promote_serving(E)`
   according to `serving_publish` (`on_commit` waits; `interval` /
   `checkpoint` skip until due). During recovery freeze, skip until writer
   `KeyState` has caught the pinned serving `KeyState` (`next_seq` and, if
   present, `evaluation.through`).
7. Update or invalidate affected WO cache entries.

Each `(key, bucket)` (and each tile partition) is one Scylla partition.
`commit_events` issues one **UNLOGGED BATCH** per such partition — one
mutation per event/tile row, one RTT per partition, not one RTT per event.
Do not use logged BATCH. Do not batch across partitions (no cross-partition
atomicity, coordinator penalty). Tiles, triggers, and other buckets are
separate requests.

Serving LWT is the WRO visibility boundary. It is **not** coupled to ingest
except under `OnCommit` after catch-up. Data rows are already stamped with
`E`; WO reload does not need head. Orphaned data from a failed promote is
invisible to WRO until a later promote names that epoch. A new epoch starts
only after the previous write outcome is known. Ownership CAS failure stops
the WO as fenced. Trigger rows from an unpublished attempt/epoch are not
returned by `load_triggers` (overlay).

`store_key_state` follows the same protocol but writes no raw, tile, trigger,
or bucket-index rows.

### WO reads

WO reads the writer snapshot. A fresh execution attempt reads only its own
versions. A recovery attempt overlays its versions over the immutable
checkpoint base from `window_recovery_bases`. Because `attempt` is clustering,
that overlay is one `LOCAL_QUORUM` read per data partition: keep the newest
row that matches the recovery attempt, else the base attempt with
`epoch <= base_epoch`. No extra partition hop, and restore still does not
copy rows.

Logical runs are mapped to time buckets, loaded, merged, and filtered back to
the exact requested ranges. Raw rows are deduplicated by `Cursor`; current
tiles replace matching base tiles.

`load_triggers` maps its stable watermark range to trigger buckets and the
`kg_shard`s that overlap the client's bound range. One hop is `LIMIT` plus
optional clustering seek `>` the last **raw** row (full tuple, not `fire_at`
alone). Overlay-hidden rows still advance the seek so they are not
re-read. The next `(bucket, shard)` resets the seek to `after`.

```text
-- first page of a shard
WHERE namespace = ? AND bucket_start = ? AND kg_shard = ?
  AND fire_ts >= ? AND fire_ts <= ?
LIMIT ?

-- continue inside the same shard
WHERE namespace = ? AND bucket_start = ? AND kg_shard = ?
  AND (fire_ts, fire_seq, business_key, trigger_kind, window_id, attempt, epoch)
      > (?, ?, ?, ?, ?, ?, ?)
  AND fire_ts <= ?
LIMIT ?
```

The client drops `key_group` outside the bound range and applies the same
attempt/epoch visibility filter as raw overlay. Resume is
`(last raw row as a WindowTrigger, raw_attempt, raw_epoch, part_idx)`.
Empty `raw_attempt` means start of the current shard (no seek).

Do not use `OFFSET`, native `PagingState`, seek on `fire_ts` alone, or
`execute_unpaged` of the whole range.

The operator helper `stream_due` pages `load_triggers`, groups by key,
loads writer `KeyState`, and yields `DueWindowWork`. It does not live on
the store trait. Watermark still advances only after the full
`(after, through]`. Prefetch is later
([#297](https://github.com/volga-project/volga/issues/297)).

### WRO reads

WRO:

1. Reads and retains the key's serving version for the request (the pin
   `maintain` must honor).
2. Builds exact raw/tile plans.
3. Loads serving-attempt data from the same partitions, filtering clustering
   `attempt` / `epoch`. If that attempt has a recovery-base covering this
   `key_group`, the base generation is visible in those same partitions.
   Chain only as many recovery-base generations as `maintain` has not yet
   dropped (target: 1–2 clauses).
4. Selects visible versions, then merges, orders, deduplicates, and rebuilds.

WRO does not read writer state, use Foyer, contact the master, or consume
checkpoint metadata. If WO is unavailable, WRO continues serving the unchanged
complete serving snapshot. Any WRO task can read any key in the operator
namespace; keyed routing is locality, not identity.

### Checkpoint

At an aligned barrier:

1. Resolve all in-flight publication outcomes and complete pending writes.
2. Capture `(current job attempt, current writer epoch)`.
3. Return `WindowBackendSnapshot::Versioned { version }`. The operator
   persists it in its checkpoint envelope. The range is the client's bound
   assignment and is not copied into the blob.
4. Continue processing at later epochs.

The checkpoint contains no keys, no key-group list, and no state payloads.
For any key this writer owned, checkpoint state is its newest version from
that attempt with `epoch <=` the cutoff. Creating a checkpoint does not insert
a recovery-base row; those rows are created only when a new attempt restores.

The operator stores namespace, its last fully processed watermark, and
`WindowBackendSnapshot` in `WindowStateSnapshot`. Durable triggers already
represent work above that watermark, so checkpointing neither drains nor
serializes an operator-local pending-key set. For unchanged task assignment,
restore passes the same `StateVersion` back; the client applies it to its
bound range. `RestorePlanner` remapping (producing `Vec<VersionedRange>`) is
future work.

### Recovery

1. v1: the replacement worker has `WindowBackendSnapshot::Versioned { version }`
   and the same assignment, so one `VersionedRange` = bound range + that
   version. After rescale: `RestorePlanner` sends the intersected
   `Vec<VersionedRange>` (still not a checkpoint format; it is restore input).
2. During operator restore, the worker's Scylla store uses the new **job**
   attempt, inserts `window_recovery_bases` rows
   `(new attempt, range) -> (base_attempt, base_epoch)` for each restore
   slice, and starts this writer's epoch at zero. The rows must exist before
   the attempt publishes data. The master only plans and sends the restore
   payload.
3. Source restores its checkpoint offset and replays post-checkpoint input.
4. On first access to a key, steal `owner_writer` (CAS). Leave `serving_*` at
   the previous cut (or `restore_base` if head had never been published).
   Restore writer `KeyState` from the checkpoint overlay for that key's range.
5. Resume watermark work by streaming checkpoint-visible triggers above the
   restored watermark while replay advances only writer state. Do not promote
   serving.
6. Once append-only replay catches the old serving `KeyState`, `SET serving_*
   IF owner = me`.
7. Continue under `serving_publish` (`on_commit` / `interval` / `checkpoint`).

During recovery, writer data and serving pins differ. Replay advances MVCC
rows; WRO keeps using `serving_*`. Compare the writer `KeyState` with the
key_state row at the pinned serving version. Once `next_seq` and
`evaluation.through` reach that state, promote. Keys without evaluation
state compare only `next_seq`. Keys not touched after recovery keep their
old serving snapshot. A zombie N publish after steal does not apply.

### WO cache

Foyer is WO-only:

```text
meta:
    PartitionKey -> (writer version, KeyState)
data:
    (PartitionKey, family, bucket) -> materialized writer-view data
triggers:
    (namespace, bucket, kg_shard) -> immutable due entries
```

`family` is raw data or tiles at a specific granularity.

The cache is cleared before each execution attempt. On a miss, run the normal
WO bucket read and cache its materialized result. Successful writes replace or
invalidate affected data buckets. Immutable trigger buckets may be cached and
paged without becoming a separate source of truth.

`writer_version` remains in the meta value because the next CAS needs it, but
it is not part of the cache key.

It needs point get, put, invalidation, and optionally batched gets. Logical
scans are assembled from bucket point reads. WRO bypasses cache.

### Scylla consistency

Minimal setup is single DC, RF=3. Because we need read-your-write after publish,
use `LOCAL_QUORUM` for both data reads and writes (`W+R > RF`). Head claim/publish
is LWT with `LOCAL_SERIAL` + learn `LOCAL_QUORUM`:

- **WO write:** non-LWT data @ `LOCAL_QUORUM`. Owner steal is one LWT on first
  touch (fence, no serving change). Serving promote is a separate LWT on the
  configured cadence (`on_commit` / `interval` / `checkpoint`), never during
  catch-up freeze.
- **WO read (cache miss):** overlay on versioned tables @ `LOCAL_QUORUM`
  (not the serving pin).
- **WRO read:** head pin + data @ `LOCAL_QUORUM` (serving pointer) for one
  coherent snapshot. Always this path; do not derive the pin from `max(epoch)`.

**Not v1:** `USING TIMESTAMP` instead of owner CAS; derived-from-`key_state`
WRO pins.

### State prune / cleanup

**Eligibility (per task):** after watermark advance to `W`,
`data_floor = W − max_window_length − lateness` (lateness default `0`).
`max_window_length` is required because raw/tiles are shared across all window
expressions on the op — shorter windows over-retain; longer ones must not
under-retain. Prune is limited to the task's owned key-group range.

**Executor (per worker):** `StateRegistry::run_maintenance_once` — parallel
loop per `OperatorKind`, then per-task `OperatorStore::maintain(ns, state)`.
Not on the master; not inside WO `poll_next`. The window impl reads cutoff and
owned range from `OperatorTaskState`.

**Window `maintain` body:**
- drop consumed triggers (`fire_at.ts ≤ W`) with a `fire_ts` clustering
  range delete on shards overlapping the task's key-group range;
- from `window_kg_buckets`, per owned `key_group`,
  `SELECT … AND bucket_start < floor_bucket` (`floor_bucket =
  align_down(data_floor, width)`; do not bind `data_floor` — a bucket with
  `bucket_start < data_floor` can still cover `[data_floor, data_floor +
  width)`). Page the result. For each `(business_key, bucket_start)`, delete
  the per-key `window_raw` partition and every `window_tiles` partition for
  that key+bucket (each `granularity_ms` from task state), then the index
  row (including stale rows from a failed serving promote). Do not probe historical
  bucket ids. Do not rely on TWCS/TTL (write-time ≠ event-time; replay
  rewrites old buckets);
- drop unreachable MVCC generations for keys still present in **live**
  `window_kg_buckets` rows for the owned groups (`window_head` is per-key and
  cannot be scanned). Drop `attempt` / `epoch` not reachable from writer
  heads, serving heads, recovery bases, or retained checkpoints;
  orphan/zombie writes. After this, overlay filters stay one or two attempt
  clauses.

**In-flight WRO:** `load_window_data` pins the key's serving version at
start. `maintain` must not drop that version while the request can still
read it. v1: never delete the current serving pointer; delay dropping a
just-superseded serving version by at least the maximum WRO request
budget (grace). No pin table.

TTL/TWCS may expire physical SSTables only when consistent with logical
`data_floor` **and** the serving-pin grace. InMem applies the same logical
rules immediately inside `maintain`, iterating in-memory partitions whose
namespace matches and whose `key_group` is in the task state's range.

## Future next steps

**Prerequisite:** operator-scoped `StateNamespace`, per-task store client
(`max_p`, key-group range), and InMem restore/maintain isolation (two tasks
on one worker, no clobber). Then Scylla.

Worker maintenance **orchestration** (`OperatorStore::maintain`, cleaner loop,
`StateSpec.maintenance_*`) is decided in the runtime; Scylla's `maintain`
implementation remains TODO. v1 `RestorePlanner` may keep identity mapping.

### Namespaced cache quota

Assign each state consumer its own memory and local-disk quota. This prevents
one operator or namespace from consuming the shared cache and gives concurrent
consumers a predictable fair share of local resources. The worker
`StateResourceTracker` is the scaffold for charging/releasing those quotas.

### Mem pressure / backpressure

Track both local cache pressure (mem+disk) and logical remote-state usage. If eviction or
spill cannot keep a consumer within its cache quota, or its retained remote
state reaches a configured logical limit, backpressure its upstream tasks.
Remote state must not act as a bottomless overflow sink; these limits make
state growth visible to normal flow control. `StateResourceTracker` will surface
pressure signals; maintenance paths must not block on admission.

### CDC and late events

The current flow is append-only. An event is accepted while its timestamp is
ahead of the task watermark; an event at or behind that frontier is dropped.
`lateness` only extends retention and does not provide a late-update grace
period or revise already emitted results.

Future late-event and CDC support must identify logical rows independently of
their arrival cursor and represent inserts, updates, and deletes explicitly.
WO would retain enough raw history, invalidate or rebuild affected tiles and
windows, and publish corrections through the same atomic version boundary.
The backend would add row identity, revision/operation metadata, and
versioned tombstones while cleanup would preserve data needed by the allowed
lateness interval. WRO would continue reading one coherent serving version;
only the materialization and merge rules would change.

### Rescaling

Rescaling remaps `key_group ↔ task` between checkpoints. Data rows stay put:
PKs are task-agnostic. `max_parallelism` is fixed for the pipeline incarnation;
only `p` changes. At any one `p`, `subtask = key_group * p / max_p` **partitions**
`[0, max_p)` into disjoint contiguous ranges — two live tasks never own the
same key group, so they never “own an intersection” of each other.

What *does* intersect is **new range ∩ old range** in the planner, when `p`
changes. Each old task’s checkpoint blob is only a `StateVersion`. The old
range is recomputed from that task’s index and the checkpoint graph’s
`(p, max_p)`. `RestorePlanner` intersects the new assignment with every old
task range and emits a `Vec<VersionedRange>` restore instruction — disjoint
slices that cover exactly the new task’s groups, each with its source cutoff.
`restore()` writes those into `window_recovery_bases`. The checkpoint blob
never stores a range.

Example, `max_p = 128`, rescale `p=3 → p=2`:

```text
old p=3:  task 0 [0, 43)   task 1 [43, 86)   task 2 [86, 128)
new p=2:  task 0 [0, 64)   task 1 [64, 128)
```

```text
new task 0 ← [0, 43) @ old0 version  +  [43, 64) @ old1 version
new task 1 ← [64, 86) @ old1 version  +  [86, 128) @ old2 version
```

Old task 1 is **split** across both new tasks; new task 0 **merges** a suffix
of old 0 with a prefix of old 1. Each inherited slice keeps its source
`(attempt, epoch)` because the two old writers had independent epoch clocks.
The restore instruction is that list of slices — not overlapping ranges for
the same groups, and not something that was persisted at checkpoint time.

Power-of-two rescale (`2 → 4`, `4 → 2`) is the aligned special case: a new
range is exactly one old range or a concatenation of whole old ranges, never a
partial split of an old range.

`RestorePlanner` still needs this intersection; that planner change is separate
from this store contract. Head fences are re-claimed by the new `WriterId` on
first write.
