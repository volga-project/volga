# Window store with Scylla design

`WindowOperatorStore` serves the sole WO owner of a key.
`WindowRequestStore` serves coherent WRO point lookups. Physical layout, MVCC,
serialization, and caching stay inside the backend.

Versioned data tables are shared. Streaming WO uses overlay + checkpoint
only. Request mode adds a per-`key_group` lease (`serving_*` / `prev_E`).
There is no per-key `window_head` and no `window_recovery_bases` table.
See **Protocol (normative)** under Scylla backend.

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
/// Opaque pager token. Only the backend that produced it should pass it back.
/// The helper must not read its fields.
pub struct TriggerResume { /* private */ }
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
- `TriggerResume` is an opaque `load_triggers` cursor. The operator helper
  must not read it. InMem stores the last visible `WindowTrigger`. Scylla
  stores a private seek (last **raw** clustering + `(bucket, shard)`), not
  a packed `WindowTrigger`.
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
land in the bound range (routing bug otherwise). The operator loops `load_triggers` in `emit_due_pages`. Tests drain with
`collect_due`. Neither lives on the store trait.

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
- `load_triggers` is **one hop**. It returns the visible triggers from that
  hop plus an opaque resume. Short pages and empty `triggers` with
  `Some(resume)` are legal (hidden-only hop). End of range is `next is None`
  — do not treat an empty page as EOF, and do not fill `limit` visible
  rows inside the store. It only returns work for the client's bound
  key-group range. The store does not group by key or load `key_state`.
  The operator loops this call in `emit_due_pages`, groups by key, and
  load+evals on the same `process_key_concurrency` pool (16). Fetch size
  is `window.process_page_size` (256) until Scylla is measured. Do not
  floor at key concurrency. Prefetch is later
  ([#297](https://github.com/volga-project/volga/issues/297)).
- `store_key_state` publishes only sequence/evaluation state.
- `checkpoint` completes pending writes and returns a backend-specific
  snapshot of the client's bound range only. Scylla returns this writer's
  `StateVersion` (after restore, writer `E = max(cp_E, serving_E, prev_E)`;
  never restart at 0); InMem serializes partitions in that range.
- `restore` starts store access from the supplied checkpoint base (in-memory
  overlay slices, no bases table) and must not clobber keys outside the bound
  range (the physical InMem/Scylla backend is shared by all tasks of the kind).
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
moments. We use application-level MVCC (`attempt`, `epoch`) so each **reader**
can pin a cut: WO overlays checkpoint + this writer; WRO pins a request-mode
lease. We also use time buckets so physical partitions and range reads stay
bounded.

v1 restore still assumes **same task assignment** (`RestorePlanner` identity
map). The physical model is key-group-native from the start so later rescaling
is a remap of ranges between checkpoints, not a data rewrite. `task_index`
does not appear in PRIMARY KEYs.

**Streaming and request share the versioned data tables.** Lease, steal, and
publish exist **only** when WRO / request-mode is on. Streaming WO never
touches a lease.

### Protocol (normative)

Data (always):

- raw / tiles / `key_state` / **triggers** cluster `(attempt, E)`.
- `E` is monotonic per `key_group` (the owning WO allocates it locally).
  **Never reset** on failover. Not per business key. Not minted by LWT.
  A writer may use one shared counter for all owned groups so the checkpoint
  blob stays a single `StateVersion`.
- Ingest: same-partition **UNLOGGED BATCH** only. **No LWT.**

Streaming WO overlay (no lease):

```text
attempt == me
OR (attempt == cp.attempt AND E <= cp_E)
```

Restore the **writer from checkpoint** (and source offsets), not from a serve
pin. `load_triggers` uses the same overlay. Until restore, overlay is `me`
only.

Attempts do **not** share a counter until restore. Uncheckpointed rows from
a dead attempt are not committed just because a later writer reused those
`E` values. Do **not** overlay `me | E <= cp_E` (any attempt): after B
checkpoints, C would `max(E)`-prefer A's leftover `E=50` over B's replay at
`E=12`. Streaming durability is the last checkpoint plus source replay, not
a WRO serving line.

Idle keys last written on an attempt older than `cp.attempt` disappear from
WO after a second failover. That is the documented cost of one
`StateVersion`. WRO still sees them on the epoch line. If idle keys on WO
matter later, add a real cut (per-group committed `E`, or a chain) — do not
widen overlay.

Request serving (only if WRO is on) — one lease per `key_group`:

```text
owner
serving_E, serving_attempt, serving_wm
prev_E    -- serving_E at last steal; 0 in CQL if serving was empty. Publish does not touch it.
```

`prev_E` is the last **published** cut at steal, not “the previous owner.”
Above that cut, only `serving.attempt` may appear. CQL stores `prev_E = 0`
(no unset). One filter when a serving pin exists:

```text
E <= serving_E
AND (E <= prev_E OR attempt == serving.attempt)
```

Empty serving (nobody has published) ⇒ WRO returns **no pin**, no data
reads. `prev_E = 0` is not a pin.

- **Steal** (lazy, first write to that group): `prev_E ← serving_E` (or `0`
  if serving is empty), then `SET owner IF owner = previous`. Do not move
  `serving_*`. Then set writer `E = max(cp_E, serving_E, prev_E)` (missing
  terms are 0).
- **Publish** (timer / OnCommit / checkpoint — **one** function): if
  `current_wm >= serving_wm`, `SET serving_* IF owner = me`. Do not write
  `prev_E`.
- **WRO:** pin the lease **once** (skip if serving empty), then every cell
  uses the filter above, then **latest per cell**: same `Cursor` /
  `(granularity, tile_start)` / `key_state` → `max(E)`, then `max(attempt)`.
  Mixing **different** cells from `A@80` and `B@160` is expected. Two
  payloads for the **same** cell is not.

Outage: keep last `serving_E` (no dip to checkpoint). Grey window until steal:
the old owner may still publish — accepted. Each steal **overwrites** `prev_E`
with the then-current serving cut.

`serving_wm` on the first write after restore is a **v1 limit**, not a hole:
the gate is event-time only, not per-key catch-up. If `serving_wm` is already
`<=` restored wm, `publish()` may run immediately. The snapshot is holes
(`E <= prev_E`) plus whatever `serving.attempt` has written.

Filter is **client-side** (clustering is time-first). CQL slices time;
`attempt` / `E` is in-process.

**Not doing:** per-key `window_head`, DashMaps / unbounded keyed claim maps,
ingest CAS, `window_recovery_bases` chain, serve = checkpoint (unless later
chosen), proving the old worker is dead, per-key catch-up freeze,
any-attempt WO overlay.

**v1 limits:** idle keys on WO after a second failover (one `StateVersion`).
Event-time GC vs a long WRO whose window is below `data_floor` (request-budget
grace, not a pin table). `serving_wm` not delaying first publish when it is
already caught.

### Versions and checkpoints

```rust
/// Job-level recovery branch. Not a task id.
/// Example: pipeline incarnation + execution_attempt_id.
pub struct AttemptToken(Vec<u8>);

/// Unique WO task execution. Request-lease `owner` only, not part of data PKs.
/// Example: AttemptToken + vertex/task identity.
pub struct WriterId(Vec<u8>);

pub struct StateVersion {
    pub attempt: AttemptToken,
    pub epoch: u64,
}

/// Restore/remap only. Not stored in the checkpoint blob.
/// Planner output; the Scylla client keeps slices in memory for WO overlay.
/// Not a table.
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

`E` is a per-`key_group` (or shared-across-owned-groups) monotonic id. Each
successful `commit_events` / `store_key_state` does `E += 1` for that group
and stamps that `E` on the write. **Do not restart at zero** on a new
attempt: `(attempt, E)` is unique anyway, but WRO and overlay need a single
never-reset number line so holes are just smaller `E`.

| Place | Role |
| --- | --- |
| Writer allocator (local) | produces `E` for the group; no LWT |
| Data rows | MVCC: this cell belongs to publish `E` |
| Checkpoint cutoff | WO overlay: `cp.attempt` with `E <= cp_E`, plus `attempt == me` |
| Lease `serving_*` | WRO/GC pin — request reads only. Not updated on ingest |
| Lease `prev_E` | steal cut: above it, only `serving.attempt` is visible |

Checkpoint `E=7` means “for this writer's owned groups, newest row with
`epoch <= 7` on **`cp.attempt`**, plus this attempt's own rows.” Numeric epochs
may match across tasks; they write different key groups. Equal `E` on two
attempts is **not** the same commit until restore continues the counter.

`AttemptToken` in table keys and in `StateVersion` is job-level. All WO tasks
of one execution share it.

`WriterId` is stored only on the request lease (`owner`). Streaming does not
use it.

`VersionedRange` is not a checkpoint record and is **not** written to Scylla.
Checkpoint persists one `StateVersion` for this writer. After a future
rescale, `RestorePlanner` intersects old task ranges with the new assignment
and produces a `Vec<VersionedRange>` as the **in-memory restore instruction**.
v1 same-assignment is the trivial case: one slice = bound range + the
checkpoint's `StateVersion`. WO overlay uses those slices; there is no
`window_recovery_bases` table.

Scylla uses `WindowBackendSnapshot::Versioned`. InMem uses `InMemory` with a
serialized snapshot of the owned range. Namespace and the last fully processed
watermark remain in the operator's `WindowStateSnapshot` envelope.

### Tables

`attempt` is **not** in any data partition key. It is clustering (or a
column) so overlay is a visibility filter on one `LOCAL_QUORUM` read, not a
second partition hop. Restore does not copy rows.

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
-- Request mode only. Streaming WO does not read or write this table.
CREATE TABLE window_kg_lease (
    namespace blob,
    key_group int,
    owner_writer blob,
    serving_attempt blob,
    serving_epoch bigint,
    serving_wm bigint,
    prev_epoch bigint,
    PRIMARY KEY ((namespace, key_group))
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

- `window_kg_lease`: one row per **key_group**, not per business key, not
  versioned. Request-mode only. `owner_writer` is the publish fence.
  `serving_*` is the published snapshot WRO always pins. `prev_E` is the
  serving cut at last steal (`0` if serving was empty). Point get by
  `(namespace, key_group)`. Do not put `attempt` in this PK. Do **not**
  restore a per-key `window_head`.
- **There is no `window_recovery_bases` table.** WO overlay uses the
  in-memory checkpoint / `VersionedRange` slices. WRO does not read
  checkpoints.
- `window_kg_buckets`: skinny GC index, not a source of truth for reads.
  One table. `commit_events` does an unversioned upsert of `business_key`
  (no `attempt`/`epoch`, no payload). `maintain` per owned `key_group`:

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
  and time bucket. Overlay / WRO filters `attempt` / `epoch` **in the client**.
- `window_key_states`: versioned `KeyState` in one partition per key.
- `window_triggers`: immutable due work, **versioned like raw**. Locality is
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
  WRO does not read triggers.

### Lease semantics (request only)

`window_kg_lease` is a singleton per `key_group`, not an MVCC table. Data rows
are versioned by `(attempt, epoch)` in their primary keys. Overlay fences
zombie **data** for WO. The lease is the **serving name** for WRO.

A last-write-wins upsert of `serving_*` would let dying attempt N overwrite
the pin after N+1 is live. Owner LWT is the fence for **publish**, not for
ingest.

| Column | Role |
| --- | --- |
| `owner_writer` | Who may change serving. Fence. |
| `serving_(attempt, epoch, wm)` | Snapshot WRO always pins. |
| `prev_E` | Serving `E` at last steal (`0` if serving empty). Publish does not touch it. |

`owner_writer` is length-prefixed `attempt || vertex` (`WriterId`). Claiming
is **only** `SET owner` plus `prev_E ← serving_E` (or `0`):

- empty group / never published: `INSERT … IF NOT EXISTS`; steal sets
  `prev_E = 0` so a predecessor's unpublished rows are not treated as holes.
- restore / failover: `UPDATE … SET owner = me, prev_E = serving_E IF owner =
  previous` (`prev_E = 0` if serving is unset). **Do not** change `serving_*`.
  **Do not** copy `serving.attempt` into a “previous owner” slot — `prev_E`
  is the published cut, not the displaced writer.

CAS not applied fences; do not fall through to a second LWT. Restore is a
key-group range and the lease is already per group, so steal is **lazy on
first write to that group**, not a scan of all owned groups at restore.
WRO does not need steal: it pins current `serving_*` even while `owner` is
still the previous writer (grey window).

WRO always pins the lease (one hop + payload). It does not discover epoch
from `key_state` and does not read writer overlay or checkpoint metadata.

**Publish** is one function (`promote_serving` LWT: `SET serving_* IF owner =
me`). Cadence is who **calls** it (`on_commit` / `interval` / `checkpoint`)
— freshness vs ingest-p99 — not a second protocol. Gate:

```text
if current_wm < serving_wm: return
SET serving_attempt, serving_epoch, serving_wm IF owner = me
```

`serving_wm` is the task `watermark_frontier` at that promote. The gate
delays publish only when serving is **ahead of** the restored watermark
(last publish after the last checkpoint). If `serving_wm <= restored wm`,
it can open on the first write — that is not a bug. `serving_E` prevents
an epoch dip; it does **not** wait for the new writer to finish replay.
v1 then serves holes (`E <= prev_E`) plus whatever `serving.attempt` has
written. Do **not** compare per-key `next_seq` / `evaluation.through` on
the ingest path. Do **not** freeze ingest.

A background timer is not required: interval can be checked on the next
ingest of that group, and checkpoint can flush owned groups.

Grey window until steal is **accepted** (CAP): the previous owner may still
publish. After steal, CAS blocks publish. Rows with `E > prev_E` are visible
only for `serving.attempt`, so a displaced writer's unpublished rows do not
leak when a later owner publishes — even if that displaced writer never
became `serving.attempt`. Do not try to prove the old process is dead.

### WO write

For one key (streaming and request ingest are the same data path):

1. Derive `key_group` from the key hash and the client's `max_p`.
2. Request-mode only, if this is the first write to the group after restore:
   steal owner (`prev_E ← serving_E`, or `0`). Set writer
   `E = max(E, cp_E, serving_E, prev_E)`. Do not move serving. Do not LWT
   ingest.
3. Load writer `KeyState` (WO overlay). No lease read on streaming.
4. Allocate group epoch `E` locally (`E += 1`, never reset).
5. Write changed raw rows, tiles, `KeyState`, and triggers under
   `(job attempt, E)`. Upsert `window_kg_buckets` unversioned (not under
   `E`).
6. Request-mode only: maybe `publish()` according to cadence and the
   `serving_wm` gate. Not on the data batch. Not during watermark catch-up.

Each `(key, bucket)` (and each tile partition) is one Scylla partition.
`commit_events` issues one **UNLOGGED BATCH** per such partition — one
mutation per event/tile row, one RTT per partition, not one RTT per event.
Do not use logged BATCH. Do not batch across partitions (no cross-partition
atomicity, coordinator penalty). Tiles, triggers, and other buckets are
separate requests.

A new epoch starts only after the previous write outcome is known. Request
ownership CAS failure (steal or publish) stops the WO as fenced. Trigger
rows from an unpublished overlay-hidden attempt/epoch are not returned by
`load_triggers`.

`store_key_state` follows the same protocol but writes no raw, tile, trigger,
or bucket-index rows.

### WO reads

WO reads the writer snapshot, not the lease.

```text
visible if
  attempt == me
  OR (attempt == cp.attempt AND E <= cp_E)
```

Until restore there is no `cp` — overlay is `me` only. After rescale, `cp`
is the `VersionedRange` slice that covers this `key_group` (in memory).

A single `StateVersion` is one generation, not a chain. Idle keys last
written on an attempt older than `cp.attempt` are **not** visible to WO.
That is v1. Do not widen to `E <= cp_E` for any attempt: uncheckpointed
rows from a dead attempt can share `E` values with the successor's replay,
and `max(E)` then prefers the dead tail.

Because `attempt` is clustering, overlay is one `LOCAL_QUORUM` read per data
partition: keep the newest row that matches, client-side. Restore does not
copy rows.

Logical runs are mapped to time buckets, loaded, merged, and filtered back to
the exact requested ranges. Raw rows are deduplicated by `Cursor`; current
tiles replace matching base tiles. Same-cell collisions among visible rows:
`max(E)`, then `max(attempt)`.

`load_triggers` is one CQL hop and the **same overlay**. Mid-shard seek is
the last **raw** clustering row (full tuple, not `fire_at` alone), even if
overlay-hidden. First hop and a new `(bucket, shard)` seek from `after`
(watermark cursor). `next_partition` walks shards then time buckets; do not
rebuild a `(after, through]` grid or store `part_idx`.

```text
WHERE namespace = ? AND bucket_start = ? AND kg_shard = ?
  AND (fire_ts, fire_seq, business_key, trigger_kind, window_id, attempt, epoch)
      > (?, ?, ?, ?, ?, ?, ?)
  AND fire_ts <= ?
LIMIT ?
```

The client drops `key_group` outside the bound range and applies the WO
overlay. Resume is a private `Seek`; the helper does not read it.

Do not use `OFFSET`, native `PagingState`, a second `fire_ts >=` query,
seek on `fire_ts` alone, or `execute_unpaged` of the whole range.

`emit_due_pages` loops `load_triggers` until `next is None`. Watermark
still advances only after the full `(after, through]`. Prefetch is later
([#297](https://github.com/volga-project/volga/issues/297)).

### WRO reads

WRO (request store only):

1. Point-get `window_kg_lease` for the key's `key_group`. If serving is
   empty, return empty (**no pin**). Otherwise **pin once**:
   `(serving_*, prev_E)`. Do not reread mid-request.
2. Build exact raw/tile plans.
3. Load from the same data partitions (time slice in CQL). Filter
   **client-side**:

   ```text
   E <= serving_E
   AND (E <= prev_E OR attempt == serving.attempt)
   ```

   `prev_E` is 0 in CQL when never stolen or when serving was empty at steal
   (the empty-serving case never reaches this step).
4. Collapse **per cell**, not per business key: same `Cursor` /
   `(granularity, tile_start)` / `key_state` → `max(E)`, then `max(attempt)`.
   Different cells may come from different `(attempt, E)` under the same pin.
5. Merge, order, deduplicate, rebuild.

WRO does not read writer overlay, triggers, Foyer, the master, or checkpoint
metadata. If WO is unavailable, WRO continues serving the unchanged pin.
Any WRO task can read any key in the operator namespace; keyed routing is
locality, not identity.

Do not implement WRO as `attempt == serving.attempt && E <= serving_E` only
(holes on older attempts disappear). Do not implement epoch-only
`E <= serving_E` (unpublished rows from a writer who stole but never
published leak once a later owner publishes). Do not implement
`NOT (attempt == prev.attempt AND E > prev.E)` — `prev_E` is the last
**published** cut, not the last writer; a B-then-C steal before B
publishes would leave `prev` as A and leak B.

### Checkpoint

At an aligned barrier:

1. Complete pending writes. Request-mode may also `publish()` owned groups
   (cadence `checkpoint`, and to make a barrier WRO-visible under `interval`).
2. Capture `(current job attempt, current writer epoch)`.
3. Return `WindowBackendSnapshot::Versioned { version }`. The operator
   persists it in its checkpoint envelope. The range is the client's bound
   assignment and is not copied into the blob.
4. Continue processing at later epochs.

The checkpoint contains no keys, no key-group list, no lease rows, and no
state payloads. Creating a checkpoint does not insert recovery-base rows.
For unchanged task assignment, restore passes the same `StateVersion` back
into the client. `RestorePlanner` remapping (producing `Vec<VersionedRange>`
in memory) is future work.

The operator stores namespace, its last fully processed watermark, and
`WindowBackendSnapshot` in `WindowStateSnapshot`. Durable triggers already
represent work above that watermark, so checkpointing neither drains nor
serializes an operator-local pending-key set.

### Recovery

1. v1: the replacement worker has `WindowBackendSnapshot::Versioned { version }`
   and the same assignment, so one in-memory `VersionedRange` = bound range +
   that version. After rescale: `RestorePlanner` sends the intersected
   `Vec<VersionedRange>` (still not a checkpoint format; it is restore input).
2. During operator restore, the Scylla client uses the new **job** attempt.
   Overlay uses the restore slices (`me | cp.attempt`). Do not write a bases
   table. The master only plans and sends the restore payload. Writer
   `E = max(cp_E, serving_E, prev_E)` (request-mode reads the lease; missing
   terms are 0). Never restart at 0 from the checkpoint cutoff alone — that
   can dip below `serving_E`.
3. Source restores its checkpoint offset and replays post-checkpoint input.
4. Resume watermark work by streaming checkpoint-visible triggers above the
   restored watermark. Replay advances only writer state.
5. Request-mode: on first write to a group, steal owner (`prev_E ← serving_E`,
   or `0` if serving empty). Leave `serving_*` at the previous cut.
   Recompute writer `E = max(current E, serving_E, prev_E)`.
   `publish()` stays gated on `serving_wm` (event-time only; v1 may publish
   on the first write if the gate is already open).
6. Streaming: no steal, no publish. Writer `E` continues from `cp_E`.

During recovery, writer data and (if request) serving pins differ. Replay
advances MVCC rows; WRO keeps using `serving_*` (epoch line, including idle
keys on older attempts). WO does **not** see those idle keys unless they are
`cp.attempt`. A zombie publish after steal does not apply. Unpublished rows
from a displaced owner (`E > prev_E` and not `serving.attempt`) stay hidden
after a later publish. Until steal, grey window may include a last old-owner
publish.

### WO cache

Foyer is optional and WO-only. It is **not** a HeadClaim / per-key lease
cache. Do not keep unbounded DashMaps of claim state.

```text
meta:
    PartitionKey -> KeyState
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

Logical scans are assembled from bucket point reads. WRO bypasses cache.

### Scylla consistency

Minimal setup is single DC, RF=3. Because we need read-your-write after
publish, use `LOCAL_QUORUM` for both data reads and writes (`W+R > RF`).
Request lease steal/publish is LWT with `LOCAL_SERIAL` + learn `LOCAL_QUORUM`:

- **WO write:** non-LWT data @ `LOCAL_QUORUM`. Streaming: never LWT.
  Request: steal is one LWT on first write to the group (fence, no serving
  change). Publish is a separate LWT on the configured cadence, gated by
  `serving_wm`.
- **WO read (cache miss):** overlay on versioned tables @ `LOCAL_QUORUM`
  (not the serving pin).
- **WRO read:** lease pin + data @ `LOCAL_QUORUM` for one coherent snapshot.
  Always this path; do not derive the pin from `max(epoch)`.

**Not v1:** `USING TIMESTAMP` instead of owner CAS; ingest CAS (SlateDB-style
write fence); derived-from-`key_state` WRO pins.

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
  row. Do not probe historical bucket ids. Do not rely on TWCS/TTL
  (write-time ≠ event-time; replay rewrites old buckets);
- drop extras above the steal cut that are not the current owner:
  `E > prev_E AND attempt != owner` (request lease; `prev_E = 0` if never
  published). In-flight WRO already filters those rows, so this is not a
  pin-refcount. Do **not** drop unpublished current-writer rows
  (`attempt == owner`, including `E > serving_E`) or `E <= prev_E` holes
  until event-time GC / compact;
- no `window_recovery_bases` walk.

**In-flight WRO:** `load_window_data` pins the lease at start. Dropping
`E > prev_E` extras is safe for that pin (the filter excludes them). The
remaining race is **event-time** GC vs a request whose window still sits
below `data_floor`. v1: never delete the current serving pointer; delay
dropping buckets below `floor_bucket` by at least the maximum WRO request
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
`restore()` keeps those slices in the client for WO overlay. There is no
`window_recovery_bases` table. The checkpoint blob never stores a range.

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
from this store contract. Request-mode leases are re-stolen by the new
`WriterId` on first write to each `key_group` (`prev_E ← serving_E`). Streaming
does not touch the lease.
