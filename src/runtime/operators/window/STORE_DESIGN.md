# Window store with Scylla design

`WindowOperatorStore` serves the sole WO owner of a key.
`WindowRequestStore` serves coherent WRO point lookups. Physical layout, MVCC,
serialization, and caching stay inside the backend.

For detailed window-operator semantics, evaluation flow, and module structure,
see the [window operator README](README.md). This document focuses on store
contracts and the proposed Scylla backend.

The current data contract is append-only. CDC and late-event correction will
require explicit mutation semantics rather than implicit delete markers - this is future work

## Contract types

These are the logical models; backend serialization may differ.

```rust
pub struct StateNamespace {
    pub bytes: Vec<u8>,
}
pub struct PartitionKey {
    pub namespace: Vec<u8>,
    pub business_key: Vec<u8>,
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

- `PartitionKey` must remain collision-safe; hashes alone are insufficient.
  In practice, persist the serialized business-key bytes directly, as in the
  schemas below. Scylla hashes the complete partition key for distribution.
- `Cursor` is raw event identity and total order within a partition.
- `RawRun` and `TileRun` are half-open. Calls contain merged logical runs, and
  backends must return exactly their union.
- `next_seq` allocates per-key sequence IDs. Optional `evaluation` keeps the
  last fired trigger together with retractable accumulator state. State-only WO
  does not keep evaluation state.
- `watermark_frontier` is the task-level late-data boundary. Logical retention
  is derived from it, the largest window, and configured lateness.
- `WindowTrigger` is durable event-time work. Current RANGE windows create one
  `RowEmit` trigger per accepted row; `WindowEnd` is reserved for scheduled
  windows.
- Tiles for all windows share `(granularity, tile_start)`, so persisted tile and
  accumulator state retain `WindowId`.
- `WindowData` is one materialized WRO snapshot. Evaluation filters its rows by
  cursor; batches need not be mapped back to individual runs.

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
    fn stream_due<'a>(
        &'a self,
        namespace: &'a StateNamespace,
        after: Option<Cursor>,
        through: Cursor,
    ) -> BoxStream<'a, Result<Vec<DueWindowWork>>>;
    async fn store_key_state(
        &self,
        partition: &PartitionKey,
        state: &KeyState,
    ) -> Result<()>;
    /// Complete pending writes before capturing the returned snapshot.
    async fn checkpoint(
        &self,
        namespace: &StateNamespace,
    ) -> Result<WindowBackendSnapshot>;
    async fn restore(
        &self,
        namespace: &StateNamespace,
        snapshot: &WindowBackendSnapshot,
    ) -> Result<()>;
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
  ordered by `Cursor`. Their Arrow schema includes `__seq_no: UInt64`.
- Tiles are unique by `(granularity, tile_start)` and restricted to requested
  runs. Missing tiles mean empty intervals.
- `commit_events` atomically publishes raw rows, replacement tiles, key state,
  and triggers. `ts_column_index` plus `__seq_no` identifies each raw cursor.
  Retries are idempotent.
- `stream_due` owns backend pagination for one stable watermark range and
  returns bounded trigger groups with their corresponding key state.
- `store_key_state` publishes only sequence/evaluation state.
- `checkpoint` completes pending writes and returns a backend-specific
  snapshot. Scylla returns only the aligned backend version.
- `restore` starts store access from the supplied checkpoint base. For Scylla,
  the current fenced attempt inherits reads from that immutable version.
- The namespace argument scopes checkpoint and restore when one store instance
  is shared by multiple window operators.
- WO reads observe their latest publication. One fenced WO owns a partition, so
  its historical raw and tile reads need not share a snapshot and can run
  concurrently.
- WRO: Each `load_window_data` is one coherent published snapshot across all
  physical buckets and pages.
- Methods must not broaden ranges or return partial results.

`InMemWindowStore` is the reference backend. One lock provides the WRO snapshot
boundary, and its checkpoint embeds serialized partition state and triggers.
Asynchronous physical retention is not emulated.

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
   `(watermark_frontier, incoming_watermark]`.
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

Tile-based slide retraction is limited to aggregates whose states can be
subtracted safely (`SUM`, `COUNT`, and `AVG`). Other sliding aggregates retract
raw leave-band rows. Rebuilds may use mergeable tile states for any aggregate.

WRO lookup:

1. Both plain and retractable aggregates use rebuild plans, since WRO has no
   prior accumulator.
2. Build and merge exact raw-edge and tile plans for all points and windows.
3. Call `load_window_data` once for one coherent snapshot.
4. Evaluate per-window request arguments and rebuild every answer.

WRO is read-only

## Scylla backend

Scylla does not provide one snapshot spanning multiple partitions, CQL queries,
or result pages. A logical window read may cross several physical ranges, so
concurrent writes could otherwise make those pieces represent different
moments. We us application-level MVCC to pin every piece to one published version and
provide a coherent logical snapshot. We also use time buckets to keep physical partitions
and individual range reads bounded instead of storing a key's entire history
in one ever-growing partition.

The initial backend assumes append-only input and stable task parallelism.
(rescaling is future work)

### Versions and checkpoints

```rust
pub struct StateVersion {
    pub attempt: AttemptToken,
    pub epoch: u64,
}

pub enum WindowBackendSnapshot {
    InMemory { snapshot: Vec<u8> }, // in mem uses whole snapshot
    Versioned { version: StateVersion }, // this is for scylla
}
```

`AttemptToken` must uniquely identify a WO task execution across master
restarts, for example pipeline incarnation + execution_attempt_id + task identity.
It is both the writer fence and the recovery branch ID.

Epoch is one task-wide counter within an attempt. Every publication, regardless
of key, gets the next epoch. A new attempt may restart at zero because
`(attempt, epoch)` remains unique. Scylla stores the two fields separately.
Scylla uses `WindowBackendSnapshot::Versioned`; the in-memory backend uses
`InMemory` with a serialized namespace snapshot. Namespace and the last fully
processed watermark remain in the operator's `WindowStateSnapshot` envelope.

### Tables

```sql
CREATE TABLE window_head (
    namespace blob,
    business_key blob,
    owner_attempt blob,
    writer_attempt blob,
    writer_epoch bigint,
    serving_attempt blob,
    serving_epoch bigint,
    PRIMARY KEY ((namespace, business_key))
);

CREATE TABLE window_recovery_bases (
    namespace blob,
    recovery_attempt blob,
    base_attempt blob,
    base_epoch bigint,
    PRIMARY KEY ((namespace, recovery_attempt))
);

CREATE TABLE window_raw (
    namespace blob,
    business_key blob,
    attempt blob,
    bucket_start bigint,
    event_ts bigint,
    seq_no bigint,
    epoch bigint,
    payload blob,
    PRIMARY KEY (
        (namespace, business_key, attempt, bucket_start),
        event_ts, seq_no, epoch
    )
) WITH CLUSTERING ORDER BY (
    event_ts ASC, seq_no ASC, epoch DESC
);

CREATE TABLE window_tiles (
    namespace blob,
    business_key blob,
    attempt blob,
    granularity_ms bigint,
    bucket_start bigint,
    tile_start bigint,
    epoch bigint,
    payload blob,
    PRIMARY KEY (
        (
            namespace,
            business_key,
            attempt,
            granularity_ms,
            bucket_start
        ),
        tile_start, epoch
    )
) WITH CLUSTERING ORDER BY (tile_start ASC, epoch DESC);

CREATE TABLE window_key_states (
    namespace blob,
    business_key blob,
    attempt blob,
    epoch bigint,
    key_state blob,
    PRIMARY KEY (
        (namespace, business_key, attempt),
        epoch
    )
) WITH CLUSTERING ORDER BY (epoch DESC);

CREATE TABLE window_triggers (
    namespace blob,
    attempt blob,
    bucket_start bigint,
    trigger_shard int,
    fire_ts bigint,
    fire_seq bigint,
    business_key blob,
    trigger_kind tinyint,
    window_id bigint,
    epoch bigint,
    PRIMARY KEY (
        (namespace, attempt, bucket_start, trigger_shard),
        fire_ts,
        fire_seq,
        business_key,
        trigger_kind,
        window_id,
        epoch
    )
) WITH CLUSTERING ORDER BY (
    fire_ts ASC,
    fire_seq ASC,
    business_key ASC,
    trigger_kind ASC,
    window_id ASC,
    epoch DESC
);
```

- `window_head`: per-key ownership fence plus writer and WRO-visible version
  pointers; the LWT publication boundary.
- `window_recovery_bases`: one immutable row per recovery attempt. It maps the
  new `recovery_attempt` to the restored checkpoint version
  `(base_attempt, base_epoch)`. This lets the new attempt inherit unchanged
  state without copying every raw row, tile, and `KeyState`.
- `window_raw`: cursor-ordered event rows, physically split into time buckets
  and versioned by attempt and epoch.
- `window_tiles`: versioned aggregate tiles, partitioned by time bucket and
  granularity.
- `window_key_states`: versioned `KeyState` history used by current WO reads and
  checkpoint recovery.
- `window_triggers`: immutable due work ordered by event time. Time buckets and
  `trigger_shard` bound each Scylla partition:
  `stable_hash(business_key) % trigger_shard_count`. Without it, every trigger
  in one namespace, attempt, and time bucket would share one hot, potentially
  oversized partition. `stream_due` queries all shards for each relevant time
  bucket and merges their results by trigger cursor. The shard is only a
  physical storage concern, not a window, task, or rescaling key group; its
  count must remain stable for the namespace. Row triggers use a sentinel
  `window_id`; scheduled windows retain their actual `WindowId`.

Raw payloads are Arrow-IPC one-row `RecordBatch` values including `__seq_no`..

### Head semantics

- `owner_attempt` is the WO allowed to publish this key.
- `writer_(attempt, epoch)` is the private WO snapshot.
- `serving_(attempt, epoch)` is the complete snapshot visible to WRO.
- Writer and serving versions are equal normally and may differ during
  recovery.

When a new WO instance (initial or recovery) first writes a key, it
LWT/CAS-claims `owner_attempt`. Every later head update checks both owner and
expected writer version.

Claiming means conditionally setting `owner_attempt`: insert it for a new head,
or replace the expected previous owner. Only one competing WO can succeed.
Fencing happens when a zombie tries to update a head now owned by the new WO:
the conditional update fails, so the zombie's data is not published.

### WO write

For one key:

1. Load writer `KeyState`.
2. Allocate task epoch `E`.
3. Write changed raw rows, tiles, `KeyState`, and triggers under `(attempt, E)`.
4. LWT-update the head, requiring the current owner and previous writer version.
5. During normal operation, set both writer and serving versions to
   `(attempt, E)`. During recovery, advance only writer until catch-up.
6. Update or invalidate affected WO cache entries.

The head update is the visibility boundary. Data written before a failed head
update is orphaned. A new epoch starts only after the previous publication
outcome is known. On success, advance the epoch; on failure, safely retry the
same publication or abort the attempt. An ownership CAS failure stops the WO as
fenced. Trigger rows from an unpublished attempt/epoch are orphaned with its
other data and are not returned by `stream_due`.

`store_key_state` follows the same protocol but writes no raw, tile, or trigger
versions.

### WO reads

WO reads the writer snapshot. A fresh attempt reads only its own versions. A
recovery attempt overlays its versions over the immutable checkpoint base from
`window_recovery_bases`: read the newest value from the recovery attempt first,
then fall back to the base version for data not replaced by recovery writes.

Logical runs are mapped to time buckets, loaded, merged, and filtered back to
the exact requested ranges. Raw rows are deduplicated by `Cursor`; current
tiles replace matching base tiles.

`stream_due` maps its stable watermark range to trigger buckets and shards,
overlays the recovery attempt over its checkpoint base, and filters trigger
versions through each key's writer head. It batch-loads writer `KeyState`,
groups triggers into `DueWindowWork`, and owns page sizing and continuation
state for the lifetime of the stream.

### WRO reads

WRO:

1. Reads and retains the key's serving version for the request.
2. Builds exact raw/tile plans.
3. Loads serving-attempt buckets. If that attempt has a recovery-base row,
   loads unchanged data from its base checkpoint as well. This may repeat when
   checkpoints span several recovery attempts.
4. Selects visible versions, then merges, orders, deduplicates, and rebuilds.

WRO does not read writer state, use Foyer, contact the master, or consume
checkpoint metadata. If WO is unavailable, WRO continues serving the unchanged
complete serving snapshot.

### Checkpoint

At an aligned barrier:

1. Resolve all in-flight publication outcomes and complete pending writes.
2. Capture `(current attempt, current task epoch)`.
3. Return `WindowBackendSnapshot::Versioned`; the operator persists it in its
   checkpoint envelope.
4. Continue processing at later epochs.

The checkpoint contains no keys or state payloads. For any key, checkpoint
state is its newest version from the checkpoint attempt with epoch at or below
the cutoff. Creating a checkpoint does not insert a recovery-base row; that row
is created only when a new attempt restores this checkpoint.

The operator stores namespace, its last fully processed watermark, and
`WindowBackendSnapshot` in `WindowStateSnapshot`. Durable triggers already
represent work above that watermark, so checkpointing neither drains nor
serializes an operator-local pending-key set. For unchanged task assignment,
restore passes the same versioned snapshot back to the store.

### Recovery

1. The replacement worker has the restored version in
   `WindowBackendSnapshot::Versioned`.
2. During operator restore, the worker's Scylla store creates the new attempt,
   inserts `window_recovery_bases(new attempt -> restored checkpoint version)`,
   and starts its epoch at zero. The row must exist before the attempt
   publishes data. The master only plans and sends the restore snapshot.
3. Source restores its checkpoint offset and replays post-checkpoint input.
4. On first access to a key, retain its old serving version, claim ownership,
   and restore writer `KeyState` from the checkpoint base.
5. Resume watermark work by streaming checkpoint-visible triggers above the
   restored watermark while replay advances only writer state.
6. Once append-only replay catches the old serving state, atomically switch
   serving to writer.
7. Continue normal publication with writer and serving together.

During recovery, writer and serving versions differ. Replay advances writer
while WRO keeps using the old serving snapshot. While they differ, compare the
writer `KeyState` with the retained serving state. Once `next_seq` and
`evaluation.through` reach the serving state, atomically promote writer to
serving. Keys without evaluation state compare only `next_seq`. Keys not
touched after recovery may continue serving their old snapshot.

### WO cache

Foyer is WO-only:

```text
meta:
    PartitionKey -> (writer version, KeyState)
data:
    (PartitionKey, family, bucket) -> materialized writer-view data
triggers:
    (namespace, attempt, bucket, shard) -> immutable due entries
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

### State Prune/Cleanup

Retain versions reachable from writer heads, serving heads, recovery bases, and
retained completed checkpoints. We should have a separet maintenance task (per worker?) that independently performs:

- trigger cleanup at or below watermarks no longer reachable from retained
  checkpoints;
- logical raw/tile retention below the floor derived from watermark, maximum
  window length, and lateness;
- MVCC cleanup of unreachable versions;
- orphan cleanup for failed or zombie writes;

TTL/TWCS may perform physical expiry when consistent with logical retention.

## Future next steps

### Namespaced cache quota

Assign each state consumer its own memory and local-disk quota. This prevents
one operator or namespace from consuming the shared cache and gives concurrent
consumers a predictable fair share of local resources.

### Mem pressure / backpressure

Track both local cache pressure (mem+disk) and logical remote-state usage. If eviction or
spill cannot keep a consumer within its cache quota, or its retained remote
state reaches a configured logical limit, backpressure its upstream tasks.
Remote state must not act as a bottomless overflow sink; these limits make
state growth visible to normal flow control.

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

When we introduce key groups, rescaling is simiply re-mapping key-group <->task
in-between checkpoints. The versioned backend snapshot becomes a
key-group-to-base-version mapping and `window_recovery_bases` gain key-group
granularity. The core head, raw, tile, `KeyState`, WO, and WRO contracts remain
unchanged.