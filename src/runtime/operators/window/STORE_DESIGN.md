# Window store with Scylla design

`WindowOperatorStore` serves the sole WO owner of a key.
`WindowRequestStore` serves coherent WRO point lookups. Physical layout, MVCC,
serialization, and caching stay inside the backend.

Versioned data tables are shared. Streaming WO uses overlay + checkpoint
only. Request mode adds `window_pins` (per-`key_group` owner + per-key serving
cut). There is no per-key `window_head`, no `window_recovery_bases`, no
`prev_E`, and no `high_E`.

This document supersedes the protocol in
[#157](https://github.com/volga-project/volga/pull/157). For operator
semantics, evaluation flow, and module structure, see the
[window operator README](README.md).

The current data contract is append-only. CDC and late-event correction will
require explicit mutation semantics — future work.

---

## Modes

| | Streaming (WO only) | Request (WO + WRO) |
|---|---|---|
| Data, triggers, `window_kg_buckets` | yes | yes |
| Per-group `E`, overlay, CP slices | yes | yes |
| `window_pins`, steal, publish, WRO | **no** | yes |

Ingest is always **UNLOGGED**. No ingest LWT. No per-business-key worker maps.

---

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

pub struct KeyState {
    pub next_seq: u64,
    pub evaluation: Option<KeyEvaluationState>,
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
  the owned key-group range, bound on the per-task store client at open.
- `PartitionKey` must remain collision-safe. Persist the serialized
  business-key bytes. Derive `key_group` from `Key.hash` and `max_parallelism`.
- `Cursor` is raw event identity and total order within a partition.
- `RawRun` and `TileRun` are half-open.
- `watermark_frontier` is the **task-level** late-data boundary. It is stored
  in the operator checkpoint and restored from it (already true in
  `WindowOperatorState`). Logical retention is derived from it, the largest
  window, and configured lateness.
- `WindowData` is one materialized WRO snapshot.

## Worker state topology

```text
physical backend     worker + OperatorKind (one InMem map / Scylla session)
WindowOperatorStore  per-task client wrapping that backend
```

The client is created at WO `open` from the task's assignment
(`WindowStoreTaskScope`) and is stable for the attempt:

```text
StateNamespace  = (pipeline, owner operator)
max_parallelism = job-wide, bound on the client
KeyGroupRange   = groups for this task_index at (p, max_p)
WriterId        = this task execution (request `owner` only)
AttemptToken    = job-level execution attempt
```

The client derives `key_group` from `Key.hash` and bound `max_p`. Per-key
calls must land in the bound range.

WRO reuses the WO owner namespace. It addresses rows by `PartitionKey`.
Request routing still sends a lookup to the WRO task that owns that key
(locality); the namespace does not encode `task_index`.

`OperatorStore` is the generic maintenance port on the **shared** physical
backend. Window eligibility is read from `OperatorTaskState` (watermark →
`retention_cutoff`, plus the same key-group range the WO client was bound to).

## Store traits

Landed on master (`WindowOperatorStore` / `WindowRequestStore`). Required
behavior that this protocol relies on:

- Missing partitions and empty runs return empty/default results.
- `commit_events` atomically publishes raw rows, replacement tiles, key state,
  and triggers **for that key**. Retries are idempotent. The backend derives
  `key_group`; it does not trust a caller-supplied group.
- `load_triggers` is one hop, same visibility as WO overlay. WRO does not
  read triggers.
- `checkpoint` completes pending writes and returns a backend snapshot of the
  client's bound range only.
- `restore` starts overlay from the supplied checkpoint / planner slices and
  must not clobber keys outside the bound range.
- WRO: each `load_window_data` is one coherent published snapshot.

`InMemWindowStore` is the reference physical backend. One lock provides the
WRO snapshot boundary. Scylla implements the same client contract with MVCC.

## Runtime flows

WO ingest:

1. Load key state (WO overlay), drop rows at or behind the task watermark,
   assign sequence IDs.
2. Load and update affected tiles.
3. For emitting WO, create one `RowEmit` trigger per accepted row.
4. `commit_events` (unlogged). Request-mode may then `publish()` if
   `publish_ok`.

WO advance: stream trigger pages for `(watermark_frontier, incoming_watermark]`,
evaluate, `store_key_state`, then advance the task watermark. Physical prune
is the worker cleaner via `maintain`.

WRO lookup: pin `window_pins` once, `load_window_data` once, evaluate. Read-only.

---

## Identity and clocks

```text
business_key   window key
key_group      hash % max_parallelism          // stable under rescale
subtask        key_group * p / max_parallelism
attempt        unique writer token (never reuse)
E              per key_group; ++ on each write batch to that group
watermark      task-global, non-decreasing
```

`E` is a **write-batch counter**, not event time. Not comparable across
attempts or across groups.

`AttemptToken` in table keys is job-level: all WO tasks of one execution
share it. `WriterId` (`attempt || vertex`) is stored only on request
`window_pins.owner`. Streaming does not use it.

Watermarks are **non-decreasing**, not “strictly greater at every publish or
barrier.” Publish and checkpoint are not watermark events; many of them can
share the same `wm` value.

---

## Protocol (normative)

### Data plane (always)

- raw / tiles / `key_state` / **triggers** cluster `(attempt, E)`.
- `E` is allocated locally per `key_group`. **No LWT** to mint it.
- Ingest: same-partition **UNLOGGED BATCH** only.

```text
if next_E[g] unset:
    next_E[g] = cp_E_g        // 0 if this group has no restore slice
E = ++next_E[g]
stamp (me, E)
```

Increment **during catch-up**. Do not wait for `publish_ok`. Do not seed from
pins. Do **not** seed `max(cp_E, serving_E)`: `E` does not need to stay
monotonic across attempts. Replay catches **watermark**, not epoch.

### WO overlay (always)

```text
visible ⇔
    attempt == me
    OR (attempt == cp_g.attempt AND E ≤ cp_E_g)
```

`cp_g` is the restore **slice** that contains `g`. Same assignment is one
slice. Until the first restore of a job, overlay is `me` only.

`load_triggers` uses the same filter.

**Do not** overlay `me | E ≤ cp_E` for any attempt: after B checkpoints, C
would `max(E)`-prefer A's leftover `E=50` over B's replay at `E=12`.

Zombie rows are the old `attempt` with `E > cp_E` → invisible. That is the
v1 **streaming fence**. Streaming does not have an `owner` row. Periodic
“am I still owner?” suicide is **not v1**; master kill reaps the process.

Idle keys last written on an attempt older than `cp_g.attempt` disappear
from **WO** after failover (one overlay generation). WRO still sees them
via per-key pins. That is the documented streaming cost.

### Request pins (only if WRO is on)

```sql
CREATE TABLE window_pins (
    namespace        blob,
    key_group        int,
    owner            blob   static,
    serving_wm       bigint static,
    business_key     blob,
    serving_attempt  blob,
    serving_E        bigint,
    PRIMARY KEY ((namespace, key_group), business_key)
);
```

| Column | Meaning |
|---|---|
| `owner` | `WriterId` who may publish |
| `serving_wm` | task wm at last successful publish LWT for this **group** |
| `serving_attempt`, `serving_E` | last published cut for that **key** |

Idle keys keep their clustering pin until that key is written and published.
Steal does **not** move clustering pins. There is no `prev_E` and no `high_E`.

Same-partition conditional BATCH is **one Paxos** in Scylla, so one publish
can update the statics and many clustering keys atomically.

### Steal (request, once per owned group at restore)

```text
SET owner = me IF owner = previous   // empty → INSERT IF NOT EXISTS
```

Do not touch `serving_*` or `serving_wm`. Steal fail or LWT timeout → **fail
the task** (unknown). WRO does not steal; it pins current `serving_*` even
while `owner` is still the previous writer (grey window).

### `publish_ok` (request, OnCommit / periodic)

Do **not** publish on the checkpoint barrier. Pins move only on the writer
cadence. `publish_ok` is **not** applied to a special CP path because there
is none.

```text
publish_ok ⇔
    current_wm is set
    AND current_wm >= serving_wm     -- caught last serve
    AND current_wm > cp_wm           -- not the restored copy of the frontier
    AND dirty ≠ ∅
```

`current_wm` is the live task `watermark_frontier` (seeded from the
checkpoint blob on restore, then only advanced on upstream `wm > frontier`).
`cp_wm` is the restored frontier for that group (the slice’s
`watermark_frontier`; task min is the conservative default).
`serving_wm` unset (never published) → skip `>= serving_wm`.

After restore, `current_wm == cp_wm`, so `> cp_wm` is false until a **new**
upstream watermark. That is the prefix guard.

| After restore | Example | What waits |
|---|---|---|
| Serve **ahead** of CP | `serving_wm=1000`, `cp_wm=800`, `current=800` | `>= serving_wm` until 1000 |
| Serve **= CP** | both `1000`, `current=1000` | `> cp_wm` until 1001 |
| Serve **behind** CP | `serving_wm=800`, `cp_wm=1000`, `current=1000` | `> cp_wm` until 1001 |

`>= serving_wm` alone is already true in the equal and behind cases because
restore **copies** `cp_wm` into `current_wm`. `current_wm > serving_wm` alone
opens immediately when the pin lags the blob. You need both compares.

`dirty ≠ ∅` only skips an empty LWT. It does **not** replace `> cp_wm`: the
first post-restore batch is still a prefix. WRO does not read WO overlay; as
soon as `serving_attempt` becomes `B`, that key’s A rows disappear.

### Publish LWT (request)

One BATCH, same partition:

```text
UPDATE window_pins SET serving_wm = current_wm
    IF owner = me
UPDATE window_pins SET serving_attempt = me, serving_E = E
    WHERE business_key IN dirty
    IF owner = me
```

Only keys in `dirty`. Others keep their clustering pin.

`Not applied` or timeout → **fail this task**. Writers always publish on a
policy (eager OnCommit and/or periodic), so a zombie that is still ingesting
eventually hits this CAS. That is a **backup** to master kill, not the
primary fence. Grey window **before** steal: the old owner may still
publish — accepted.

**Hops:** async ingest = 1 (data). OnCommit = 2 (data, then pin LWT). No
ingest LWT.

### WRO filter

Pin the clustering row **once** per request; every page uses that cut.
Empty serving (no pin row for that key) ⇒ return empty, no data reads.

```text
visible ⇔
    attempt == serving_attempt
    AND E ≤ serving_E
```

No overlay, no `me`, no `prev_E`. During failover WRO stays on last publish
until B’s first successful `publish_ok`.

Same-cell collisions among visible rows: `max(E)`, then `max(attempt)`.
Filter is **client-side** (clustering is time-first). CQL slices time;
`attempt` / `E` is in-process.

WRO does not read writer overlay, triggers, Foyer, the master, or checkpoint
metadata. Any WRO task can read any key in the operator namespace; keyed
routing is locality.

**Not** epoch-only `E ≤ serving_E` (zombie leak across attempts).
**Not** `prev_E` hole-fill: idle keys keep their own `serving_*`, so older
attempts stay visible until that key is republished.

### Local RAM (bounded)

| State | Scope |
|---|---|
| `me` | task |
| overlay slices | restore, once |
| `next_E[g]` | first-touch per owned group |
| dirty keys since last publish | request; cleared on flush |
| `cp_wm` / cached `serving_wm` | request |

No per-business-key epoch map. No `HeadClaim`.

---

## Checkpoint blob

Each WO task, at an aligned barrier, flushes then reports a **small**
control-plane payload. Cells stay in Scylla.

Because `E` is per group, the overlay cut **cannot** be one task-wide epoch
(a slow group’s post-CP writes would fall under `E ≤ max_E`).

```text
WindowStateSnapshot
  watermark_frontier     // cp_wm; already stored today
  backend:
    attempt              // this writer
    range [g0, g1)       // groups this task owns (or implied by task key)
    cp_E[g] for g in range
```

Size is `O(max_p / p)` u64s. No keys, no pin rows, no payloads.

```rust
/// Restore/remap instruction. Not stored as a Scylla table.
pub struct VersionedRange {
    pub range: KeyGroupRange,
    pub attempt: AttemptToken,
    pub cp_E: Vec<u64>,   // one per group in `range`
    pub cp_wm: i64,
}

pub enum WindowBackendSnapshot {
    InMemory { snapshot: Vec<u8> },
    Versioned {
        attempt: AttemptToken,
        cp_E: Vec<u64>,   // parallel to the client's bound range
    },
}
```

`WindowBackendSnapshot::Versioned` does not store the range: the payload is
already keyed by task on the master, and the client has a bound assignment.
On rescale the **planner** produces `Vec<VersionedRange>` as restore input
(not a second checkpoint format).

Do **not** publish pins at the barrier. Sources checkpoint offsets
separately. The assigner (`max_event_time_seen`) is **not** in the source CP
today; post-restore wms are generated from **new** records. Restoring the WO
frontier keeps the late-data line until those wms exceed it.

---

## Restore and rescale

`key_group` does not move. Parallelism only changes the owner:

```text
max_p = 8
p=2:  task0 [0,4)   task1 [4,8)
p=4:  task0 [0,2)   task1 [2,4)  task2 [4,6)  task3 [6,8)
```

**Do not recompute watermarks from data.** Three clocks, three sources:

| Clock | Source |
|---|---|
| Slice `cp_wm` / `cp_E[]` | Source task blob |
| Task `watermark_frontier` | `min(slice cp_wm)` |
| `serving_wm` / `serving_*` | Unchanged on `window_pins` |

**Why min:** advancing to `max` would skip fires for keys from a slower
parent. Min can accept a bit of late data on keys from the faster parent;
per-key last-fired in `KeyState` still blocks duplicate emits. Task wm is
global, so this is the rescale tradeoff.

Live `current_wm` then only moves on upstream `wm > frontier`.

**Planner:** decode each source blob; intersect ranges with each target
task’s owned groups → slices.

```text
scale-out, new task1 owns [2,4):
  [{ range:[2,4), attempt:A, cp_E: A's [2,4), cp_wm: wmA }]

scale-in, new task0 owns [0,4):
  [
    { range:[0,2), attempt:A, cp_E: old0, cp_wm: wm0 },
    { range:[2,4), attempt:C, cp_E: old1, cp_wm: wm1 },
  ]
```

Same assignment = one slice = the old blob.

Example, `max_p = 128`, rescale `p=3 → p=2`:

```text
old p=3:  task 0 [0, 43)   task 1 [43, 86)   task 2 [86, 128)
new p=2:  task 0 [0, 64)   task 1 [64, 128)

new task 0 ← [0, 43) @ old0  +  [43, 64) @ old1
new task 1 ← [64, 86) @ old1  +  [86, 128) @ old2
```

Each inherited slice keeps its source `(attempt, cp_E[])` because the two
old writers had independent epoch clocks.

**Target `restore`:**

1. New `attempt = B`.
2. Overlay / `cp_E_g` / slice `cp_wm` from the slice that contains `g`.
3. Task frontier = `min(slice cp_wm)`.
4. `next_E[g]` unset until first write → `cp_E_g`.
5. Request: steal `owner` per owned group. Pins stay.
6. Replay sources from this barrier. **Do not copy** cells.

---

## Failover picture

```text
            CP@800,E=100     publish@1000,E=5000     crash      B restores
WO overlay  A≤100            + A's later writes      A≤100 ∪ B
WRO         last pin         A@5000                  A@5000 until B publish_ok
B ingest    —                —                       B@101… immediately
B publish   —                —                       after publish_ok
```

First B publish may be `B@150` replacing `A@5000` on keys in that batch. New
`attempt` — `150` vs `5000` is not compared. Other keys keep A.

Streaming: no steal, no publish. Overlay + source replay only.

---

## Tables

`attempt` is **not** in any data partition key. It is clustering so overlay
is a visibility filter on one `LOCAL_QUORUM` read. Restore does not copy
rows.

**GC default: skinny index + per-key data PK. Do not cluster `business_key`
under `(namespace, key_group, bucket)` on the data tables.** `load_raw` is
per-key; a key group is unbounded, so that clustering would make one data
partition hold every key in the group+bucket.

That data layout cannot list expired buckets, so `commit_events` also writes
one payload-free index. **`bucket_start` is clustering on the index, not a
partition-key component.** Bind `floor_bucket` = first bucket that still
overlaps `data_floor`. Only drop buckets whose entire range is below the
floor: `bucket_start < floor_bucket`.

```sql
-- Request mode only. Streaming WO does not read or write this table.
CREATE TABLE window_pins (
    namespace        blob,
    key_group        int,
    owner            blob   static,
    serving_wm       bigint static,
    business_key     blob,
    serving_attempt  blob,
    serving_E        bigint,
    PRIMARY KEY ((namespace, key_group), business_key)
);

-- Skinny GC index only: no payloads, no attempt/epoch.
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

- **There is no `window_recovery_bases` table** and no `window_kg_lease`
  singleton. WO overlay uses in-memory slices. WRO uses `window_pins`.
- `window_kg_buckets`: skinny GC index. `commit_events` upserts
  `business_key` (no `attempt`/`epoch`, no payload). `maintain` per owned
  `key_group`:

  ```cql
  SELECT * FROM window_kg_buckets
   WHERE namespace = ? AND key_group = ?
     AND bucket_start < ?   -- floor_bucket, not data_floor
  ```

  For each hit, delete that key’s `window_raw` partition for the bucket
  **and** every `window_tiles` partition for the same key+bucket, then the
  index row.
- `window_raw`: **one CQL row per event**, clustered by event time. Do not
  pack a batch into one blob.
- `window_tiles` / `window_key_states`: versioned; overlay / WRO filters
  `attempt` / `epoch` in the client.
- `window_triggers`: immutable due work, **versioned like raw**. Locality is
  `(namespace, bucket_start, kg_shard)`. Cluster by `fire_ts` first.

  ```text
  kg_shard = key_group * SHARD_COUNT / max_parallelism
  ```

  `SHARD_COUNT` is a store constant (e.g. 32 or 64), not `p`. WRO does not
  read triggers.

---

## WO write

For one key (streaming and request ingest are the same data path):

1. Derive `key_group` from the key hash and the client's `max_p`.
2. Load writer `KeyState` (WO overlay). No pin read on streaming.
3. Allocate `E = ++next_E[g]` (seed `cp_E_g` on first touch).
4. UNLOGGED BATCH: changed raw, tiles, `KeyState`, triggers under
   `(me, E)`. Upsert `window_kg_buckets` unversioned.
5. Request-mode only: if `publish_ok`, pin LWT for `dirty`. Not on the data
   batch.

Each `(key, bucket)` is one Scylla partition. `commit_events` issues one
**UNLOGGED BATCH** per such partition. Do not use logged BATCH. Do not batch
across partitions.

A new epoch starts only after the previous write outcome is known. Request
ownership CAS failure (steal or publish) stops the WO as fenced.

`store_key_state` follows the same protocol but writes no raw, tile, trigger,
or bucket-index rows.

---

## WO reads

```text
visible if
  attempt == me
  OR (attempt == cp_g.attempt AND E <= cp_E_g)
```

Until restore there is no `cp` — overlay is `me` only. After rescale, `cp_g`
is the `VersionedRange` slice that covers this `key_group`.

Because `attempt` is clustering, overlay is one `LOCAL_QUORUM` read per data
partition: keep the newest row that matches, client-side.

Logical runs are mapped to time buckets, loaded, merged, and filtered back
to the exact requested ranges. Raw rows are deduplicated by `Cursor`.
Same-cell collisions among visible rows: `max(E)`, then `max(attempt)`.

`load_triggers` is one CQL hop and the **same overlay**. Mid-shard seek is
the last **raw** clustering row (full tuple, not `fire_at` alone), even if
overlay-hidden.

```text
WHERE namespace = ? AND bucket_start = ? AND kg_shard = ?
  AND (fire_ts, fire_seq, business_key, trigger_kind, window_id, attempt, epoch)
      > (?, ?, ?, ?, ?, ?, ?)
  AND fire_ts <= ?
LIMIT ?
```

The client drops `key_group` outside the bound range and applies the WO
overlay. Resume is a private `Seek`.

Do not use `OFFSET`, native `PagingState`, a second `fire_ts >=` query,
seek on `fire_ts` alone, or `execute_unpaged` of the whole range.

---

## WRO reads

1. Hash the key → `key_group`. Point-get `window_pins` for
   `(namespace, key_group, business_key)`. If no serving row, return empty.
   Otherwise **pin once**: `(serving_attempt, serving_E)`. Do not reread
   mid-request.
2. Build exact raw/tile plans.
3. Load from the same data partitions (time slice in CQL). Filter
   **client-side**:

   ```text
   attempt == serving_attempt AND E <= serving_E
   ```

4. Collapse **per cell**: same `Cursor` / `(granularity, tile_start)` /
   `key_state` → `max(E)`, then `max(attempt)`.
5. Merge, order, deduplicate, rebuild.

If WO is unavailable, WRO continues serving the unchanged pin.

---

## Checkpoint

At an aligned barrier:

1. Complete pending writes. **Do not** publish pins here.
2. Capture `attempt` plus `cp_E[g]` for every owned group.
3. Return `WindowBackendSnapshot::Versioned`. The operator wraps it with
   namespace + `watermark_frontier` in `WindowStateSnapshot`.
4. Continue processing at later epochs.

Durable triggers already represent work above that watermark, so checkpoint
neither drains nor serializes an operator-local pending-key set.

---

## Recovery

1. Planner produces `Vec<VersionedRange>` (same assignment: one slice).
2. Scylla client uses the new **job** attempt. Overlay uses the slices.
   Writer `next_E[g]` starts at `cp_E_g`. Never restart from a pin epoch.
3. Source restores its checkpoint offset and replays post-checkpoint input.
4. Resume watermark work by streaming checkpoint-visible triggers above the
   restored watermark. Replay advances only writer state.
5. Request-mode: steal `owner` per owned group. Leave `serving_*` at the
   previous cut. `publish()` stays gated on `publish_ok`.
6. Streaming: no steal, no publish.

During recovery, writer data and (if request) serving pins differ. Replay
advances MVCC rows; WRO keeps using per-key `serving_*` (including idle
keys). A zombie publish after steal does not apply.

---

## WO cache

Foyer is optional and WO-only. It is **not** a HeadClaim / per-key lease
cache. Do not keep unbounded DashMaps of claim state.

```text
meta:      PartitionKey -> KeyState
data:      (PartitionKey, family, bucket) -> materialized writer-view data
triggers:  (namespace, bucket, kg_shard) -> immutable due entries
```

The cache is cleared before each execution attempt. WRO bypasses cache.

---

## Scylla consistency

Minimal setup is single DC, RF=3. Use `LOCAL_QUORUM` for data reads and
writes (`W+R > RF`). Request steal/publish is LWT with `LOCAL_SERIAL` +
learn `LOCAL_QUORUM`:

- **WO write:** non-LWT data @ `LOCAL_QUORUM`. Streaming: never LWT.
  Request: steal is LWT at restore per group. Publish is a separate LWT on
  the configured cadence, gated by `publish_ok`.
- **WO read (cache miss):** overlay on versioned tables @ `LOCAL_QUORUM`.
- **WRO read:** pin + data @ `LOCAL_QUORUM`.

**Not v1:** `USING TIMESTAMP` instead of owner CAS; ingest CAS
(SlateDB-style write fence).

---

## State prune / cleanup

**Eligibility (per task):** after watermark advance to `W`,
`data_floor = W − max_window_length − lateness` (lateness default `0`).
Prune is limited to the task's owned key-group range.

**Executor (per worker):** `StateRegistry::run_maintenance_once` — parallel
loop per `OperatorKind`, then per-task `OperatorStore::maintain(ns, state)`.

**Window `maintain` body:**

- drop consumed triggers (`fire_at.ts ≤ W`) with a `fire_ts` clustering
  range delete on shards overlapping the task's key-group range;
- from `window_kg_buckets`, per owned `key_group`,
  `SELECT … AND bucket_start < floor_bucket`, then delete matching raw/tile
  partitions and the index row;
- drop rows that are in **neither** WO overlay (`me` ∪ `cp_g`) **nor** a
  request serving pin `(serving_attempt, E ≤ serving_E)` (if WRO is on);
- do **not** GC with a group-wide epoch high-water;
- no `window_recovery_bases` walk.

**In-flight WRO:** `load_window_data` pins serving at start. The remaining
race is **event-time** GC vs a request whose window still sits below
`data_floor`. v1: never delete the current serving pointer; delay dropping
buckets below `floor_bucket` by at least the maximum WRO request budget
(grace). No pin-refcount table.

TTL/TWCS may expire physical SSTables only when consistent with logical
`data_floor` **and** the serving-pin grace. InMem applies the same logical
rules immediately inside `maintain`.

---

## Explicitly not v1

- Publish on checkpoint (the `> cp_wm` gate covers pin-behind-CP).
- Periodic WO-only owner suicide. Overlay + master kill.
- `high_E`, `prev_E`, `window_kg_lease` singleton, `window_recovery_bases`.
- Ingest LWT, copy-on-restore, any-attempt overlay, pin = CP as the only
  publish, per-key catch-up freeze, HeadClaim / DashMaps.

---

## Future next steps

Worker maintenance **orchestration** is decided in the runtime; Scylla's
`maintain` implementation remains TODO. `RestorePlanner` identity mapping
on master must grow the range intersection described above.

### Namespaced cache quota

Assign each state consumer its own memory and local-disk quota.
`StateResourceTracker` is the scaffold.

### Mem pressure / backpressure

If eviction cannot keep a consumer within quota, backpressure upstream.
Remote state must not be a bottomless overflow sink.

### CDC and late events

Append-only today. An event at or behind the task watermark is dropped.
`lateness` only extends retention. Future CDC must identify logical rows
independently of arrival cursor and represent inserts/updates/deletes
explicitly. WRO would continue reading one coherent serving version.

### WO-only zombie suicide (follow-up)

Streaming has no `owner` row. A later safeguard can reuse request `owner`
(restore sets it, periodic `IF owner = me`) or ask master whether the
attempt still owns the vertex. Not required for read correctness.
