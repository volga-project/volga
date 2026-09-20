# Window store with Scylla design

`WindowOperatorStore` serves the single WO owner of a key group.
`WindowRequestStore` serves WRO point lookups. Physical layout, MVCC,
serialization and caching stay inside the backend. For operator semantics,
evaluation flow and module structure see the
[window operator README](README.md).

Two ideas carry the whole protocol:

1. **Every row carries a totally ordered `Version = (attempt, epoch)`**, and
   visibility is a **per-key-group cut history** — a short sorted list of at
   most one `(attempt, epoch)` prefix per attempt. Older attempts keep their
   prefix forever, so pre-crash history never disappears, and post-checkpoint
   zombie writes stay above their attempt's prefix and are excluded
   permanently.
2. **Publication is the checkpoint.** The cut a reader sees is the cut of the
   last *completed* checkpoint. There is no separate publish path, no
   ownership lease, no per-key pin, and no clamp: the published cut is exactly
   the cut a successor restores from, so it only ever grows.

Everything else follows. This document is the normative protocol; it
supersedes the earlier drafts in
[#298](https://github.com/volga-project/volga/pull/298) and
[#299](https://github.com/volga-project/volga/pull/299).

The data contract is append-only. CDC and late-event correction need explicit
mutation semantics — future work.

---

## Modes

| | Streaming (WO only) | Request (WO + WRO) |
|---|---|---|
| Data tables, due index, `window_kg_buckets` | yes | yes |
| Versions, cut history, checkpoint cuts | yes | yes |
| `window_kg_meta` (published cut, floor, `cur_attempt`) | no | yes |
| WRO reads, `ReadOptions` | no | yes |
| Triggers and downstream emit | yes | per-operator `WindowOutputMode` |

Ingest is always **UNLOGGED** and identical in both modes. No ingest LWT, no
per-business-key worker maps.

`WindowOutputMode` is per operator, not per mode: a multi-operator graph may
run interior operators as `Emit` and only the tail operator at the read cut as
`StateOnly`. Request mode does not globally disable triggers.

---

## Contract types

Logical models; backend serialization may differ. This is an engine contract
(WO, WRO, InMem, Scylla). Physical layout may add columns such as `key_group`;
it must not change what `StateNamespace` means.

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

`KeyGroupRange`, `key_group_of` and `subtask_of` are landed in
`src/common/key_group.rs`:

```text
key_group = hash % max_parallelism
subtask   = key_group * p / max_parallelism
```

Each WO/WRO task owns a contiguous `KeyGroupRange`. `max_parallelism` is
job-wide and must not change for a pipeline incarnation.

- `StateNamespace` is the operator state space, not a task. It is fixed at
  compile time and shared by the WO and WRO sides of one operator, which is
  what lets request serving be deployed independently of the streaming
  topology. Task isolation is the owned key-group range, bound on the
  per-task store client at open.
- `PartitionKey` must stay collision-safe. Persist the serialized
  business-key bytes; derive `key_group` from `Key.hash` and
  `max_parallelism`.
- `Cursor` is raw event identity and total order within a partition. `seq_no`
  is allocated from `KeyState.next_seq` at ingest, so it is **not** stable
  across replays that reorder arrivals — see *Accepted divergences*.
- `RawRun` and `TileRun` are half-open.
- `watermark_frontier` is the task-level late-data boundary, stored in and
  restored from the operator checkpoint.

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
Attempt         = durably monotonic execution attempt (see below)
```

The client derives `key_group` from `Key.hash` and the bound `max_p`. Per-key
calls must land in the bound range.

WRO reuses the same namespace and addresses rows by `PartitionKey`. It shares
no runtime context with WO: it reads `window_kg_meta` for everything it needs
about the live execution. Request routing still sends a lookup to the WRO task
that owns the key, but that is locality only — any WRO task can answer any key
in the namespace.

`OperatorStore` is the generic maintenance port on the shared physical
backend.

## Store traits

Landed on master (`WindowOperatorStore` / `WindowRequestStore`). Required
behavior this protocol relies on:

- Missing partitions and empty runs return empty/default results.
- `commit_events` publishes raw rows, replacement tiles, key state and due
  work **for one key** at a single `Version`. It is atomic **at the version
  level, not the CQL level** — see *Commit atomicity*. Retries at the same
  version are idempotent.
- The due-work read is one hop and uses the same filter as WO data reads. WRO
  does not read due work.
- `checkpoint` completes pending writes and returns a backend snapshot of the
  client's bound range only. No Scylla round trips beyond draining in-flight
  writes.
- `restore` starts the cut history from the supplied checkpoint / planner
  slices and must not clobber keys outside the bound range.
- `load_window_data` takes `ReadOptions` and returns one coherent snapshot
  plus the metadata needed to judge it (see *Read contract*).

`InMemWindowStore` is the reference physical backend; one lock provides the
snapshot boundary. Scylla implements the same client contract with MVCC.

---

## Identity and clocks

```text
business_key   window key
key_group      hash % max_parallelism          // stable under rescale
subtask        key_group * p / max_parallelism
attempt        durably monotonic, never reused, job-wide
epoch          per key_group write-batch counter, local, starts at 0
Version        (attempt, epoch), lexicographic total order
watermark      task-global, non-decreasing
```

`epoch` is a **write-batch counter**, not event time, and not the per-row
`seq_no` in `Cursor`. It only has to increase within one `(attempt,
key_group)`, so a plain local counter from zero satisfies it. It does **not**
need seeding from a checkpoint cut, because `attempt` is the high-order
component of the order.

`attempt` is job-wide: every WO task of one execution shares it.

### Attempt allocation (master prerequisite)

`attempt` must be monotonic and never reused **across master restarts**,
because Scylla data outlives the master process. Neither obvious source works
alone:

- `MasterLifecycle::attempt_id` increments per recovery but resets to `0` on
  pipeline start (`src/runtime/master/lifecycle.rs`). A new incarnation at
  attempt `0` sits below every existing cut, so its writes are filtered out
  and its cells lost to LWW.
- `max(restored cut attempts) + 1` repeats: attempts 2 and 3 commonly restore
  from the **same** checkpoint, so both compute the same value and share a
  version space with each other's zombie.

Required: the master allocates the attempt from **durable** state
(`max(observed) + 1`), persists it **before** configuring workers, and passes
it to tasks with restore data. Assertions: the new attempt exceeds every
attempt in any restored cut history, and no attempt is handed to two
executions. Tracked by
[#156](https://github.com/volga-project/volga/issues/156). Until it lands the
Scylla backend must refuse to open.

### Checkpoint-completion notification (master prerequisite)

Tasks today see only the barrier. The master knows
`latest_complete_checkpoint` and records `LifecycleEvent::CheckpointCompleted`
(`src/runtime/master/state.rs`), but never tells the tasks.

The protocol needs it: the cut may only be published once its checkpoint has
**completed** globally. Publishing at the barrier would let the published cut
name writes of a checkpoint that later aborts; a successor then restores from
an earlier checkpoint and replays that input, and the reader would see both
the old rows and the replayed rows. Repairing that requires clamping another
attempt's entry down — the single largest source of complexity in the earlier
drafts, and it disappears entirely if publication waits for completion.

Required: `notify_checkpoint_complete(checkpoint_id)` delivered to tasks and
dispatched on the operator next to barrier handling, default no-op. This is
the standard engine capability and is independently useful (source offset
commit). Tracked by
[#300](https://github.com/volga-project/volga/issues/300).

Delivery may be at-most-once — a missed notification is repaired by the heal
at open (*Writing it*, trigger 2), not by waiting for the next checkpoint.
Until this lands, request mode must refuse to open.

---

## Protocol (normative)

### Version stamping

- raw / tiles / `key_state` / due work cluster `(attempt, epoch)`.
- `epoch` is allocated locally per `key_group`. **No LWT** to mint it.
- Ingest: same-partition **UNLOGGED BATCH** only.

```text
epoch = next_epoch[g]++      // starts at 0 on first touch in this attempt
stamp (my_attempt, epoch)
```

Increment during catch-up. Do not gate on anything.

### Cut history

```rust
pub type Attempt = u64;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Version {
    pub attempt: Attempt,
    pub epoch: u64,
}

/// One per key group. Sorted ascending by attempt, attempts unique.
#[derive(Clone)]
pub struct CutHistory {
    entries: Vec<Version>,
}

impl CutHistory {
    pub fn allows(&self, v: Version) -> bool {
        match self.entries.binary_search_by_key(&v.attempt, |e| e.attempt) {
            Ok(i) => v.epoch <= self.entries[i].epoch,
            Err(_) => false,
        }
    }

    /// Add or raise **only the caller's own** entry. Never touches another
    /// attempt's entry.
    pub fn advance(&self, me: Attempt, acked_prefix: Option<u64>) -> CutHistory {
        let mut next = self.clone();
        // No acked write for this group in this attempt: contribute nothing.
        // Never insert `(me, 0)` — `epoch` is 0-based, so a zero entry is
        // indistinguishable from "epoch 0 is committed".
        let Some(epoch) = acked_prefix else { return next };
        match next.entries.binary_search_by_key(&me, |e| e.attempt) {
            Ok(i) => {
                assert!(next.entries[i].epoch <= epoch, "own prefix must not go backwards");
                next.entries[i].epoch = epoch;
            }
            Err(i) => {
                assert!(
                    next.entries.get(i).map_or(true, |e| e.attempt > me),
                    "attempt must dominate every inherited entry",
                );
                next.entries.insert(i, Version { attempt: me, epoch });
            }
        }
        next
    }
}
```

A row whose attempt has **no** entry is outside the cut. That is the right
answer for an attempt that wrote but never checkpointed: all of its writes are
post-checkpoint and will be replayed.

Because a cut is only ever published after its checkpoint completes, and a
successor inherits exactly that checkpoint's history, `advance` is the only
mutation the history ever needs. The published cut is monotonic in both
coverage and row membership.

Size is `O(attempts)` per group at 16 bytes per entry. v1 does not retire
entries — see *Accepted divergences*.

### Acked prefix

A cut may only name an `epoch` whose writes are all durable:

```text
cut_top[g] : Option<u64>
           = max epoch such that EVERY write at (my_attempt, e <= epoch)
             for group g has been ACKED
           = None if no write in this attempt has acked yet
```

Track it as a **low-water mark** over in-flight epochs per group. This is what
makes cross-partition commits safe and torn commits invisible.

`epoch` is allocated by an **atomic** per-group counter, because ingest runs
up to `ingest_key_concurrency` keys concurrently and they can share a group.
Concurrent in-flight epochs in one group are fine; the prefix is what makes
them safe.

The prefix is a prefix, **never a skip**. If epoch 5 times out and 6 acks,
`cut_top` is 4. Key Y's data at 6 is durable and visible to its writer, but no
cut may name it until 5 resolves. Do not skip the hole; do not allocate around
it. A write that times out has an unknown outcome, so retry it at the **same**
`Version` until acked, or fail the task.

### WO visibility

```text
visible(row) ⇔ row.attempt == my_attempt          -- read your own writes
               OR cp_cut[g].allows(row.version)
```

`cp_cut[g]` is the cut history restored for group `g`. Before the first
restore of a job it is empty, so visibility is `my_attempt` only. The due-work
read uses the same filter.

Zombie rows sit above their attempt's entry, so they are invisible
permanently, to every later attempt. That is the v1 **read fence** for
streaming. No keys and no cells disappear after failover: every attempt that
ever checkpointed keeps its prefix.

### Commit atomicity

One logical `commit_events` touches several partitions — `window_raw` per
bucket, `window_tiles` per granularity × bucket, `window_key_states`,
`window_kg_buckets`, the due index. Cross-partition batches are forbidden, so
there is no CQL-level atomicity and a crash can tear a commit.

The version restores it:

- All writes of one commit carry the **same** `Version`.
- A torn commit leaves a partial set at `(attempt, epoch)`. Because the
  acked-prefix rule never advances a cut past an unacked write, a torn commit
  always sits **above** every cut, so no cut-filtered reader ever sees it.

WO reads of its own attempt are **not** capped by the prefix, so the writer
can see its own torn commit. The rule is **per key**:

> While a commit for key `K` is in flight, the writer issues no read of `K`
> and no second commit for `K`. `commit_events` either returns with every
> write acked, or the task fails.

Cross-key concurrency inside a group stays legal, which is the point. Do
**not** instead cap the writer at `epoch <= cut_top[g]`: with key X's epoch 5
unresolved and key Y's epoch 6 acked, the group prefix is 4, so Y's own
committed state would become invisible to its writer, which would re-ingest
and re-apply it. Read-your-writes for the current attempt is uncapped on
purpose.

Fresh reads (below) can observe a torn commit, and the bound on what that
means is part of the Fresh contract.

State all of this in the trait docs. "Atomic" without the version qualifier
will be built on and is wrong.

### Cell collision

Among visible rows for the same cell — same `Cursor`, same
`(granularity, tile_start)`, or `key_state` — keep the greatest `Version`,
compared lexicographically: `attempt` first, then `epoch`. The filter is
client-side for raw and tiles, since clustering is time-first: CQL slices
time, version comparison happens in process. Deduplication by key must run
**after** version filtering, or rows from a filtered-out attempt can win.

### Local RAM (bounded)

| State | Scope |
|---|---|
| `my_attempt` | task |
| `cp_cut[g]`, `cp_wm` | restore, once; `O(groups x attempts)` |
| `next_epoch[g]` (atomic), in-flight epochs, low-water mark | per owned group |
| last published checkpoint id | task; scalar |

No per-business-key epoch map, no dirty-key set, no claim/lease maps.

---

## Published metadata

`window_kg_meta` is the entire interface between the streaming side and the
serving side. One row per key group, `max_parallelism` rows in the table.

```sql
-- Request mode only. Streaming WO neither reads nor writes this table.
CREATE TABLE window_kg_meta (
    namespace       blob,
    key_group       int,
    cur_attempt     bigint,   -- live execution, for fresh reads
    cut             blob,     -- encoded CutHistory of the last COMPLETED cp
    prev_cut        blob,     -- the cut this one replaced; GC grace
    committed_wm    bigint,   -- task watermark_frontier at that checkpoint
    retention_floor bigint,   -- nothing below this is guaranteed to exist
    checkpoint_id   bigint,
    PRIMARY KEY ((namespace, key_group))
);
```

### Writing it

**Two** statements, per owned key group, issued in parallel across groups.
Do not merge them.

`prev_cut` must come from the **row being replaced**, never from the writer's
memory of what it last published. After a failover the successor has
published nothing, so a RAM-derived `prev_cut` would be empty exactly when
readers are most likely to be holding an older cut — see *Per-cell version
retention*. That is why publication is a read-then-CAS and not a blind write.

```text
row = SELECT * FROM window_kg_meta WHERE namespace = ? AND key_group = ?

-- (A) publish: the cut moves forward
publish(payload):                       -- payload = cut, wm, floor, cp_id
    UPDATE window_kg_meta
       SET cur_attempt   = me,
           prev_cut      = row.cut,     -- the value being replaced
           cut           = payload.cut,
           committed_wm  = payload.wm,
           retention_floor = payload.floor,
           checkpoint_id = payload.cp_id
     WHERE namespace = ? AND key_group = ?
       IF cur_attempt <= me AND checkpoint_id = row.checkpoint_id

-- (B) take the attempt: the cut does not move
take_attempt():
    UPDATE window_kg_meta
       SET cur_attempt = me
     WHERE namespace = ? AND key_group = ?
       IF cur_attempt <= me
```

**(B) is not (A) with an unchanged cut.** Running (A) with
`payload.cut = row.cut` would also set `prev_cut = row.cut`, collapsing the
two retention slots into one. GC would then drop the superseded tile and
`key_state` versions that a WRO request on the previous generation is still
filtering against, and that request undercounts silently. The grace slot
exists precisely to survive a restart, so destroying it at open would defeat
the mechanism at the one moment it is needed.

Not applied ⇒ re-read and retry, or skip if the row already carries
`checkpoint_id >= payload.cp_id`. Each of these runs once per group per
checkpoint or per open, so a retry costs nothing.

The `checkpoint_id` condition on (A) is doing the real work. `IF cur_attempt
= me` alone is not a fence: `attempt` is job-wide, so it cannot order two
writes from the **same** attempt, and completion notifications are not
ordered against each other — a delayed publish for checkpoint `N` can land
after `N+1` and walk the published cut backwards. A compare-and-swap on
`checkpoint_id` makes publication monotonic regardless of delivery order, and
incidentally makes it irrelevant whether two writers of one attempt can ever
own the same group.

**Trigger 1 — checkpoint completion.** (A) with the cut captured at that
barrier. One LWT per group per checkpoint; at `max_p = 128` and a 30s
interval that is a few per second.

**Trigger 2 — WO open, request mode.** (A) with the **restored** cut if
`row.checkpoint_id < restored_checkpoint_id`, else (B).

Trigger 2 is what makes at-most-once completion delivery safe. A missed
notification or a failed CAS otherwise leaves the published row at `N-1`
while the durable blob is at `N`, and nothing repairs it until the next
checkpoint completes — which, for a job crash-looping before it can
checkpoint, is never. Healing at open bounds published staleness to the
checkpoint the attempt actually restored from. The restored cut is by
definition a completed checkpoint's cut, so publishing it is safe for the
same reason trigger 1 is.

**Absent row.** `INSERT ... IF NOT EXISTS`, and only from a publish: the
first completed checkpoint creates the row, or a heal does when the row is
missing but a restored checkpoint exists. **Open does not create the row on a
fresh job.** There is nothing to put in it — no cut, and no
`retention_floor`, so the coverage guard would have nothing to check and
`Fresh` would serve rows with no coverage guarantee. Leaving it absent keeps
one rule for both modes: WRO returns empty until the first checkpoint
completes.

`retention_floor = committed_wm - max_window_length - lateness`, the same
expression WO uses for retention eligibility, evaluated on the **committed**
watermark rather than the live one. Publishing it is what turns retention into
something a decoupled reader can reason about, and it is why **GC deletes only
below the published floor** (see *Retention and GC*). Both properties come
from one rule.

Nothing here is on the ingest hot path.

---

## Read contract

A read states what it wants to see. This is the only place freshness is
configurable, and it is the seam that previously let WO and WRO disagree about
the same query.

```rust
pub enum ReadOptions {
    /// As of the last completed checkpoint. Deterministic, replayable,
    /// identical to what the WO emit path produced for the same range.
    Committed,
    /// As of now, best effort. Head-scoped; see below.
    Fresh,
}
```

`load_window_data` returns the data plus `committed_wm` and the
`checkpoint_id` it was answered at, so a caller can judge staleness without a
second call.

### Coverage guard

WRO is not watermark-driven and must not pretend to be, but it does have to
refuse queries it cannot answer. For a request over `[lo, hi]`:

```text
if lo < retention_floor: refuse (range no longer retained)
```

No upper guard: `hi` above the data is not an error, it just means the tail is
empty. Note that a normal "as of now" query always passes, because
`lo = hi - window_length` with `hi ≈ committed_wm` and `window_length ≤
max_window_length`. Only genuinely historical queries are refused. This
replaces the `// No lateness filter. Answer from whatever state the backend
still retains.` comment in `request.rs`, and removes the `lateness: 300_000`
workaround in `tests/matrix.rs`.

### Committed

```text
visible(row) ⇔ cut.allows(row.version)
```

Read `window_kg_meta` once per request and use that cut for every page. Absent
row or empty cut ⇒ empty result, no data reads. If WO is down, WRO keeps
serving the last completed checkpoint.

This is the mode that makes the split-path abstraction hold, but the
equivalence is **at the cut, not at the instant**. A live `Emit` WO computes
from `my_attempt | cp_cut`, which includes its own post-checkpoint writes; a
`Committed` read sees only the last completed cut. So at any given moment the
emitted value for a window can be ahead of what `Committed` returns for the
same range, and the two converge when the checkpoint covering those writes
completes.

The property to hold onto — and to test — is that both surfaces compute the
same function of the same input set:

> For a range `[lo, hi]` whose events are all inside the published cut, a
> `Committed` read returns what the WO emitted for `[lo, hi]`.

Which makes the test a quiesce, not a race: drain the input, let one
checkpoint complete, then compare emitted rows against `Committed` reads. At
that point the WO's own view and the published cut coincide, so any
difference is a real defect rather than publication lag. Asserting the
equivalence against a live emit path would be asserting that lag is zero,
which this design deliberately does not promise.

### Fresh

Fresh adds the current attempt's uncommitted tail, scoped to the head so it
stays cheap:

```text
H = committed_wm

[lo, H]   read Committed: tiles + raw at the cut
(H, hi]   read raw only, visible ⇔ cut.allows(v) OR v.attempt == cur_attempt
```

Interior tiles are always read at the cut; only the head raw slice is read
above it, and the existing incremental merge combines them. `(H, hi]` is
roughly one checkpoint interval of raw data, so the extra cost is one bounded
raw slice per request and no additional tile reads.

Two bounded consequences, both stated to callers rather than engineered away:

- **A torn commit can be partially visible.** Because Fresh only widens
  *raw* above the cut — never tiles or key state — a torn commit appears as a
  subset of one batch's events, which is indistinguishable from those events
  not having arrived yet. It cannot produce a corrupt aggregate.
- **Late arrivals below `H` are not visible until their checkpoint
  completes.** A late event accepted after the last checkpoint lands below the
  head scope, so Fresh misses it for at most one checkpoint interval, after
  which the normal `Committed` path picks it up. Making that window zero
  requires scanning for post-cut writes across the whole window on every
  request, which is the cost this scoping exists to avoid.

Zombie fencing on the read path is `v.attempt == cur_attempt`, read from
`window_kg_meta`.

### Metadata read, and why it is not cached in v1

v1 reads `window_kg_meta` on every request: two hops, the first into a
single-row partition, one of `max_parallelism` in a tiny table.

A request pins its cut when it reads the row, and GC retains the version
named by `prev_cut` — **one** generation of slack (see *Per-cell version
retention*). So a request that outlives a checkpoint interval can end up
filtering on a cut whose versions GC has already collected, and the symptom
is a silent undercount rather than an error.

That makes the bound a real limit, not an assumption:

```text
wro_request_timeout < checkpoint_interval
```

Enforce it. Give WRO an explicit request deadline, assert the inequality at
configure time, and fail the request on expiry rather than letting it read
against a cut that is two generations old. The margin is wide — milliseconds
against tens of seconds — which is precisely why it should be checked rather
than assumed.

Caching the row per WRO task would remove the second hop, and is left for
later because it turns that structural bound into a tuning one:
`TTL + request timeout < checkpoint_interval`. Adding the cache means either
accepting that bound explicitly or widening retention beyond one previous
cut.

A stale `cur_attempt` would only affect Fresh, which is best effort by
definition, so it is not the constraint here.

---

## Request-mode ingest

Ingest is the same write path in both modes. Two behaviors differ, and both
follow from the floor rather than from a mode flag:

- **Admission.** Streaming drops rows at or behind the task watermark, because
  the emit path has already fired for that time. A `StateOnly` operator has no
  emit path, so the only reason to drop a row is that its bucket may already
  be gone. Admit on `ts > retention_floor` instead of `ts > watermark`. Late
  events therefore update state and become visible at the next checkpoint.
- **Retention.** Unchanged in shape, but derived from the committed watermark
  (above). Watermarks are still required in request mode for exactly this
  reason: without them the window never moves and nothing can be reclaimed.
  They are no longer required to decide *answers*.

---

## Checkpoint

At an aligned barrier:

1. Complete pending writes; resolve or fail any timed-out write.
2. Capture `attempt` plus `cuts[g].advance(attempt, cut_top[g])` for every
   owned group. `cut_top[g] == None` leaves inherited entries untouched and
   adds nothing.
3. Return `WindowBackendSnapshot::Versioned`; the operator wraps it with
   namespace and `watermark_frontier`.
4. Continue processing at later epochs.

On the **completion** notification for that checkpoint id, request mode writes
the captured cut, `committed_wm` and `retention_floor` to `window_kg_meta`
(see *Published metadata*). The barrier itself costs **zero** Scylla round
trips beyond draining in-flight writes.

```text
WindowStateSnapshot
  watermark_frontier     // cp_wm
  backend:
    attempt              // this writer
    cuts[g] for g in the client's bound range
```

```rust
pub enum WindowBackendSnapshot {
    InMemory { snapshot: Vec<u8> },
    Versioned {
        attempt: Attempt,
        /// Parallel to the client's bound `KeyGroupRange`.
        cuts: Vec<CutHistory>,
    },
}

/// Restore/remap instruction. Not a Scylla table.
pub struct VersionedSlice {
    pub range: KeyGroupRange,
    pub attempt: Attempt,
    pub cuts: Vec<CutHistory>,
    pub cp_wm: i64,
}
```

Because `epoch` is per group, the cut cannot be one task-wide value: a slow
group's post-checkpoint writes would fall under a task-wide `epoch <= max`.

Groups this writer never touched need no special case: the history simply has
no entry for this attempt, and inherited entries are still there.

---

## Restore and rescale

`key_group` does not move; parallelism only changes the owner.

```text
max_p = 8
p=2:  task0 [0,4)   task1 [4,8)
p=4:  task0 [0,2)   task1 [2,4)  task2 [4,6)  task3 [6,8)
```

**Do not recompute watermarks from data.** Three clocks, three sources:

| Clock | Source |
|---|---|
| Slice `cp_wm` / `cuts[]` | Source task blob |
| Task `watermark_frontier` | `min(slice cp_wm)` |
| Published cut / floor | Unchanged on `window_kg_meta` until the next completion |

**Why min:** advancing to `max` would skip fires for keys from a slower
parent. Min can accept a little late data on keys from the faster parent;
per-key last-fired in `KeyState` still blocks duplicate emits.

**Planner:** decode each source blob, intersect ranges with each target task's
owned groups → slices. Same assignment = one slice = the old blob.

```text
max_p = 128, rescale p=3 -> p=2:
old:  task0 [0,43)   task1 [43,86)   task2 [86,128)
new:  task0 [0,64) <- [0,43)@old0 + [43,64)@old1
      task1 [64,128) <- [64,86)@old1 + [86,128)@old2
```

Histories are **per group**, so slices never need merging: a group appears in
exactly one source blob. Because attempts are globally ordered, histories from
different source tasks are directly comparable and one new attempt dominates
all of them.

**Target `restore`:**

1. Take the master-allocated `attempt`; assert it exceeds every inherited
   attempt.
2. `cp_cut[g]` and slice `cp_wm` from the slice containing `g`.
3. Task frontier = `min(slice cp_wm)`.
4. `next_epoch[g] = 0`.
5. Request: one `window_kg_meta` CAS per owned group, in parallel — take
   `cur_attempt`, and publish the restored cut if the row is behind it
   (*Writing it*, trigger 2).
6. Replay sources from this barrier. **Do not copy** cells.

**Bootstrap (first start):** `cp_cut[g]` empty so WO visibility is
`my_attempt` only; `cp_wm` unset; `next_epoch[g] = 0`; `window_kg_meta` row
absent, so WRO returns empty until the first checkpoint completes.

**What WRO sees across a failover.** It serves the last completed
checkpoint's cut throughout — the same state the new attempt restored from —
and switches to the new attempt's cut when that attempt's first checkpoint
completes. If the published row had fallen behind the restored checkpoint,
the successor's open advances it forward immediately rather than leaving it
stale for another interval. There is no window in which the published cut
names rows that get replayed, so no clamp. Freshness cost of a failover is
recovery time plus one checkpoint interval, or, in Fresh mode, recovery time
(with replay possibly partially applied — accepted).

---

## Tables

`attempt` is **not** in any data partition key. It is clustering, so the
filter is a visibility test on one `LOCAL_QUORUM` read, and restore copies
nothing. Version clustering is **descending** (`attempt DESC, epoch DESC`) so
the newest version of a cell sorts first and readers stop early.

**GC default: skinny index plus per-key data PK. Do not cluster
`business_key` under `(namespace, key_group, bucket)` on the data tables** —
`load_raw` is per key and a key group is unbounded, so that clustering would
put every key in the group+bucket into one partition. That layout cannot list
expired buckets, so `commit_events` also writes one payload-free index.

```sql
-- Skinny GC index: no payloads, no versions.
CREATE TABLE window_kg_buckets (
    namespace    blob,
    key_group    int,
    bucket_start bigint,
    business_key blob,
    PRIMARY KEY ((namespace, key_group), bucket_start, business_key)
) WITH CLUSTERING ORDER BY (bucket_start ASC, business_key ASC);

CREATE TABLE window_raw (
    namespace    blob,
    key_group    int,
    business_key blob,
    bucket_start bigint,
    event_ts     bigint,
    seq_no       bigint,
    attempt      bigint,
    epoch        bigint,
    payload      blob,
    PRIMARY KEY (
        (namespace, key_group, business_key, bucket_start),
        event_ts, seq_no, attempt, epoch
    )
) WITH CLUSTERING ORDER BY (
    event_ts ASC, seq_no ASC, attempt DESC, epoch DESC
);

CREATE TABLE window_tiles (
    namespace      blob,
    key_group      int,
    business_key   blob,
    granularity_ms bigint,
    bucket_start   bigint,
    tile_start     bigint,
    attempt        bigint,
    epoch          bigint,
    payload        blob,
    PRIMARY KEY (
        (namespace, key_group, business_key, granularity_ms, bucket_start),
        tile_start, attempt, epoch
    )
) WITH CLUSTERING ORDER BY (tile_start ASC, attempt DESC, epoch DESC);

CREATE TABLE window_key_states (
    namespace    blob,
    key_group    int,
    business_key blob,
    attempt      bigint,
    epoch        bigint,
    key_state    blob,
    PRIMARY KEY ((namespace, key_group, business_key), attempt, epoch)
) WITH CLUSTERING ORDER BY (attempt DESC, epoch DESC);

CREATE TABLE window_due (
    namespace    blob,
    bucket_start bigint,
    kg_shard     int,
    fire_ts      bigint,
    fire_seq     bigint,
    business_key blob,
    trigger_kind tinyint,
    window_id    bigint,
    key_group    int,
    attempt      bigint,
    epoch        bigint,
    PRIMARY KEY (
        (namespace, bucket_start, kg_shard),
        fire_ts, fire_seq, business_key, trigger_kind, window_id,
        attempt, epoch
    )
) WITH CLUSTERING ORDER BY (
    fire_ts ASC, fire_seq ASC, business_key ASC, trigger_kind ASC,
    window_id ASC, attempt DESC, epoch DESC
);
```

- There is no `window_recovery_bases`, no `window_kg_lease`, and no per-key
  pin table. The WO cut lives in memory, restored from the blob; the reader's
  cut lives in `window_kg_meta`.
- `window_kg_buckets`: `commit_events` upserts `business_key` unversioned.
  `maintain` scans `bucket_start < floor_bucket` per owned group, then deletes
  that key's raw partition for the bucket, every tile partition for the same
  key+bucket, and the index row. `floor_bucket` is the first bucket still
  overlapping the floor; only whole buckets below it are dropped.
- `window_raw`: **one CQL row per event**, clustered by event time. Do not
  pack a batch into one blob.
- `window_tiles` / `window_key_states`: versioned, and they accumulate one row
  per write until GC collapses them — see *Per-cell version retention*, which
  is not optional.
- `window_due`: immutable due work, versioned like raw, locality
  `(namespace, bucket_start, kg_shard)` where
  `kg_shard = key_group * SHARD_COUNT / max_parallelism` and `SHARD_COUNT` is
  a store constant (e.g. 32 or 64), not `p`. WRO does not read it.

---

## WO write

For one key (streaming and request ingest are the same path):

1. Derive `key_group` from the key hash and the client's `max_p`.
2. Load writer `KeyState` (WO filter).
3. Allocate `epoch = next_epoch[g]++`.
4. UNLOGGED BATCH per touched partition — changed raw, tiles, `KeyState`, due
   rows, all at `(my_attempt, epoch)`; upsert `window_kg_buckets`
   unversioned. Issue the per-partition batches concurrently. Never a logged
   batch, never across partitions.
5. Register `epoch` as in flight; retire on ack. A timeout keeps it in flight
   and blocks the cut.

`store_key_state` follows the same protocol but writes no raw, tile, due or
index rows.

---

## WO reads

```text
visible(row) ⇔ row.attempt == my_attempt OR cp_cut[g].allows(row.version)
```

**`key_state`** is the hot path, once per key per batch. Clustering is
`(attempt DESC, epoch DESC)` and per-cell retention bounds the partition, so
**one** bounded read resolves it:

```cql
SELECT attempt, epoch, key_state FROM window_key_states
 WHERE namespace = ? AND key_group = ? AND business_key = ?
 LIMIT 8
```

Keep the first row that passes the filter. Do **not** walk one query per
candidate attempt: that is `1 + |history|` round trips on an idle key whose
last writer sits at the bottom of a long history. Only if every returned row
is filtered out (a zombie rewrote the key many times and GC has not swept yet)
double the `LIMIT` to a cap, then fall back to one restricted query per
history entry, newest attempt first. Never `SELECT *` unbounded.

**Raw and tiles.** Slice time in CQL, filter versions client-side, keep the
first (newest) visible row per cell, then deduplicate raw by `Cursor`. Read
amplification is proportional to retained versions per cell, which is why
per-cell retention is correctness-adjacent rather than a background nicety.

**Due work** is one CQL hop with the same filter. Mid-shard seek uses the last
**raw** clustering tuple, even if filtered out:

```text
WHERE namespace = ? AND bucket_start = ? AND kg_shard = ?
  AND (fire_ts, fire_seq, business_key, trigger_kind, window_id, attempt, epoch)
      > (?, ?, ?, ?, ?, ?, ?)
  AND fire_ts <= ?
LIMIT ?
```

The client drops key groups outside the bound range. Do not use `OFFSET`,
native paging state, a second `fire_ts >=` query, a seek on `fire_ts` alone,
or an unpaged read of the whole range.

---

## WRO reads

1. Hash the key → `key_group`. Read the `window_kg_meta` row.
   Absent or empty cut ⇒ empty.
2. Apply the coverage guard against `retention_floor`; refuse if uncovered.
3. Build exact raw/tile plans, load from the same data partitions (time slice
   in CQL), filter client-side per `ReadOptions`.
4. Collapse per cell by greatest `Version`; merge, order, deduplicate,
   rebuild.
5. Return the data with `committed_wm` and `checkpoint_id`.

Two hops. WRO reads no due work, no WO cache, no master, no checkpoint
metadata.

---

## Due-work index (decision)

Today only `WindowTriggerKind::RowEmit` exists (`WindowEnd` bails in
`eval/advance.rs`) and WO records one due row per accepted raw row, so
`window_due` is a versioned 1:1 shadow of `window_raw`. The costs are real:
double write volume, an extra round trip per key per batch, a write hotspot on
a few dozen partitions per bucket, tombstone-heavy paging scans.

**v1 keeps it as specified.** It is correct under the version filter, has no
superseded versions to collapse, and is the contract
[#296](https://github.com/volga-project/volga/pull/296) already builds on.

**Follow-up, tracked separately:** replace it with a skinny unversioned "this
key has due work in this bucket" index — the same shape as
`window_kg_buckets`, `O(keys x buckets)` instead of `O(events)`. Due cursors
are recoverable from the key's raw rows and `evaluation.through`, which
advance already loads. This changes the `load_triggers` contract and the
operator's paging, so it is not folded into the protocol change.

---

## WO cache

Foyer is optional and WO-only, keyed:

```text
meta: PartitionKey -> KeyState
data: (PartitionKey, family, bucket) -> materialized writer-view data
due:  (namespace, bucket, kg_shard) -> immutable due entries
```

Cleared before each execution attempt. WRO bypasses it entirely.

---

## Scylla consistency

Single DC, RF=3 minimum. `LOCAL_QUORUM` for data reads and writes
(`W+R > RF`). `window_kg_meta` writes are LWT with `LOCAL_SERIAL` + learn
`LOCAL_QUORUM`; its reads are `LOCAL_QUORUM`.

- **WO write:** non-LWT data at `LOCAL_QUORUM`. Streaming never uses LWT.
  Request uses one LWT per group at open and one per group per completed
  checkpoint.
- **WO read (cache miss):** version filter on versioned tables at
  `LOCAL_QUORUM`.
- **WRO read:** metadata + data at `LOCAL_QUORUM`.

Not v1: `USING TIMESTAMP` instead of the metadata CAS; ingest CAS
(SlateDB-style write fence).

---

## Retention and GC

**Eligibility.** `retention_floor = committed_wm - max_window_length -
lateness`, computed at checkpoint completion and published. **Physical
deletion is gated on the published floor, never on the live watermark.** That
single rule gives both the reader's coverage guarantee (anything above the
published floor is present) and restart safety (the floor is derived from
durable state, so it never moves backwards across a failover).

Retention is asynchronous and lags the floor; that is fine in both directions
— the floor is what readers are promised, and GC only ever has less deleted
than the floor allows.

**Executor:** `StateRegistry::run_maintenance_once` — parallel loop per
`OperatorKind`, then per-task `OperatorStore::maintain(ns, state)`, limited to
the task's owned key-group range.

### Per-cell version retention (required)

Tiles are rewritten on every ingest batch that touches them and `key_state` on
every batch and every advance, so without this rule `window_tiles` and
`window_key_states` partitions grow with write count — on the ingest hot read
path. For each cell keep exactly:

1. the newest version with `attempt == cur_attempt` (possibly uncommitted);
2. the newest version allowed by the published `cut`;
3. the newest version allowed by `prev_cut` (covers requests in flight across
   a publish).

Drop everything else, including rows of older attempts **above** their cut
entry, which is how zombie writes are collected. Bound: three versions per
cell.

Slot 3 is not optional either. A tile rewritten between two checkpoints has
one version allowed by `cut` and an older one allowed by `prev_cut`; without
the slot, a request that pinned the earlier cut finds the partition non-empty
and every row filtered out, and returns nothing for that tile. That is a
silent undercount on the normal path, and the reader cannot detect it: "no
version at my cut because it was collected" and "no version at my cut because
the cell was first written after my cut" are indistinguishable from the data,
and the second is legitimate.

`prev_cut` is a published column rather than owner RAM so that a successor's
GC honours grace opened by its predecessor — a restart is exactly when
readers hold the oldest cuts, and a successor has published nothing of its
own. It is written from the row it replaces, never from the writer's memory
(see *Writing it*). Because the cut is monotonic, `prev_cut ⊆ cut` always, so
one slot suffices and no wall-clock comparison appears anywhere.

One slot covers readers at most **one** generation stale. That is the whole
reason `wro_request_timeout < checkpoint_interval` is enforced and the
metadata row is not cached in v1.

### Rest of `maintain`

- drop consumed due rows (`fire_at.ts <= published floor`) with a `fire_ts`
  clustering-range delete on shards overlapping the owned range;
- scan `window_kg_buckets` for `bucket_start < floor_bucket` per owned group
  and delete the matching raw/tile partitions and index row;
- drop a `window_kg_meta` row only when the whole group is gone;
- do **not** GC with a group-wide version high-water mark.

**In-flight requests.** A request pins its cut at start. Deleting a version
named by a live or previous cut is forbidden by the rule above, and whole
buckets only go below `floor_bucket`, which a covered request never reads.
No pin refcount table.

TTL/TWCS may expire physical SSTables only when consistent with the published
floor. InMem applies the same logical rules immediately inside `maintain`.

---

## Cost summary

| Operation | Scylla round trips | LWT |
|---|---|---|
| Ingest, one key-batch | 1 bounded `key_state` read + 1 read per (granularity, bucket) tile set; writes 1 per touched partition, concurrent | none |
| WO advance page | 1 due-work read + the per-key reads it already needed | none |
| WRO lookup, `Committed` | 1 metadata read + planned raw/tile reads | none |
| WRO lookup, `Fresh` | same + 1 bounded raw slice over the head | none |
| Checkpoint barrier | 0 | none |
| Checkpoint completion | 1 per owned group | 1 per owned group |
| Restore, streaming | 0 | none |
| Restore, request | `O(max_p / p)` parallel metadata CAS (attempt + heal) | 1 per owned group |

Storage per cell after GC: at most three versions. Control-plane blob:
`O(groups x attempts)` × 16 bytes.

---

## Accepted divergences and known limits

1. **Read-your-writes across the WO/WRO split is checkpoint-granular in
   `Committed` mode**, and head-scoped in `Fresh`. For event-time correctness
   this is rarely the binding constraint — watermark lag usually dominates
   publication lag — but it is a real difference from a single-process store
   and should be stated in user-facing docs.
2. **Late arrivals are invisible to `Fresh` until the next completed
   checkpoint.** See *Fresh*.
3. **Late-drop race during replay.** A successor restores its frontier from
   the checkpoint, so it accepts everything its predecessor accepted, unless
   replay reordering advances the frontier past an event the predecessor had
   taken. Pre-existing engine nondeterminism, bounded by lateness; not
   introduced here.
4. **Non-deterministic raw cursors.** `seq_no` comes from `KeyState.next_seq`
   at ingest, so a replay with different arrival order gives the same logical
   event a different `Cursor`. This is why zombie rows must be excluded by the
   cut rather than merely overwritten — LWW alone would leave duplicate raw
   rows. A stable source-assigned event identity would remove the need and is
   worth having for other reasons; future work.
5. **Cut history is not retired in v1.** An entry can only be dropped when no
   row of that attempt remains, and a zombie row with a far-future `event_ts`
   is retained by design. Entries are 16 bytes; 1000 attempts across 128
   groups is ~2 MB of control plane plus a binary search per row. Cap the list
   and fail loudly if the cap is hit; forced version compaction is future
   work. The cap bounds the checkpoint blob, the published cut payload, the
   per-row filter cost and the pathological `key_state` fallback. It does not
   bound steady-state query counts — the bounded `LIMIT` read does that.
6. **Head-of-line blocking on the published cut.** Cuts are per group and the
   prefix is a low-water mark, so one slow or timed-out write holds back that
   group's whole cut, including acked writes to other keys at a higher epoch.
   Bounded by write timeout; an unresolvable write fails the task. Skipping
   the hole is not an option — see *Acked prefix*.
7. **Streaming write zombies are unfenced.** No ownership row, no ingest LWT.
   The cut fences every **reader** permanently, but a dead worker can still
   write unlogged rows (collected by per-cell retention) and still emit
   downstream until master kill. Output fencing is the job attempt on the data
   plane.
8. **Two master prerequisites.** Durable attempt allocation and a
   checkpoint-completion notification. The backend refuses to open without
   them.

---

## Explicitly not v1

- A separate publish path, ownership lease, per-key pins, per-key epoch maps,
  copy-on-restore, or any cut clamp.
- Ingest LWT, periodic owner suicide, `USING TIMESTAMP` LWW.
- Cut history retirement, skinny due index, stable source event identity.
- Zero-lag visibility for late events in `Fresh`.
- Caching the `window_kg_meta` row on the WRO side.

## Future next steps

`RestorePlanner` identity mapping on master must grow the range intersection
described above; Scylla's `maintain` implementation remains TODO.

**WRO metadata cache.** Removes one hop from every request. Needs either an
explicit `TTL + max request duration < checkpoint interval` bound or a second
retention slot; see *Metadata read*. Measure the hop first — it is a
single-row read from a `max_parallelism`-row table, so it may not be worth a
tuning knob.

**Namespaced cache quota.** Give each state consumer its own memory and
local-disk quota; `StateResourceTracker` is the scaffold.

**Mem pressure / backpressure.** If eviction cannot keep a consumer within
quota, backpressure upstream. Remote state must not be a bottomless overflow
sink.

**CDC and late events.** Append-only today. Future CDC must identify logical
rows independently of arrival cursor and represent inserts/updates/deletes
explicitly; readers would continue to see one coherent version.
