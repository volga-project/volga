# Window store with Scylla design

`WindowOperatorStore` serves the sole WO owner of a key group.
`WindowRequestStore` serves coherent WRO point lookups. Physical layout, MVCC,
serialization, and caching stay inside the backend.

Versioned data tables are shared by both roles. Every row carries a totally
ordered `Version = (generation, seq)`. Visibility is a **per-key-group cut
history**: a short sorted list of `(generation, seq)` prefixes. Streaming WO
filters on its restored cut history; request mode publishes a second cut
history on `window_pins`.

There is no per-key `window_head`, no `window_recovery_bases`, no
`window_kg_lease`, no per-key serving pin, no `prev_E`, and no `high_E`.

This document supersedes the protocols in
[#157](https://github.com/volga-project/volga/pull/157) and
[#298](https://github.com/volga-project/volga/pull/298). For operator
semantics, evaluation flow, and module structure, see the
[window operator README](README.md).

The current data contract is append-only. CDC and late-event correction will
require explicit mutation semantics — future work.

---

## What changed and why

[#298](https://github.com/volga-project/volga/pull/298) stamped rows with an
**opaque** `attempt` token plus a per-group epoch `E`, and made visibility
`attempt == me OR (attempt == cp.attempt AND E <= cp_E)`. Because `attempt`
was matched by **equality**, the visible set was a union of per-attempt
prefixes rather than a prefix of one order. Two consequences, both found in
review of that PR, are silent wrong answers:

1. **History loss after the second failover.** `A -> B -> checkpoint -> C`
   leaves `C` with overlay `C | (B AND seq <= cp_B)`. Every cell `A` wrote
   and `B` did not happen to rewrite is invisible, and source replay only
   covers post-checkpoint input. The unit of loss is a **cell**, not an idle
   key: active keys lose their retained raw history too. Coverage planning
   treats a missing tile as *an interval with no rows*
   ([README](README.md) — *Coverage planning*), so the result is an
   undercount, not an error.
2. **Republish truncates a key's history.** With per-key serving pins and
   `attempt == serving_attempt`, the first publish that touches a key hid
   every row an older attempt wrote for it. `prev_E` in #157 existed to
   hole-fill exactly this; per-key pins only removed the need for *idle*
   keys.

Both collapse into one root cause. The fix is to make the version **totally
ordered** and the cut a **history** instead of a scalar:

- `generation` is a durably monotonic, never-reused integer, so
  `(generation, seq)` is comparable across attempts and across rescales.
- The cut is a sorted list of one `(generation, seq)` prefix per generation
  that ever checkpointed or published. Older generations keep their prefix
  forever, so pre-crash history never disappears; post-checkpoint zombie
  writes stay above their generation's prefix and are excluded permanently.

That removes the need for `prev_E`, per-key pins, copy-on-restore, and
per-key catch-up freezes. It also lets the serving cut go back to being
**per key group**, which collapses publish to a single-row LWT and deletes
the unbounded dirty-key set.

Retained from #298 without change: per-group cuts (not one task-wide epoch),
rescale as slice remap with no data copy, the three-clock restore rule,
publish off the checkpoint barrier, the `publish_ok` gate and its restore
table, the skinny `window_kg_buckets` GC index with `floor_bucket`, the
refusal to cluster `business_key` under a key group on data tables,
`LOCAL_QUORUM` data with LWT only for ownership.

New in this document beyond the protocol change:

- **Per-cell version retention in GC.** #298's rule ("drop rows in neither
  overlay nor serving pin") could never reclaim a superseded version, because
  `attempt == me` matched every epoch of the current writer. Tiles are
  rewritten on every ingest batch that touches them and `key_state` on every
  batch, so `window_tiles` and `window_key_states` partitions grew with write
  count on the hot read path.
- **Version-level, not CQL-level, commit atomicity.** One logical commit spans
  four to six partitions and cross-partition batches are forbidden, so
  `commit_events` is not atomic in CQL. The acked-prefix rule is what makes
  torn commits invisible.
- **Newest-first version clustering** with bounded `LIMIT` scans, available
  only because versions are totally ordered.
- Lexicographic `(generation, seq)` cell collision resolution, replacing
  #298's `max(E)` then `max(attempt)`, which compared a non-comparable epoch
  first and took `max` of an opaque token.
- A durable generation allocator as an explicit master prerequisite.

---

## Modes

| | Streaming (WO only) | Request (WO + WRO) |
|---|---|---|
| Data, due index, `window_kg_buckets` | yes | yes |
| Versions, cut history, checkpoint cuts | yes | yes |
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

`KeyGroupRange`, `key_group_of`, and `subtask_of` are landed in
`src/common/key_group.rs`. Routing:

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
  `seq_no` is allocated from `KeyState.next_seq` at ingest, so it is **not**
  stable across replays that reorder arrivals. See *Accepted divergences*.
- `RawRun` and `TileRun` are half-open.
- `watermark_frontier` is the **task-level** late-data boundary, stored in and
  restored from the operator checkpoint. Logical retention is derived from it,
  the largest window, and configured lateness.
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
Generation      = durably monotonic execution generation (see below)
WriterId        = this task execution (request `owner` only)
```

The client derives `key_group` from `Key.hash` and bound `max_p`. Per-key
calls must land in the bound range.

WRO reuses the WO owner namespace and addresses rows by `PartitionKey`.
Request routing still sends a lookup to the WRO task that owns that key
(locality); the namespace does not encode `task_index`.

`OperatorStore` is the generic maintenance port on the **shared** physical
backend. Window eligibility is read from `OperatorTaskState` (watermark →
`retention_cutoff`, plus the same key-group range the WO client was bound to).

## Store traits

Landed on master (`WindowOperatorStore` / `WindowRequestStore`). Required
behavior that this protocol relies on:

- Missing partitions and empty runs return empty/default results.
- `commit_events` publishes raw rows, replacement tiles, key state, and due
  work **for one key** at a single `Version`. It is atomic **at the version
  level, not at the CQL level** — see *Commit atomicity*. Retries at the same
  version are idempotent.
- The due-work read is one hop and uses the same filter as WO data reads.
  WRO does not read due work.
- `checkpoint` completes pending writes and returns a backend snapshot of the
  client's bound range only. It performs **no** Scylla round trips beyond
  draining in-flight writes.
- `restore` starts the cut history from the supplied checkpoint / planner
  slices and must not clobber keys outside the bound range.
- WRO: each `load_window_data` is one coherent published snapshot.

`InMemWindowStore` is the reference physical backend. One lock provides the
WRO snapshot boundary. Scylla implements the same client contract with MVCC.

## Runtime flows

WO ingest:

1. Load key state (WO filter), drop rows at or behind the task watermark,
   assign sequence IDs.
2. Load and update affected tiles.
3. For emitting WO, record due work for each accepted row.
4. `commit_events` (unlogged) at one `Version`. Request-mode may then
   `publish()` if `publish_ok`.

WO advance: stream due-work pages for
`(watermark_frontier, incoming_watermark]`, evaluate, `store_key_state`, then
advance the task watermark. Physical prune is the worker cleaner via
`maintain`.

WRO lookup: read `window_pins` once, `load_window_data` once, evaluate.
Read-only.

---

## Identity and clocks

```text
business_key   window key
key_group      hash % max_parallelism          // stable under rescale
subtask        key_group * p / max_parallelism
generation     durably monotonic, never reused, job-wide
seq            per key_group write-batch counter, local, starts at 0
Version        (generation, seq), lexicographic total order
watermark      task-global, non-decreasing
```

`seq` is a **write-batch counter**, not event time. It only has to increase
within one `(generation, key_group)`; a plain local counter from zero
satisfies that. It does **not** need to be seeded from any checkpoint or pin
value, because `generation` is the high-order component of the order. #157's
`E = max(cp_E, serving_E, prev_E)` and #298's `next_E[g] = cp_E_g` seeding
rules are both deleted.

`generation` is job-wide: every WO task of one execution shares it.
`WriterId` (`generation || vertex`) is stored only on request
`window_pins.owner`. Streaming does not use it.

Watermarks are **non-decreasing**, not "strictly greater at every publish or
barrier". Publish and checkpoint are not watermark events; many of them can
share one `wm` value.

### Generation allocation (master prerequisite)

`generation` must be monotonic and never reused **across master restarts**,
because Scylla data outlives the master process. Neither of the two obvious
sources is sufficient on its own:

- `MasterLifecycle::attempt_id` increments per recovery but is reset to `0`
  on pipeline start (`src/runtime/master/lifecycle.rs`). A new incarnation at
  generation `0` would sit below every existing cut, so its writes would be
  filtered out and its cells lost to LWW.
- `generation = max(restored cut generations) + 1` repeats: attempts 2 and 3
  commonly restore from the **same** checkpoint, so both would compute the
  same generation and share a version space with each other's zombie.

Required: the master allocates the generation from **durable** state
(`max(observed) + 1`), persists it **before** configuring workers, and passes
it to tasks alongside restore data. Assertions:

- `new_generation > every generation present in any restored cut history`.
- A generation is never handed to two execution attempts.

This is a small, load-bearing master change. Until it lands, the Scylla
backend must refuse to open.

---

## Protocol (normative)

### Version stamping

- raw / tiles / `key_state` / due work cluster `(generation, seq)`.
- `seq` is allocated locally per `key_group`. **No LWT** to mint it.
- Ingest: same-partition **UNLOGGED BATCH** only.

```text
seq = next_seq[g]++          // next_seq[g] starts at 0 on first touch
stamp (my_generation, seq)
```

Increment **during catch-up**. Do not wait for `publish_ok`. Do not seed from
a checkpoint cut or from a pin.

### Cut history

A cut history is a short list, sorted by generation, with at most one entry
per generation:

```rust
pub type Generation = u64;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Version {
    pub generation: Generation,
    pub seq: u64,
}

/// One per key group. Sorted ascending by generation, generations unique.
#[derive(Clone)]
pub struct CutHistory {
    entries: Vec<Version>,
}

impl CutHistory {
    /// Is `v` inside the cut?
    pub fn allows(&self, v: Version) -> bool {
        match self.entries.binary_search_by_key(&v.generation, |e| e.generation) {
            Ok(i) => v.seq <= self.entries[i].seq,
            Err(_) => false,
        }
    }

    /// Add or raise **only the caller's own** entry, and return a new
    /// history. Never raises, lowers, or removes another generation's entry:
    /// clamping is what the *caller's choice of base* does, not this
    /// function. See *Clamp invariant*.
    ///
    /// Takes `&self` on purpose. Publish computes its payload from `cp_cut`,
    /// which is also the live WO read filter; an `&mut self` signature makes
    /// it trivial to turn the restored checkpoint cut into the serving
    /// prefix by accident.
    pub fn advance(&self, me: Generation, acked_prefix: Option<u64>) -> CutHistory {
        let mut next = self.clone();
        // No acked write for this group in this generation: contribute
        // nothing. Never insert `(me, 0)` — `seq` is 0-based, so a zero
        // entry is indistinguishable from "seq 0 is committed".
        let Some(seq) = acked_prefix else { return next };
        match next.entries.binary_search_by_key(&me, |e| e.generation) {
            Ok(i) => {
                assert!(
                    next.entries[i].seq <= seq,
                    "own prefix must not go backwards",
                );
                next.entries[i].seq = seq;
            }
            Err(i) => {
                assert!(
                    next.entries.get(i).map_or(true, |e| e.generation > me),
                    "generation must dominate every inherited entry",
                );
                next.entries.insert(i, Version { generation: me, seq });
            }
        }
        next
    }

    /// True if `self` allows a version that `other` does not. Publish uses
    /// it to decide whether it is removing coverage (*GC grace*).
    pub fn outlives(&self, other: &CutHistory) -> bool {
        self.entries.iter().any(|e| {
            match other.entries.binary_search_by_key(&e.generation, |o| o.generation) {
                Ok(i) => other.entries[i].seq < e.seq,
                Err(_) => true,
            }
        })
    }
}
```

A row whose generation has **no** entry is outside the cut. That is the
correct answer for a generation that wrote but never checkpointed: all of its
writes are post-checkpoint and will be replayed.

Size is `O(generations)` per group, 16 bytes per entry. v1 does **not**
retire entries; see *Accepted divergences*.

### Acked prefix

A cut may only name a `seq` whose writes are all durable:

```text
cut_top[g] : Option<u64>
           = max seq such that EVERY write at (my_generation, s <= seq)
             for group g has been ACKED
           = None if no write in this generation has acked yet
```

Track it as a **low-water mark** over in-flight `seq` values per group. This
is the single rule that makes cross-partition commits safe (*Commit
atomicity*) and torn commits invisible. It applies to both the checkpoint cut
and the serving cut, and `None` contributes no history entry.

`seq` is allocated by an **atomic** per-group counter, because ingest runs up
to `ingest_key_concurrency` keys concurrently and they can share a group.
Concurrent in-flight `seq` values in one group are allowed; the prefix is
what makes them safe.

The prefix is a prefix, **never a skip**. If `seq=5` times out and `seq=6`
acks, `cut_top` is `4`: key Y's committed data at `6` is durable and visible
to the writer, but no cut may name it until `5` resolves. Do not "skip the
hole"; do not allocate around it.

A write that times out has an unknown outcome and therefore blocks the
prefix. Retry it at the **same** `Version` until it is acked, or fail the
task. (#298's "a new epoch starts only after the previous write outcome is
known" is unsatisfiable on timeout; this is the replacement rule.) The
resulting head-of-line blocking of that group's checkpoint and publish is
accepted — see *Accepted divergences*.

### WO visibility

```text
visible(row) ⇔
    row.generation == my_generation          -- read your own writes
    OR cp_cut[g].allows(row.version)
```

`cp_cut[g]` is the cut history restored for group `g`. Until the first
restore of a job it is empty, so visibility is `my_generation` only. The
due-work read uses the same filter.

Zombie rows are an older generation above that generation's entry →
invisible, **permanently**, including to every later generation. That is the
v1 **streaming fence** for reads. Streaming has no `owner` row; periodic
"am I still owner?" suicide is not v1 and master kill reaps the process.

Unlike #298, no keys and no cells disappear after failover: every generation
that ever checkpointed keeps its prefix in the history.

### Commit atomicity

One logical `commit_events` touches several partitions — `window_raw` (per
bucket), `window_tiles` (per granularity × bucket), `window_key_states`,
`window_kg_buckets`, and the due index. Cross-partition batches are
forbidden, so there is **no** CQL-level atomicity and a crash can tear a
commit.

The version is what restores atomicity:

- All writes of one commit carry the **same** `Version`.
- A torn commit leaves a partial set at `(generation, seq)`. Because the
  acked-prefix rule never advances a cut past an unacked write, a torn
  commit always sits **above** every cut. No **cut-filtered** reader — any
  successor, and WRO — ever observes it.

WO reads of its own generation are **not** capped by the prefix, so the
writer *can* see its own torn commit. Requiring it to be resolved before the
next checkpoint or publish is not enough: the advance/emit path reads before
either of those. The rule is **per key**:

> While a commit for key `K` is in flight, the writer issues no read of `K`
> and no second commit for `K`. `commit_events` either returns with every
> write acked, or the task fails.

Cross-key concurrency inside a group stays legal, which is the point. A torn
commit then blocks exactly two things: reads of its own key (for the writer)
and the group's acked prefix (for cuts).

Do **not** instead cap the writer at `seq <= cut_top[g]`. With key X's
`seq=5` unresolved and key Y's `seq=6` acked, the group prefix is `4`, so
Y's own committed state would become invisible to its writer, which would
then re-ingest and re-apply it. Read-your-writes for the current generation
is uncapped on purpose.

State all of this in the trait docs. "Atomic" without the version qualifier
will be built on and is wrong.

### Request ownership and serving cut

```sql
-- Request mode only. Streaming WO does not read or write this table.
CREATE TABLE window_pins (
    namespace        blob,
    key_group        int,
    owner            blob,     -- WriterId allowed to publish
    serving_wm       bigint,   -- task wm at the last successful publish
    serving_cut      blob,     -- encoded CutHistory for this key group
    prev_serving_cut blob,     -- cut replaced by the last coverage-removing
                               -- publish; GC grace only
    prev_expires_at  bigint,   -- wall-clock ms after which prev may be
                               -- ignored
    PRIMARY KEY ((namespace, key_group))
);
```

**One row per key group, no clustering column.** `max_parallelism` rows in
the whole table. #298 clustered `business_key` under `((namespace,
key_group))`, which is the same unbounded-group partition the design forbids
on data tables, and which every request had to point-get.

There are now two cut histories of the same shape, moving independently:

| Cut history | Written | Read by |
|---|---|---|
| `cp_cut[g]` | checkpoint barrier, into the control-plane blob | WO after restore |
| `serving_cut` | publish cadence, into `window_pins` | WRO |

Do not collapse them.

### Steal (request, once per owned group at restore)

`previous` is **not** in the checkpoint blob. Read it:

```text
row = SELECT owner FROM window_pins WHERE namespace = ? AND key_group = ?
if row is absent:  INSERT ... IF NOT EXISTS
else:              UPDATE SET owner = me IF owner = row.owner
on not-applied:    re-read once; if owner == me, done; otherwise fail the task
on LWT timeout:    fail the task (unknown)
```

Do not touch `serving_wm` or `serving_cut`. Run the `O(max_p / p)` steals in
**parallel**. WRO does not steal; it reads the current `serving_cut` even
while `owner` is still the previous writer (grey window).

### `publish_ok` (request, OnCommit / periodic)

Do **not** publish on the checkpoint barrier. Pins move only on the writer
cadence.

```text
publish_ok ⇔
    current_wm is set
    AND current_wm >= serving_wm      -- caught the last served frontier
    AND current_wm > cp_wm            -- not the restored copy of the frontier
    AND cut_top[g] > published_top[g] -- something new is durable
```

`current_wm` is the live task `watermark_frontier` (seeded from the
checkpoint blob on restore, then advanced only on upstream `wm > frontier`).
`cp_wm` is the restored frontier for that group (the slice's
`watermark_frontier`; task min is the conservative default).
`serving_wm` unset (never published) → skip `>= serving_wm`.

After restore, `current_wm == cp_wm`, so `> cp_wm` is false until a **new**
upstream watermark arrives. That is the prefix guard.

| After restore | Example | What waits |
|---|---|---|
| Serve **ahead** of CP | `serving_wm=1000`, `cp_wm=800`, `current=800` | `>= serving_wm` until 1000 |
| Serve **= CP** | both `1000`, `current=1000` | `> cp_wm` until 1001 |
| Serve **behind** CP | `serving_wm=800`, `cp_wm=1000`, `current=1000` | `> cp_wm` until 1001 |

`>= serving_wm` alone is already true in the equal and behind cases because
restore **copies** `cp_wm` into `current_wm`. `current_wm > serving_wm` alone
opens immediately when the pin lags the blob. Both compares are required.

The last condition replaces #298's `dirty != {}`. It is a scalar compare, not
a set: #298 listed "dirty keys since last publish" under bounded local RAM,
but at high ingest with a periodic cadence that set is millions of keys.

Under #298 this gate was necessary but not sufficient, because the data it
guarded was truncated. With a complete cut history it **is** sufficient: see
*Clamp invariant*.

### Publish LWT (request)

The payload is computed from **`cp_cut[g]`**, not from the live serving cut:

```text
next = cp_cut[g].advance(my_generation, cut_top[g])     -- non-mutating
```

This one line is the clamp. `advance` only ever adds or raises the caller's
own entry, so basing it on the live serving cut would keep
`(A, serving_seq_A)` and merely append `B` — the clamp would never happen and
WRO would serve `A`'s post-checkpoint rows next to `B`'s replay of the same
input, with different `Cursor`s. That is duplicate raw data, i.e. a wrong
answer, and it is the trap #298 fell into from the other direction. Basing it
on `cp_cut[g]` gets all three restore cases right:

| Live serving vs CP | `next` | Effect |
|---|---|---|
| ahead (`A:5000` vs `A:100`) | `{A:100, B:n}` | clamps `A` down |
| behind (`A:80` vs `A:100`) | `{A:100, B:n}` | fills `A` up to the acked blob prefix |
| later publishes of mine | `{A:100, B:n'}` | raises only `B`; `cp_cut` is stable between barriers |

One single-row conditional update per group:

```text
UPDATE window_pins
   SET serving_wm       = current_wm,
       serving_cut      = next,
       -- only when `removes_coverage` (see below):
       prev_serving_cut = <the serving_cut being replaced>,
       prev_expires_at  = now_ms + grace_ms
 WHERE namespace = ? AND key_group = ?
    IF owner = me

removes_coverage = observed_serving_cut.outlives(next)
```

The publisher already holds `observed_serving_cut`: it read the row at steal
and knows every value it has written since. No extra read.

One Paxos round, one row, payload bounded by `O(generations)` × 16 bytes. No
conditional BATCH, no chunking, no partial-publish semantics, no per-key
condition list. #298 needed "statics plus all dirty keys in one Paxos", which
exceeds Scylla's batch thresholds for a large dirty set and then has to be
chunked, which in turn makes a publish non-atomic.

`Not applied` or timeout → **fail this task**. A timeout is *unknown*, the
same as a steal timeout: do **not** retry it as a fresh publish at a later
`cut_top`, because the first attempt may yet apply and a second payload would
race it. Writers always publish on a policy (eager OnCommit and/or periodic),
so a zombie that is still ingesting eventually hits this CAS. That is a
**backup** to master kill, not the primary fence. Grey window **before**
steal: the old owner may still publish — accepted.

**Hops:** async ingest = 1 (data). OnCommit = 2 (data, then pin LWT). No
ingest LWT.

### Clamp invariant

The serving cut is **not append-only**. At restore, `B` inherits `A`'s entry
from the checkpoint blob, which is `(A, cp_seq_A)`. The live serving cut says
`(A, serving_seq_A)` with `serving_seq_A >= cp_seq_A`. `B`'s first publish
therefore **clamps `A`'s entry down** to `cp_seq_A`, and rows `A` wrote in
`(cp_seq_A, serving_seq_A]` stop being served.

That clamp is mandatory: those are `A`'s post-checkpoint writes, which may be
torn or may diverge from `B`'s replay, and `B` replays the same input. It is
produced by basing the publish payload on `cp_cut[g]`, **not** by `advance`
touching another generation — `advance` never does.

A generation's entry can also be **dropped** outright, not just lowered. A
generation that stole and published but died before any checkpoint never
enters any `cp_cut`, so the next publish omits it entirely. That is correct
for the same reason (its writes are all post-checkpoint and get replayed),
and it is why the GC grace test is "lowers **or** drops", not "lowers".

So the published **row set** moves backwards at that instant. What does not
move backwards is **event-time coverage**, because `publish_ok` holds `B`'s
first publish until `current_wm >= serving_wm`, by which point `B` has
reprocessed everything `A` had served. State the invariant as:

> The serving cut is monotonic in event-time coverage, not in row membership.

Write it down. An implementer who reads "the cut history grows" will find the
clamp and file it as a bug.

### WRO filter

Read the `window_pins` row **once** per request; every page uses that cut.
Missing row, or empty `serving_cut`, ⇒ return empty with no data reads.

```text
visible(row) ⇔ serving_cut.allows(row.version)
```

No `my_generation` clause, no per-key pin, no `prev_E`. During failover WRO
stays on the last publish until `B`'s first successful `publish_ok` — for
**every** key, including keys `B` has already rewritten.

WRO does not read the WO cut, due work, Foyer, the master, or checkpoint
metadata. Any WRO task can read any key in the operator namespace; keyed
routing is locality.

v1 reads the pin row per request. It is a single row in a tiny partition.
Caching it is an optional optimization and its TTL **must not exceed** the GC
grace for superseded serving cuts (*Prune and cleanup*), or a stale reader can
point at rows GC has already dropped after a clamp.

### Cell collision

Among visible rows for the same cell — same `Cursor`, same
`(granularity, tile_start)`, or `key_state` — keep the one with the greatest
`Version`, compared **lexicographically**: `generation` first, then `seq`.

#298 specified `max(E)`, then `max(attempt)`. That compares an epoch the same
document calls incomparable across attempts, takes `max` of an opaque token,
and only produced right answers because `next_E` was seeded from `cp_E`.

The filter is **client-side** for raw and tiles (clustering is time-first).
CQL slices time; version comparison is in-process.

### Local RAM (bounded)

| State | Scope |
|---|---|
| `my_generation` | task |
| `cp_cut[g]`, `cp_wm` | restore, once; `O(groups x generations)` |
| `next_seq[g]` (atomic), in-flight seqs, low-water mark | per owned group |
| `serving_wm` / `published_top[g]` | request; scalars |
| `observed_serving_cut[g]` | request; from steal, then own publishes |

No per-business-key epoch map. No dirty-key set. No `HeadClaim`.

`cp_cut[g]` is the live WO read filter **and** the base of every publish
payload. Publish must not mutate it; `advance` returns a new value for
exactly this reason.

---

## Checkpoint blob

Each WO task, at an aligned barrier, flushes then reports a **small**
control-plane payload. Cells stay in Scylla. The barrier costs **zero**
Scylla round trips beyond draining in-flight writes.

Because `seq` is per group, the cut cannot be one task-wide value: a slow
group's post-checkpoint writes would fall under a task-wide
`seq <= max_seq`.

```text
WindowStateSnapshot
  watermark_frontier     // cp_wm; already stored today
  backend:
    generation           // this writer
    cuts[g] for g in the client's bound range
```

```rust
pub enum WindowBackendSnapshot {
    InMemory { snapshot: Vec<u8> },
    Versioned {
        generation: Generation,
        /// Parallel to the client's bound `KeyGroupRange`.
        cuts: Vec<CutHistory>,
    },
}

/// Restore/remap instruction. Not stored as a Scylla table.
pub struct VersionedSlice {
    pub range: KeyGroupRange,
    pub generation: Generation,
    /// One per group in `range`.
    pub cuts: Vec<CutHistory>,
    pub cp_wm: i64,
}
```

`Versioned` does not store the range: the payload is already keyed by task on
the master, and the client has a bound assignment. On rescale the **planner**
produces a vector of `VersionedSlice` as restore input, not a second checkpoint
format.

`generation` in the blob is the writing generation. Restore asserts
`new_generation >` it, and `>` every generation inside `cuts`.

**Groups this writer never touched need no special case.** The history for
such a group simply has no entry for `generation`, and the inherited entries
are still there. #298 had to specify "persist the inherited slice cut, not
0"; here inheritance is structural.

Do **not** publish pins at the barrier. Sources checkpoint offsets
separately. The assigner (`max_event_time_seen`) is **not** in the source
checkpoint today; post-restore watermarks are generated from **new** records.
Restoring the WO frontier keeps the late-data line until those watermarks
exceed it.

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
| Slice `cp_wm` / `cuts[]` | Source task blob |
| Task `watermark_frontier` | `min(slice cp_wm)` |
| `serving_wm` / `serving_cut` | Unchanged on `window_pins` |

**Why min:** advancing to `max` would skip fires for keys from a slower
parent. Min can accept a bit of late data on keys from the faster parent;
per-key last-fired in `KeyState` still blocks duplicate emits. Task wm is
global, so this is the rescale tradeoff.

**Planner:** decode each source blob; intersect ranges with each target
task's owned groups → slices.

```text
scale-out, new task1 owns [2,4):
  [{ range:[2,4), generation:gA, cuts: gA's [2,4), cp_wm: wmA }]

scale-in, new task0 owns [0,4):
  [
    { range:[0,2), generation:gA, cuts: old0, cp_wm: wm0 },
    { range:[2,4), generation:gC, cuts: old1, cp_wm: wm1 },
  ]
```

Same assignment = one slice = the old blob.

Example, `max_p = 128`, rescale `p=3 -> p=2`:

```text
old p=3:  task 0 [0, 43)   task 1 [43, 86)   task 2 [86, 128)
new p=2:  task 0 [0, 64)   task 1 [64, 128)

new task 0 <- [0, 43) @ old0  +  [43, 64) @ old1
new task 1 <- [64, 86) @ old1  +  [86, 128) @ old2
```

Each inherited slice keeps its source cut history. Histories are **per
group**, so slices never need merging: a group appears in exactly one source
blob. Because generations are globally ordered, histories from different
source tasks are directly comparable and a single new generation dominates
all of them — this is what #298's independent per-attempt epoch clocks could
not do.

**Target `restore`:**

1. Take the master-allocated `generation`. Assert it exceeds every inherited
   generation.
2. `cp_cut[g]`, slice `cp_wm` from the slice that contains `g`.
3. Task frontier = `min(slice cp_wm)`.
4. `next_seq[g] = 0`.
5. Request: steal `owner` per owned group, in parallel. Serving cut untouched.
6. Replay sources from this barrier. **Do not copy** cells.

### Bootstrap (first start, no restore)

```text
generation     = first durable generation (not necessarily 0)
cp_cut[g]      = empty  -> WO visibility is my_generation only
cp_wm          = unset
next_seq[g]    = 0
serving_wm     = unset
serving_cut    = empty  -> WRO returns empty for every key until first publish
publish_ok     = current_wm is set AND cut_top[g] > published_top[g]
                 (skip both watermark compares)
Request: window_pins row is absent -> steal is INSERT ... IF NOT EXISTS
```

---

## Failover picture

```text
              CP@800,seq=100   publish@1000,seq=5000   crash    B restores
WO cut        {A:100}          + A's later writes               {A:100}, B live
WRO           {A:<last pub>}   {A:5000}                         {A:5000} until publish_ok
B ingest      —                —                                B@0.. immediately
B publish     —                —                                after publish_ok:
                                                                {A:100, B:n}
```

At `B`'s first publish, `A`'s serving entry is clamped `5000 -> 100` and
`A`'s rows in `(100, 5000]` stop being served; `B` has replayed that input
and `current_wm >= serving_wm` holds, so coverage does not regress.

`A`'s rows at or below `seq=100` stay visible to `B`, to `B`'s successor, and
to WRO, forever. That is the whole point of the history.

Streaming: no steal, no publish. Cut history + source replay only.

---

## Tables

`generation` is **not** in any data partition key. It is clustering, so the
filter is a visibility test on one `LOCAL_QUORUM` read. Restore does not copy
rows.

Version clustering is **descending** (`generation DESC, seq DESC`) so the
newest version of a cell sorts first and readers can stop early. This is only
possible because versions are totally ordered.

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
-- Skinny GC index only: no payloads, no versions.
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
    generation   bigint,
    seq          bigint,
    payload      blob,
    PRIMARY KEY (
        (namespace, key_group, business_key, bucket_start),
        event_ts, seq_no, generation, seq
    )
) WITH CLUSTERING ORDER BY (
    event_ts ASC, seq_no ASC, generation DESC, seq DESC
);

CREATE TABLE window_tiles (
    namespace      blob,
    key_group      int,
    business_key   blob,
    granularity_ms bigint,
    bucket_start   bigint,
    tile_start     bigint,
    generation     bigint,
    seq            bigint,
    payload        blob,
    PRIMARY KEY (
        (namespace, key_group, business_key, granularity_ms, bucket_start),
        tile_start, generation, seq
    )
) WITH CLUSTERING ORDER BY (tile_start ASC, generation DESC, seq DESC);

CREATE TABLE window_key_states (
    namespace    blob,
    key_group    int,
    business_key blob,
    generation   bigint,
    seq          bigint,
    key_state    blob,
    PRIMARY KEY ((namespace, key_group, business_key), generation, seq)
) WITH CLUSTERING ORDER BY (generation DESC, seq DESC);

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
    generation   bigint,
    seq          bigint,
    PRIMARY KEY (
        (namespace, bucket_start, kg_shard),
        fire_ts, fire_seq, business_key, trigger_kind, window_id,
        generation, seq
    )
) WITH CLUSTERING ORDER BY (
    fire_ts ASC, fire_seq ASC, business_key ASC, trigger_kind ASC,
    window_id ASC, generation DESC, seq DESC
);
```

- **There is no `window_recovery_bases` table** and no `window_kg_lease`
  singleton. The WO cut history lives in memory, restored from the blob.
  WRO's lives in `window_pins`.
- `window_kg_buckets`: skinny GC index. `commit_events` upserts
  `business_key` (no version, no payload). `maintain` per owned `key_group`:

  ```cql
  SELECT * FROM window_kg_buckets
   WHERE namespace = ? AND key_group = ?
     AND bucket_start < ?   -- floor_bucket, not data_floor
  ```

  For each hit, delete that key's `window_raw` partition for the bucket
  **and** every `window_tiles` partition for the same key+bucket, then the
  index row.
- `window_raw`: **one CQL row per event**, clustered by event time. Do not
  pack a batch into one blob.
- `window_tiles` / `window_key_states`: versioned; the client applies the WO
  or WRO filter. Both accumulate one row per write until GC collapses them —
  see *Per-cell version retention*, which is not optional.
- `window_due`: immutable due work, versioned like raw. Locality is
  `(namespace, bucket_start, kg_shard)`. Cluster by `fire_ts` first.

  ```text
  kg_shard = key_group * SHARD_COUNT / max_parallelism
  ```

  `SHARD_COUNT` is a store constant (e.g. 32 or 64), not `p`. WRO does not
  read due work.

---

## WO write

For one key (streaming and request ingest are the same data path):

1. Derive `key_group` from the key hash and the client's `max_p`.
2. Load writer `KeyState` (WO filter). No pin read on streaming.
3. Allocate `seq = next_seq[g]++`.
4. UNLOGGED BATCH per touched partition: changed raw, tiles, `KeyState`, due
   rows, all at `(my_generation, seq)`. Upsert `window_kg_buckets`
   unversioned.
5. Register `seq` as in-flight; retire it on ack; a timeout keeps it in
   flight and blocks the cut (*Acked prefix*).
6. Request-mode only: if `publish_ok`, pin LWT. Not on the data batch.

Each `(key, bucket)` is one Scylla partition. `commit_events` issues one
**UNLOGGED BATCH** per such partition; issue them concurrently. Do not use
logged BATCH. Do not batch across partitions.

Request ownership CAS failure (steal or publish) stops the WO as fenced.

`store_key_state` follows the same protocol but writes no raw, tile, due, or
bucket-index rows. It advances `seq` like any other write, which is what
makes `publish_ok`'s `cut_top > published_top` fire for advance-only work.

---

## WO reads

```text
visible(row) ⇔ row.generation == my_generation OR cp_cut[g].allows(row.version)
```

Until the first restore, `cp_cut[g]` is empty and visibility is
`my_generation` only.

**`key_state` (hot path, once per key per batch).** Clustering is
`(generation DESC, seq DESC)` and per-cell retention bounds the partition to
four versions, so **one** bounded read resolves it:

```cql
SELECT generation, seq, key_state FROM window_key_states
 WHERE namespace = ? AND key_group = ? AND business_key = ?
 LIMIT 8
```

Keep the first returned row that passes the filter. Do **not** walk one
query per candidate generation: that is `1 + |history|` round trips on an
idle key whose last writer sits at the bottom of a long history, which is
100 queries after 100 failovers.

Fallback, only if every returned row is filtered out — a zombie rewrote this
key many times after the checkpoint and GC has not swept the partition yet:
double the `LIMIT` up to a cap, then issue one restricted query per history
entry, newest generation first:

```cql
SELECT key_state FROM window_key_states
 WHERE namespace = ? AND key_group = ? AND business_key = ?
   AND generation = ? AND seq <= ?
 LIMIT 1
```

Never `SELECT *` unbounded on the partition.

**Raw and tiles.** Slice time in CQL, filter versions client-side, keep the
first (newest) visible row per cell. Logical runs are mapped to time buckets,
loaded, merged, and filtered back to the exact requested ranges. Raw rows are
deduplicated by `Cursor`.

Read amplification is proportional to the number of retained versions per
cell, which is why per-cell version retention is a correctness-adjacent
requirement rather than a background nicety.

The due-work read is one CQL hop with the **same filter**. Mid-shard seek is
the last **raw** clustering row (full tuple, not `fire_ts` alone), even if
filtered out:

```text
WHERE namespace = ? AND bucket_start = ? AND kg_shard = ?
  AND (fire_ts, fire_seq, business_key, trigger_kind, window_id, generation, seq)
      > (?, ?, ?, ?, ?, ?, ?)
  AND fire_ts <= ?
LIMIT ?
```

The client drops `key_group` outside the bound range and applies the WO
filter. Resume is a private `Seek`.

Do not use `OFFSET`, native `PagingState`, a second `fire_ts >=` query, seek
on `fire_ts` alone, or `execute_unpaged` of the whole range.

---

## WRO reads

1. Hash the key → `key_group`. Read `window_pins` for
   `(namespace, key_group)`. Absent row or empty `serving_cut` ⇒ return
   empty. Otherwise **pin once**: that `CutHistory`. Do not reread
   mid-request.
2. Build exact raw/tile plans.
3. Load from the same data partitions (time slice in CQL). Filter
   client-side: `serving_cut.allows(row.version)`.
4. Collapse **per cell**: greatest `Version` lexicographically.
5. Merge, order, deduplicate, rebuild.

If WO is unavailable, WRO continues serving the unchanged cut.

One pin read per request, on one of `max_parallelism` tiny single-row
partitions. Under #298 the same read was a point-get into a partition holding
every key in the group.

---

## Checkpoint

At an aligned barrier:

1. Complete pending writes; resolve or fail any timed-out write.
2. Capture `generation` plus `cuts[g].advance(generation, cut_top[g])` for
   every owned group. `cut_top[g] == None` (no acked write in this generation)
   leaves the inherited entries untouched and adds nothing — do not write a
   `(generation, 0)` entry, which would be indistinguishable from "seq 0 is
   committed".
3. Return `WindowBackendSnapshot::Versioned`. The operator wraps it with
   namespace + `watermark_frontier` in `WindowStateSnapshot`.
4. Continue processing at later `seq` values.

**Do not** publish pins here. Durable due work already represents work above
that watermark, so checkpoint neither drains nor serializes an
operator-local pending-key set.

---

## Recovery

1. Planner produces one `VersionedSlice` per inherited slice (same
   assignment: exactly one).
2. Client takes the master-allocated `generation`, asserts it dominates every
   inherited generation, and loads `cp_cut[g]`. `next_seq[g] = 0`.
3. Source restores its checkpoint offset and replays post-checkpoint input.
4. Resume watermark work by streaming checkpoint-visible due work above the
   restored watermark. Replay advances only writer state.
5. Request: steal `owner` per owned group, in parallel. Leave `serving_wm`
   and `serving_cut` at the previous values. `publish()` stays gated on
   `publish_ok`.
6. Streaming: no steal, no publish.

During recovery, the writer cut and the serving cut differ. Replay advances
MVCC rows; WRO keeps using the previous `serving_cut` for **every** key. A
zombie publish after steal does not apply.

---

## Due-work index (decision)

Today only `WindowTriggerKind::RowEmit` is implemented — `WindowEnd` bails in
`eval/advance.rs` — and WO records exactly **one due row per accepted raw
row** (`state.rs`, ingest). So `window_due` is a 1:1 shadow of `window_raw`,
versioned, in a partition shared by `SHARD_COUNT` shards per bucket, emptied
by clustering-range deletes.

The costs are real: double the write volume, one extra round trip per key per
batch (different partition, cannot batch with the key's data), a write
hotspot on a few dozen partitions per bucket, and tombstone-heavy scans on the
paging read.

**v1 keeps the table as specified above.** It is correct under the version
filter, it has no superseded versions to collapse (due rows are written once),
and it is the contract
[#296](https://github.com/volga-project/volga/pull/296) already builds on.

**Follow-up (tracked separately):** replace it with a skinny, unversioned
"this key has due work in this bucket" index — same shape as
`window_kg_buckets`, `O(keys x buckets)` instead of `O(events)`. The due
cursors are recoverable from the key's raw rows and `evaluation.through`,
which advance already loads, so the per-key work does not increase. This
changes the `load_triggers` contract and the operator's paging, so it is not
folded into the protocol change. Any non-`RowEmit` trigger kind must be
re-evaluated against it before adopting.

---

## WO cache

Foyer is optional and WO-only. It is **not** a HeadClaim / per-key lease
cache. Do not keep unbounded DashMaps of claim state.

```text
meta:      PartitionKey -> KeyState
data:      (PartitionKey, family, bucket) -> materialized writer-view data
due:       (namespace, bucket, kg_shard) -> immutable due entries
```

The cache is cleared before each execution generation. WRO bypasses it.

---

## Scylla consistency

Minimal setup is single DC, RF=3. Use `LOCAL_QUORUM` for data reads and
writes (`W+R > RF`). Request steal/publish is LWT with `LOCAL_SERIAL` +
learn `LOCAL_QUORUM`:

- **WO write:** non-LWT data @ `LOCAL_QUORUM`. Streaming: never LWT.
  Request: steal is LWT at restore per group; publish is a separate
  single-row LWT on the configured cadence, gated by `publish_ok`.
- **WO read (cache miss):** version filter on versioned tables @
  `LOCAL_QUORUM`.
- **WRO read:** pin + data @ `LOCAL_QUORUM`.

**Not v1:** `USING TIMESTAMP` instead of owner CAS; ingest CAS
(SlateDB-style write fence).

---

## Prune and cleanup

**Eligibility (per task):** after watermark advance to `W`,
`data_floor = W - max_window_length - lateness` (lateness default `0`).
Prune is limited to the task's owned key-group range.

**Executor (per worker):** `StateRegistry::run_maintenance_once` — parallel
loop per `OperatorKind`, then per-task `OperatorStore::maintain(ns, state)`.

### Per-cell version retention (required)

#298's rule — "drop rows that are in neither the WO overlay nor a serving
pin" — can never reclaim a superseded version, because `attempt == me`
matched **every** epoch of the current writer. Tiles are rewritten on every
ingest batch that touches them, and `key_state` on every batch and every
advance, so `window_tiles` and `window_key_states` partitions grow with write
count. `load_key_state` is on the ingest hot path and reads that partition.

Replace it. For each cell, keep exactly:

1. the newest version with `generation == my_generation` (the writer's
   current value, possibly still uncommitted);
2. the newest version allowed by `cp_cut[g]`;
3. the newest version allowed by the current `serving_cut` (request mode);
4. the newest version allowed by `prev_serving_cut`, while
   `now_ms < prev_expires_at` (request mode).

Drop everything else, including rows of older generations **above** their
cut entry — which is what collects zombie writes and the rows dropped by a
clamp, usually within one sweep of the partition.

Bound: four versions per cell.

**Rule 4 must be a procedure, not a predicate over history.** "Any
`serving_cut` published within grace" is not implementable: the pin row is
overwritten on every publish, so after a clamp the old cut exists nowhere
that GC can read, and an in-flight WRO that pinned `{A:5000}` still needs
`A`'s `(100, 5000]` rows until its request ends.

A single `prev_serving_cut` slot is sufficient **because it is written only
by a coverage-removing publish** (`removes_coverage` in *Publish LWT*).
Ordinary publishes only raise the publisher's own entry, so their predecessor
is a strict subset of the new cut and retaining it would be a no-op. Writing
`prev` unconditionally would be wrong for the opposite reason: a clamp
followed by two ordinary publishes would push the clamped cut out of the slot
while a reader is still pinned to it. Coverage-removing publishes happen once
per failover, so one slot with an expiry covers every case.

The slot lives in the pin row rather than in the owner's RAM so that a
successor's GC honours a grace window opened by its predecessor.

Grace is the maximum WRO request budget **plus a clock-skew allowance**:
`prev_expires_at` is absolute wall-clock, written by one node and compared by
another. The optional WRO pin-cache TTL must not exceed grace.

### Rest of `maintain`

- drop consumed due rows (`fire_at.ts <= W`) with a `fire_ts` clustering
  range delete on shards overlapping the task's key-group range;
- from `window_kg_buckets`, per owned `key_group`,
  `SELECT ... AND bucket_start < floor_bucket`, then delete matching raw/tile
  partitions and the index row;
- request mode: drop the `window_pins` row for a key group only when the
  whole group is gone; there is one row per group, so #298's per-key pin leak
  (expired keys leaving pin rows that WRO point-gets into empty data) does
  not exist;
- do **not** GC with a group-wide version high-water mark;
- no `window_recovery_bases` walk.

**In-flight WRO:** `load_window_data` pins the serving cut at start. The
remaining race is **event-time** GC versus a request whose window still sits
below `data_floor`. v1: never delete a version named by a live cut, and delay
dropping buckets below `floor_bucket` by at least the grace interval. No pin
refcount table.

TTL/TWCS may expire physical SSTables only when consistent with logical
`data_floor` **and** the grace. InMem applies the same logical rules
immediately inside `maintain`.

---

## Cost summary

| Operation | Scylla round trips | LWT |
|---|---|---|
| Streaming ingest, one key-batch | 1 bounded read (`key_state`, `LIMIT 8`) + 1 read per (granularity, bucket) tile set; writes: 1 per touched partition, issued concurrently | none |
| Request ingest | same, plus 1 single-row LWT per group per publish cadence | 1, amortized |
| WO advance page | 1 due-work read + per-key reads it already needed | none |
| WRO lookup | 1 pin read + planned raw/tile reads | none |
| Checkpoint | 0 (flush + in-memory blob) | none |
| Restore, streaming | 0 | none |
| Restore, request | `O(max_p / p)` parallel steals | 1 per owned group |

Storage per cell after GC: at most four versions. Control-plane blob:
`O(groups x generations)` × 16 bytes.

---

## Accepted divergences and known limits

1. **Late-drop race during replay.** `B` restores its frontier from the
   checkpoint, so it accepts everything `A` accepted, unless replay
   reordering advances `B`'s frontier past an event `A` had already taken.
   Then `B` drops it as late and the cell differs. Pre-existing engine
   nondeterminism, bounded by lateness; not introduced here.
2. **Non-deterministic raw cursors.** `seq_no` comes from `KeyState.next_seq`
   at ingest, so a replay with different arrival order gives the same logical
   event a different `Cursor`. This is why zombie rows must be excluded by
   the cut rather than merely overwritten: LWW alone would leave duplicate
   raw rows. A stable source-assigned event identity would remove the need
   and is worth having for other reasons — future work.
3. **Cut history is not retired in v1.** An entry can only be dropped when no
   row of that generation remains, and a zombie row with a far-future
   `event_ts` is retained by design. Entries are 16 bytes; 1000 generations
   across 128 groups is ~2 MB of control plane and a binary search per row.
   Cap the list and fail loudly if the cap is hit; the mitigation (forced
   version compaction) is future work. The cap bounds **four** things: the
   checkpoint blob, the `serving_cut` payload on the publish LWT, the
   client-side filter cost per row, and the pathological `key_state`
   fallback. It does not bound steady-state query counts — the bounded
   `LIMIT` read does that.
4. **Grey window before steal.** Until the steal is accepted, the old owner
   can still publish. WRO can jump forward on the old cut after `B` has
   started. Accepted; do not start ingest before steal completes, or the
   window widens.
5. **Streaming write zombies are unfenced.** No `owner` row, no ingest LWT.
   The cut fences every **reader**, permanently. The dead worker can still
   write unlogged rows (collected by the drop-everything-else clause of
   per-cell retention) and still emit downstream until master kill. Output
   fencing is the job attempt on the data plane, not `window_pins`.
6. **Publish and checkpoint freshness have head-of-line blocking.** Cuts are
   per group and the prefix is a low-water mark, so one slow or timed-out
   write holds back that group's whole cut — including acked writes to other
   keys at a higher `seq`. Bounded by write timeout, and an unresolvable
   write fails the task. This is the cost of dropping per-key pins; per-key
   pins would isolate it, at the price of everything in *What changed and
   why*. Skipping the hole instead is not an option — see *Acked prefix*.
7. **Generation allocation is a master prerequisite.** See above. The backend
   must refuse to open until it exists.
8. **GC grace compares wall clock across nodes.** `prev_expires_at` is
   written by the publishing owner and may be read by a successor's GC, so
   grace must include a clock-skew allowance on top of the WRO request
   budget.

---

## Alternatives rejected

| Alternative | Why not |
|---|---|
| Opaque `attempt` + equality (#298) | Loses all non-rewritten cells on the second failover; truncates a key's history on republish. |
| Group-wide `prev_E` (#157) | One generation of hole-fill only; needs GC to retain `prev` rows; still wrong on the third generation. |
| Per-key `prev_*` on publish | Same one-generation limit, now multiplied by key cardinality. |
| Copy-on-first-touch / copy-on-restore | Correct but pays a full rewrite of the retained window for every key that goes dirty; that is the cost the cut history exists to avoid. |
| Freeze publish until `B` covers the window | Same cost, and "covers the window" is not observable from the store. |
| `USING TIMESTAMP` LWW instead of cuts | Gives last-writer-wins by generation but cannot express "exclude the post-checkpoint writes of generation `g`", and cannot hide a torn commit. |
| Ingest CAS / write fence | One Paxos per ingest batch. Revisit only if streaming output fencing proves insufficient. |
| Per-key serving pins | Needs a dirty set (unbounded RAM), a chunked conditional BATCH (non-atomic publish), and an unbounded `window_pins` partition that every request point-gets. |

---

## Explicitly not v1

- Publish on checkpoint (the `> cp_wm` gate covers pin-behind-CP).
- Periodic WO-only owner suicide. Cut history + master kill.
- `high_E`, `prev_E`, `window_kg_lease`, `window_recovery_bases`, per-key
  pins, per-key epoch maps, HeadClaim / DashMaps.
- Ingest LWT, copy-on-restore, any-generation cuts, pin = CP as the only
  publish, per-key catch-up freeze.
- Cut history retirement, skinny due index, stable source event identity.

---

## Future next steps

Worker maintenance **orchestration** is decided in the runtime; Scylla's
`maintain` implementation remains TODO. `RestorePlanner` identity mapping on
master must grow the range intersection described above.

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

### WO-only zombie suicide

Streaming has no `owner` row. A later safeguard can reuse the request
`owner` (restore sets it, periodic `IF owner = me`) or ask the master whether
the generation still owns the vertex. Not required for read correctness.
