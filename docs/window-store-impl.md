# Window store implementation plan

Stacked PRs to land the Scylla window backend. Architecture, schemas, MVCC,
consistency, GC, and rescale live in the store design — **do not duplicate
them here**.

- Design (Scylla + engine contract): [`STORE_DESIGN.md`](../src/runtime/operators/window/STORE_DESIGN.md)
- Operator semantics: [`window/README.md`](../src/runtime/operators/window/README.md)

**This protocol supersedes [#157](https://github.com/volga-project/volga/pull/157)
and [#298](https://github.com/volga-project/volga/pull/298).** Ignore older
comments and commits that specify `window_kg_lease`, `prev_E`, `high_E`,
`window_recovery_bases`, per-key serving pins, HeadClaim / DashMaps, ingest
LWT, epoch seeding from a checkpoint or pin, or publish-on-checkpoint. The
normative cut is `STORE_DESIGN.md` → *Protocol (normative)*.

Engine contract + InMem isolation already landed
([#285](https://github.com/volga-project/volga/pull/285)).

---

## Protocol (stack must match this)

**Version (always).** Rows on raw / tiles / `key_state` / due work cluster
`(generation, seq)`, **descending**. `generation` is durably monotonic and
never reused; `seq` is a per-`key_group` local counter starting at 0. There is
no seeding from checkpoints or pins. Ingest is unlogged, **no LWT**.

**Cut history.** Per `key_group`, a sorted list of one `(generation, seq)`
prefix per generation:

```text
allows(v) ⇔ history has an entry for v.generation AND v.seq <= entry.seq
```

A cut may only name an **acked** prefix. Timed-out writes are retried at the
same version and block the cut.

**WO visibility.**

```text
row.generation == my_generation OR cp_cut[g].allows(row.version)
```

Empty until the first restore. Checkpoint blob stores a **per-owned-group
cut history**; groups this writer never touched need no special case.

**Request (only if WRO is on)** — `window_pins`, **one row per key group, no
clustering column**:

```text
owner, serving_wm, serving_cut
```

- Steal (restore, per owned group, in parallel): read `owner`, then
  `SET owner = me IF owner =` the observed owner; absent row ⇒
  `INSERT ... IF NOT EXISTS`. Serving untouched. Fail the task on
  miss-after-reread or timeout.
- Publish (OnCommit / periodic, **not** on CP): if `publish_ok`, one
  single-row LWT setting `serving_wm` and `serving_cut` `IF owner = me`. Fail
  the task on miss or timeout; a timeout is unknown, so do not retry it as a
  fresh publish at a later prefix.

**Publish payload is the clamp.** It is computed from `cp_cut`, never from
the live serving cut:

```text
next = cp_cut[g].advance(my_generation, cut_top[g])
```

`advance` takes `&self`, returns a new history, and only adds or raises the
caller's own entry. Basing it on the live cut keeps the predecessor's
post-checkpoint prefix and serves it next to the successor's replay of the
same input — duplicate raw rows. `cut_top[g] == None` contributes nothing;
never write a `(me, 0)` entry.

```text
publish_ok ⇔
    current_wm is set
    AND current_wm >= serving_wm
    AND current_wm > cp_wm
    AND cut_top[g] > published_top[g]
```

- WRO: read the pin row **once** (absent or empty ⇒ empty result), then
  `serving_cut.allows(row.version)`. Cell collisions resolve on the greatest
  `(generation, seq)` **lexicographically**.
- `CutHistory::advance` never raises an entry for another generation, so a
  successor's first publish **clamps** the predecessor's entry down to the
  checkpoint prefix. The serving cut is monotonic in event-time coverage, not
  in row membership.

**Writer torn commits.** Cut-filtered readers never see a torn commit, but
WO reads of its own generation are uncapped, so the rule is per key: while a
commit for key `K` is in flight, issue no read of `K` and no second commit
for `K`. Cross-key concurrency inside a group stays legal. Do **not** cap the
writer at the acked prefix — that hides another key's acked write behind a
hole and breaks read-your-writes. `seq` needs an atomic per-group allocator;
the prefix is a low-water mark and never skips a hole.

**GC.** Per cell keep at most four versions: newest of `my_generation`,
newest allowed by `cp_cut`, newest allowed by `serving_cut`, and newest
allowed by `prev_serving_cut` while `now < prev_expires_at`. Drop everything
else, including above-cut rows of older generations. "In the cut or in a pin"
is **not** a sufficient rule — it never collapses the writer's own superseded
versions.

`prev_serving_cut` / `prev_expires_at` are two extra columns on the pin row,
written **only** by a publish that lowers or drops another generation's entry
(in practice the first publish after a restore). Unconditional writes would
push a clamped cut out of the slot while a reader is still pinned to it;
omitting them entirely makes the grace rule unimplementable, because the pin
row is overwritten on every publish.

**Not doing:** publish on CP, `prev_E`, `high_E`, per-key pins, dirty-key
sets, conditional BATCH publishes, ingest CAS, `recovery_bases`,
any-generation cuts, WO-only owner suicide.

---

## Rules

1. Merge **this design PR** first. Stack impl PRs on `master` after that.
2. Engine contract / InMem isolation is already on master. Later PRs
   implement the Scylla protocol above.
3. **PR 0b (durable generation) gates every versioned write.** The Scylla
   backend must refuse to open without it.
4. Rescale is in the **protocol** (per-group cut histories, planner slices).
   The `RestorePlanner` code change can follow; do not design as
   same-assignment-only.

---

## Stack

Each PR stacks on the previous. Do not merge adjacent PRs.

| # | PR | Branch | Scope |
|---|---|---|---|
| 0 | Design | `docs/window-store-cuts` | This PR. Spec only. Replaces #157 and #298. |
| 0b | Durable generation | `feat/master-generation` | Master allocates `generation` from durable state (`max(observed) + 1`), persists before `Configure`, passes it with restore data. Assert it dominates every restored cut generation. Small, but **prerequisite**. |
| 1 | Due paging | `feat/wo-load-triggers` ([#296](https://github.com/volga-project/volga/pull/296)) | Store `load_triggers`; operator pages. Prefetch later: [#297](https://github.com/volga-project/volga/issues/297). Unaffected by the protocol change. |
| 2 | InMemoryGrpc + request-mode runner | `feat/request-inmem-grpc` | [#162](https://github.com/volga-project/volga/issues/162). |
| 3 | Scylla write path | `feat/scylla-wo-write` ([#287](https://github.com/volga-project/volga/pull/287)) | Session, DDL, unlogged versioned writes, due-work read. Cut history **empty** (visibility `my_generation` only). Acked-prefix tracking. **No pins, no ingest LWT.** Rewrite schema vs any `window_head` / `attempt`-blob leftover. |
| 4 | Checkpoint / restore + pins | `feat/scylla-wo-checkpoint` ([#288](https://github.com/volga-project/volga/pull/288)) | `CutHistory` per owned group in `Versioned`. WO filter `my_generation \| cp_cut[g].allows(..)`. Request: single-row `window_pins` (incl. `prev_serving_cut` / `prev_expires_at`); read-then-CAS steal; `publish_ok`; single-row publish LWT with the payload computed from `cp_cut`. **Rewrite:** drop `prev_E`, `recovery_bases`, HeadClaim, pin=CP, per-key pins, dirty sets. |
| 5 | `maintain` / GC | `feat/scylla-wo-maintain` ([#289](https://github.com/volga-project/volga/pull/289)) | `window_kg_buckets`, `floor_bucket`, **per-cell version retention** (four versions), grace driven by `prev_serving_cut` / `prev_expires_at`. |
| 6 | Scylla request store | `feat/scylla-request-store` ([#290](https://github.com/volga-project/volga/pull/290)) | Read the pin row once. Client-side `serving_cut.allows(..)` + per-cell greatest `(generation, seq)`. |

#287–#290 were stacked on the #157 protocol and partly restacked onto #298.
Both cuts are dead. The row key of every data table changes
(`(generation, seq)` descending, replacing `(attempt, epoch)` ascending) and
`window_pins` loses its clustering column, so restacking before this merges
means rewriting the schema twice. #287 is the least affected and can go
first if forward progress is needed while the protocol settles.

### Deliberately out of the stack

- Cut history retirement (entries are 16 bytes; cap and fail loudly).
- Skinny due index replacing per-event due rows — perf follow-up, changes the
  `load_triggers` contract from PR 1. Rationale and expected win are in the
  design's *Due-work index (decision)*.
- Stable source-assigned event identity (would make raw replay idempotent).
- Foyer / cache quotas, `USING TIMESTAMP` instead of owner CAS, ingest CAS,
  CDC / late events, WO-only periodic owner suicide.

[#162](https://github.com/volga-project/volga/issues/162) is **PR 2**. **PR 6**
only adds Scylla as another `backend` on that runner.

---

## Testing

Request mode is a **generic graph split**: write path (ingest / maintain
state) and read path (point lookup against published state). Windows (WO/WRO)
are the first SQL shape, not the harness.

Three jobs. Do not fold them into one suite.

| Layer | What it proves | Where | Backend |
|---|---|---|---|
| **A. Operator / store contract** | Window eval, tiles, watermarks, key-group isolation | `window/tests/*` | Process-local `InMemWindowStore`. PR 3 adds a Scylla testcontainer for write-path store ops. |
| **B. Request HTTP plumbing** | Request source ↔ sink, concurrency, echo | `tests/inprocess/request_source.rs` | No operator store. |
| **C. Request-mode correctness** | Write path, read path, and storage on separate processes; exact oracle vs published frontier | One runner on `VolgaCluster` ([#162](https://github.com/volga-project/volga/issues/162)) | `InMemoryGrpc` first, then Scylla. |

### Oracle (Layer C)

| | InMemoryGrpc | Scylla |
|---|---|---|
| Happy path | Last successful `commit` | Last published `serving_cut`: `serving_cut.allows(version)`, greatest version per cell |
| Write-worker kill | Storage stays up. After the new write worker attaches, reads = that store's current contents. No fencing/MVCC. | WRO keeps the previous `serving_cut` for **every** key while the writer recovers. Grey window until steal is accepted. After steal, CAS blocks the zombie's publish. After `publish_ok`, reads = new serving. |
| Read-worker kill | Requests fail until replacement; then same store. | Same lifecycle; answers still pinned to the serving cut. |
| Storage kill | State gone. Do not test as correctness. | Out of v1 unless we add a dedicated Scylla HA case. |

### Protocol cases that need explicit tests

These are the ones the two reviews of #298 found by reading, not by running.
Each is cheap against the Scylla testcontainer plus a fake generation
allocator.

1. **Two failovers, history survives.** `A` writes, checkpoints, writes more,
   dies; `B` restores, rewrites *some* cells, checkpoints, dies; `C` restores.
   Assert `A`'s pre-checkpoint cells that neither `B` nor `C` rewrote are
   still readable by `C`, for an active key and for an idle key. This is the
   case #298 lost.
2. **Republish keeps a key's retained window.** Key with a long window,
   history from `A`, then one new event under `B` and a publish. Assert WRO
   returns the full window, not just `B`'s row. This is the other #298 bug.
3. **Zombie exclusion is permanent.** `A`'s post-checkpoint writes must be
   invisible to `B`, to `C`, and to WRO after every subsequent publish.
4. **Clamp.** After `B`'s first publish, `A`'s rows above the checkpoint
   prefix stop being served; event-time coverage does not regress. Assert the
   published cut equals `cp_cut.advance(B, cut_top)` and **not**
   `live_serving.advance(...)` — the second form leaves `A`'s post-checkpoint
   rows visible next to `B`'s replay, which is duplicate raw data. Include a
   generation that published but never checkpointed, so its entry is
   *dropped* rather than lowered.
4b. **GC grace after a clamp.** Pin a WRO read at the pre-clamp cut, publish
   the clamp, run `maintain`, then finish the read: the rows it needs must
   still be there. Then advance past `prev_expires_at` and assert they are
   collected. Also assert an ordinary (non-clamping) publish does not
   overwrite the grace slot.
5. **Torn commit.** Kill between the raw write and the tile write of one
   commit; assert no reader sees the partial commit and that the cut did not
   advance past it.
6. **Timed-out write blocks the cut.** Inject a write timeout; assert the
   checkpoint and the publish both refuse to name that prefix.
7. **Version retention.** Rewrite one tile and one `key_state` `N` times,
   run `maintain`, assert at most four versions per cell remain and that
   `load_key_state` is one CQL query. Separately assert `load_key_state` is
   still one query on an idle key after many generations — the failure mode
   is a per-generation walk costing `1 + |history|` round trips.
8. **Generation monotonicity.** Restore twice from the same checkpoint;
   assert the two generations differ and that the first one's writes are
   excluded from the second. Assert the backend refuses to open on a
   non-dominating generation.
9. **Rescale.** `p=3 -> p=2` at `max_p=128`; assert each new task inherits
   per-group cut histories from the right source blob and reads pre-rescale
   cells for every group it now owns.
10. **Bootstrap.** Fresh job: WRO returns empty until the first publish;
    `publish_ok` skips both watermark compares.

Layer A keeps the window semantics matrix on `InMemWindowStore`. Do not re-run
the matrix on the cluster. PR 6 adds `backend = Scylla` to the Layer C
runner; failure cases use the same runner plus `FaultAction` already on
`VolgaCluster`.
