# Window operators (WO / WRO) — SortedKV path

Canonical notes for the RANGE window stack over SortedKV (design, backends, testing, roadmap).  
**Detailed handoff for a new chat:** [`HANDOFF.md`](HANDOFF.md).  
`storage/sorted_kv/` holds the trait + InMem only; [`top/README.md`](top/README.md) is aggregate API docs (separate).

**Status:** All evaluate logic under [`eval/`](eval/) (WO + WRO, slide \| rebuild). Remaining work under [Next steps](#next-steps).

| Operator | Role |
|----------|------|
| **WO** (`WindowOperator`) | Ingest + watermark advance (`advance_key`); sole writer |
| **WRO** (`WindowRequestOperator`) | Read-only point eval (`evaluate_points`) |

Both share one SortedKV (separate clients, no peer `Arc` / `wait_for_operator_state`). Frames: **RANGE only**. ROWS dropped. Scan is **forward-only**. Consistency/caching are backend concerns, not the trait.

---

## Architecture

```text
WO / WRO
  └─ EventStore / TileStore / MetaStore   (byte keys + serde/IPC values today)
       └─ SortedKV
            ├─ InMemSortedKV          (tests; future shared gRPC backend)
            ├─ RocksDB / SlateDB      (planned durable)
            └─ Scylla                 (planned; single-partition batch)
```

**Namespaces:** `raw/{ns}/{hash}/{bucket_ts}/{ts}/{seq_no}` (one row per key), `tile/` (acceleration), `meta/` (`KeyState` incl. `next_seq` + `max_seen`).

**Ingest (atomic):** assign `next_seq` → one put per row under `align_bucket(ts)` → plan dirty tiles → merge → one `WriteBatch` (raw + tiles + meta) → `kv.write`. `bucket_ts` is for envelope scan planning, not multi-row packing ([#155](https://github.com/volga-project/volga/issues/155)).

**Concurrent reads:** [`load_envelope`](store/load.rs) opens `kv.snapshot_partition(partition_key)` then meta + raw/tile scans on that [`KvSnapshot`](../../../storage/sorted_kv/README.md). Rare WO underfetch uses [`load_data`](store/load.rs). No window-level generation/retry.

**Tiles:** multi-window packing in one KV value (`WindowTiles` / `TileState`). Missing tile ≡ empty bucket. Raw events are source of truth; tiles are acceleration.

**Eval:** estimate envelope without meta ([`eval/envelope.rs`](eval/envelope.rs)) → [`store::load_envelope`](store/load.rs) (meta+data, one snap) → slide \| rebuild in memory. Cold WO: if `cold_needed_from` &lt; estimate, one `load_data` widen. Chunks must be Cursor-ordered (KV scan); `RowNav` / emit assert order + dedup. WRO exclude ⇒ skip request-row args only (store window still through `T`).

**Batch mode:** WO ingest/advance only. WRO stays request/row-oriented (not a batch path).

---

## Policy decisions

### Lateness

| Concept | Meaning |
|---------|---------|
| **Streaming late** | `ts ≤ processed_pos` → drop on ingest (`drop_late_entries`). No slack. |
| **`spec.lateness`** | Retention/prune only: drop state older than `processed − max_wl − lateness`. |
| **WRO** | No lateness filter; answer any point still in retained state. |
| **CDC / true late data** | Deferred. |

### Keys

Order-preserving `i64` encoding (`be_i64` / XOR sign bit) for ts and related fields so byte order matches numeric order.

### Parallelism

Stores expose atomic ops; callers own `try_join_all` / `join` (advance, WRO points, prune). No nested joins inside store helpers.

---

## SortedKV contract

Trait: `get` / `scan` / `put` / `delete` / **`write(WriteBatch)`** / `flush`.

- Prefer `write` for multi-key mutations (ingest).
- `write` must be all-or-nothing visibility; `flush` is durability only.
- Last op wins if the same key appears twice in one batch.

### Backend matrix

| Backend | Atomic multi-key write | Notes |
|---------|------------------------|-------|
| **InMem** | Yes (one lock) | Unit/e2e; also stand-in behind future gRPC KV |
| **RocksDB** | Yes (`WriteBatch`) | Single DB |
| **SlateDB** | Yes (`WriteBatch`) | Same model; full SSI txn optional |
| **Scylla** | Same **partition** only | Put raw/tile/meta as clustering rows under one partition key per stream key |

Scylla must not split raw vs tile across partitions if WO→WRO torn-free visibility is required.

---

## Module map

| Area | Path |
|------|------|
| Operators | `window_operator.rs`, `window_request_operator.rs`, `window_tuning.rs` |
| State / ingest / prune | `state/window_operator_state.rs` |
| Tiles | `state/tile/` (`plan`, `update`, `tile`, `granularity`) |
| Stores | `store/` (`load`, `event_chunk`, `row_nav`, `event_store`, `tile_store`, `meta_store`, `keys`) |
| Eval | `eval/` (`envelope`, `advance`, `emit`, `rebuild`, `slide`, `wro`, `primitives`) |
| Aggs | `aggregates/`, `top/`, `cate/` |
| KV | `storage/sorted_kv/` |

Top aggregate API details: [`top/README.md`](top/README.md).

---

## Testing plan

Deferred suite once tile ingest, plain eval, and retractable tile-slide are stable. Goal: correctness first (**tiles ≡ raw-only**), then planner/slide stress.

### Correctness oracle

Same input, two configs — Mode A tiling off, Mode B tiling on — bit-identical (or float-eq) outputs. WO matrix oracle in `tests/matrix.rs`; WRO point oracle (`matrix_wro_points_match_oracle`) covers before/at/after `processed_pos` and exclude on/off.

### Aggregate matrix

| Agg | Plain + tiles | Retractable + tile slide |
|-----|---------------|--------------------------|
| sum / count / avg | yes | yes (`window_supports_tile_slide`) |
| min / max | yes (merge) | N/A — row retract only |
| TOP-N / CATE | as supported | no tile slide |

### Stress cases (summary)

- Window ≪ / ≫ min gran; multi-gran packing; unaligned edges; sparse empty buckets
- Slide ≪ min gran (row retract) vs leave span ≥ gran (tile retract)
- Cold start large window; large watermark jump; duplicate ts/seq
- Multi-window / multi-key; WRO subset; concurrent WO ingest (optional torn-read stress)

### Suggested fixtures

Dense short (tiles idle), sparse long (tile retract), aligned, unaligned edges, multi-agg, multi-window.

### Layout (later)

```text
tests/window/
  oracle.rs, fixtures.rs,
  plain_tiles.rs, retractable_slide.rs, multi_window.rs, wro_points.rs
```

Unit: `state/tile/tests.rs`, agg retract roundtrip, SortedKV `write` atomicity.

**Out of scope for first pass:** perf/hit-rate benches; Rocks/Slate/Scylla; ROWS; min/max tile retract.

---

## Next steps

Ordered roughly by dependency / payoff. Skip impl until picked up explicitly.

### 1. Store / load (done shape)

- [`EventStore`](store/event_store.rs): one-row keys under `bucket_ts`
- [`load_envelope`](store/load.rs) / [`load_data`](store/load.rs): partition loads (meta+data / data-only)
- [`RowNav`](store/row_nav.rs) / emit over [`EventChunk`](store/event_chunk.rs) (assert Cursor order)
- Optional later: multi-row packing inside a bucket ([#155](https://github.com/volga-project/volga/issues/155))

WO advance is always [`eval::advance_key`](eval/). Keep **`InMemSortedKV`** for separate WO/WRO workers.

### 2. Testing suite

WO matrix + thin WRO point oracle are in `tests/matrix.rs`. Remaining: stress fixtures / layout under `TESTING` above.

### 3. Eval / tile follow-ups

- Hop emit + tile-add path (add band still raw-only today)
- ~~Tiled rebuild two-phase emit-band~~ — geometry envelope load (shared WO/WRO)

### 4. Late data / CDC

Proper late-data handling beyond drop-on-`processed_pos` (deferred product work).

### 5. Durable backends

Rocks / Slate adapters; Scylla single-partition layout. Optional snapshots for long WRO scans.

### 6. Cleanup

Fold/rename leftover WO state; remove stale legacy paths if any remain.

---

## Explicitly not doing now

- Multi-row raw packing inside buckets (see #155)
- Compiling / running the full deferred test suite until asked
