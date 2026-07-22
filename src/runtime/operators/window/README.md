# Window operators (WO / WRO) — SortedKV path

Canonical notes for the RANGE window stack over SortedKV (design, backends, testing, roadmap).  
**Detailed handoff for a new chat:** [`HANDOFF.md`](HANDOFF.md).  
`storage/sorted_kv/` holds the trait + InMem only; [`top/README.md`](top/README.md) is aggregate API docs (separate).

**Status:** Core cutover is in-tree (stores, `evaluate/`, tile planner, WO/WRO as separate KV clients). Remaining work is under [Next steps](#next-steps).

| Operator | Role |
|----------|------|
| **WO** (`WindowOperator`) | Ingest + watermark advance (`advance_key`); sole writer |
| **WRO** (`WindowRequestOperator`) | Read-only point eval (`evaluate_range_points`) |

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

**Future (not built):** a typed / batch façade above SortedKV — see [Next steps §1](#1-window-store-façade-above-sortedkv). Keep SortedKV as the streaming byte primitive; do not push RecordBatch into that trait.

**Namespaces:** `raw/` (events), `tile/` (acceleration), `meta/` (processed_pos, retractable acc).

**Ingest (atomic):** plan dirty tiles → load → merge in memory → one `WriteBatch` (raw + tiles + meta) → `kv.write`. Readers only use `get` / `scan`; never see torn updates.

**Tiles:** multi-window packing in one KV value (`WindowTiles` / `TileState`). Missing tile ≡ empty bucket. Raw events are source of truth; tiles are acceleration.

**Eval:** see `evaluate/mod.rs` matrix. Cold WO uses `first_ingested` (meta) only to close the load lower bound — same rebuild/slide paths (no separate cold module). Plain+tiles loads gap-only via union of per-emit coverages. WRO retractable: meta + slide when `T >= processed_pos`; plain skips meta. WRO exclude ⇒ skip request-row args only (store window still through `T`).

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
| Stores | `store/` (`event_store`, `tile_store`, `meta_store`, `keys`) |
| Eval | `evaluate/` (`advance`, `points`, `load`, `rebuild`, `slide`, `coverage`) |
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
| sum / count / avg | yes | yes (`supports_tile_slide`) |
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

### 1. Window store façade above SortedKV

Today `EventStore::append_batch_rows` / `scan` pay per-row IPC even for InMem, and the WO path assumes `StoredRow` = 1-row batches. Goal: one abstraction for WO that (a) skips serde on in-proc paths and (b) lets streaming vs batch backends keep their natural layout — without changing `SortedKV`.

```text
WO
  └─ EventLog / WindowStateKv   (append + scan in RecordBatch / typed values)
        ├─ InMem / same-process     — Arc batches or typed values; no IPC/serde
        ├─ StreamingDurable         — 1-row (or small) values over SortedKV
        │                              (Rocks / Slate / gRPC InMemSortedKV)
        └─ BatchDurable (later)     — Arrow/Parquet segments; range append/scan
```

**Design notes (agreed, skip impl for now):**

- **Keep `SortedKV` byte-oriented** (`get` / `scan` / `write`). It is the streaming primitive; batch backends will not map cleanly onto put-per-cursor.
- **Lift EventStore (and later tiles/meta) to batch APIs:** `append(key, batch)`, `scan(key, from, to) → RecordBatch`s (or typed chunks). Streaming impl still splits to 1-row SortedKV puts; batch impl writes segments.
- **Typed in-proc path:** `WindowValue` as `Arc<…>` (raw / tile / meta) so same-process tests and co-located WO skip encode/decode. Cross-worker / durable still goes bytes over SortedKV (or gRPC).
- **Read path must evolve with write:** `RowNav` / eval still assume 1-row `StoredRow`. Multi-row scan results need `(batch, row)` nav (or concat) or advance stays row-taxed even after batch append.
- **WRO:** not in scope for batch-mode store work; remains point eval.

Keep **`InMemSortedKV`**: needed when simulating **separate WO/WRO workers** (local gRPC or kube) with a **shared SortedKV gRPC service**. Wire is bytes → Durable adapter → InMemSortedKV (or Rocks later).

### 2. Testing suite

WO matrix + thin WRO point oracle are in `tests/matrix.rs`. Remaining: stress fixtures / layout under `TESTING` above.

### 3. Eval / tile follow-ups

- Hop emit + tile-add path (add band still raw-only today)
- Plain per-emit full recompute: measure; prefer retractable + tiles over caching (`evaluate/rebuild.rs` TODO)

### 4. Late data / CDC

Proper late-data handling beyond drop-on-`processed_pos` (deferred product work).

### 5. Durable backends

Rocks / Slate adapters; Scylla single-partition layout. Optional snapshots for long WRO scans.

### 6. Cleanup

Fold/rename leftover WO state; remove stale legacy paths if any remain.

---

## Explicitly not doing now

- Implementing the window store façade / `EventLog` / `InMemWindowKv` / multi-row `RowNav` (tracked in Next steps §1)
- Compiling / running the full deferred test suite until asked
