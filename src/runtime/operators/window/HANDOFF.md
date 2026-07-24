# WO / WRO SortedKV — handoff doc (continue from here)

**Purpose:** Full design dump so a new chat can continue without prior transcript.  
**Branch context:** RANGE windows; **WO/WRO in `eval/`** (envelope estimate + slide \| rebuild).  
**Canonical short notes:** also keep [`README.md`](README.md) in sync for roadmap; this file is the detailed handoff.

**Do not** run tests/compile unless the user asks (recent preference).

---

## 0. Evaluate layout (current)

Everything lives under [`eval/`](eval/):

| Path | Role |
|------|------|
| `eval/envelope` | Envelope bounds (`estimate_*`, `cold_needed_from`, `window_start_floor`) |
| `eval/advance` | WO: estimate → `load_envelope` → **slide \| rebuild** → emit |
| `eval/emit` | WO emit ends + input-row assembly |
| `eval/wro` | WRO: same via `load_envelope` + `evaluate_points` |
| `eval/slide`, `rebuild`, `primitives` | Shared kernels (CPU coverage only; no KV LoadPlan) |
| `store/load` | `PartitionData` / `WindowData` / `WindowView`; `load_envelope` + `load_data` |
| `store/event_chunk`, `row_nav` | One-row `EventChunk`s; `RowNav` asserts Cursor order |

Branching is only [`can_slide`](eval/mod.rs) (retractable acc), not a "plain operator" family.
`AggregatorType::PlainAccumulator` remains in the registry only for DF acc construction (min/max until retractable).

### Design targets (still open)

- Retractable min/max (then registry can drop Plain)
- AccCentric hop; CDC
- Optional multi-row packing inside a bucket ([#155](https://github.com/volga-project/volga/issues/155))

---

## 1. Operators and roles

| Operator | File | Role |
|----------|------|------|
| **WO** `WindowOperator` | `window_operator.rs` | Sole writer: ingest + watermark advance (`eval::advance_key`) |
| **WRO** `WindowRequestOperator` | `window_request_operator.rs` | Read-only point eval (`eval::evaluate_points`) |

- Shared **SortedKV** (separate store clients; no peer `Arc` / `wait_for_operator_state`).
- Frames: **RANGE only** (ROWS dropped).
- Scans: **forward-only**.
- **Batch mode:** WO ingest/advance only. WRO is request/row-oriented (not a batch path).

---

## 2. Storage layout

```text
WO / WRO
  └─ EventStore / TileStore / MetaStore   (byte keys + IPC/serde values today)
       └─ SortedKV (InMem today; Rocks/Slate/Scylla planned)
```

| Namespace | Store | Contents |
|-----------|--------|----------|
| `raw/` | `EventStore` | One row/key: `raw/{ns}/{hash}/{bucket_ts}/{ts}/{seq_no}` |
| `tile/` | `TileStore` | `(granularity, tile_start)` → `WindowTiles` (multi-window packed) |
| `meta/` | `MetaStore` | Per stream-key `KeyState` |

`bucket_ms` from `WindowOperatorSpec::resolve_bucket_ms` (tiling min gran, else `60_000`) — used to `align_bucket` for raw keys and envelope scans. Multi-row packing deferred ([#155](https://github.com/volga-project/volga/issues/155)). Breaking key format OK for InMem tests.

### `KeyState` (`store/meta_store.rs`)

| Field | Meaning |
|-------|---------|
| `max_seen` | Max ingested `Cursor` `(ts, seq)` (event-time frontier; ts-first) |
| `next_seq` | Next row `seq_no` to assign (monotonic; **not** derived from `max_seen.seq`) |
| `processed_pos` | WO advance frontier (**shared across all windows** on the key) |
| `first_ingested` | First accepted ingest cursor — **cold coverage lower bound only**. Set on ingest as min; **not** maintained on prune/retract. `#[serde(default)]` for old blobs |
| `accumulators` | Per-`WindowId` retractable acc state (plain windows: absent) |

### Cursor

`Cursor { ts: i64, seq_no: u64 }` — lexicographic order. Seq from `KeyState.next_seq` after late-drop (`append_seq_no_column`).

### Ingest (`state/window_operator_state.rs`)

1. `drop_late_entries`: drop `ts <= processed_pos` (streaming late; **no** lateness slack).
2. Assign seqs from `next_seq`; update `max_seen` / `first_ingested` / bump `next_seq`.
3. Plan dirty tiles → parallel get → `apply_batch_to_tiles` in mem.
4. One atomic `WriteBatch`: one raw put per row under `bucket_ts` + tiles + meta → `kv.write`.

### Prune

`spec.lateness` = retention only: drop raw/tiles older than `processed − max_wl − lateness`. Does **not** affect ingest acceptance.

---

## 3. Tiling geometry (`state/tile/`)

| Piece | Role |
|-------|------|
| `plan_coverage` / `CoveragePlan` | Pure geometry: `tile_runs` + `raw_head` / `raw_tail`. Missing KV tile after a covered scan ⇒ **empty bucket** (not an error) |
| `plan_update_runs` | Ingest: which tile keys a batch dirties (per gran, not coalesced like eval) |
| `project_tiles` | `TileMap` → `Vec<Tile>` for one `WindowId` |
| `window_supports_tile_slide` | sum/count/avg (etc.) can tile-retract; min/max cannot |

**Never** plan tiles from `i64::MIN` (geometry walk hangs). Cold needs a **closed** lower bound → `first_ingested.ts − wl`.

Ideal pipeline: **estimate envelope (no meta) → `store.load_envelope` → slide | rebuild in memory**.  
Formulas live in [`eval/envelope.rs`](eval/envelope.rs). Eval plans CPU coverage
(`plan_rebuild_prep`) only — store owns bucket/tile KV scans; slide leave plans in `slide`.
Overfetch ≈ extra tiles; raw edges &lt; one min-gran.

---

## 4. Evaluate module map

```text
eval/
  envelope (bounds + window_start_floor / cold_needed_from)
  advance, emit, slide, rebuild, primitives, wro, output
store/load.rs         PartitionData / WindowData / WindowView + load_envelope / load_data
config.rs             WindowConfig + BuiltWindows
```

| Path | Responsibility |
|------|----------------|
| `eval/envelope` | Conservative IO bounds without meta; post-meta floors |
| `eval/advance` | WO entry: estimate → `load_envelope` → optional `load_data` widen → slide \| rebuild |
| `eval/emit` | WO emit ends + input-row scalars (`flatten_ordered`) |
| `eval/slide` | leave+add produce; `slide_same_ts` for WRO |
| `eval/rebuild` | `eval_rebuild` + produce |
| `eval/wro` | WRO entry (same envelope load + slide \| rebuild) |
| `primitives/*` | acc / `plan_rebuild_prep` |
| `store/load` | One snap: meta + bucket raw + tiles |

### Execution matrix

| | **!can_slide** (e.g. min/max today) | **can_slide** |
|--|-------------------------------------|---------------|
| **WO** | Estimate → load; rebuild each emit | Estimate → load; leave+add (or seed) |
| **WRO** | Estimate `T−wl` → load; rebuild | Estimate `T−2·wl` → load; slide or rebuild |

Cold WO: after meta, if `cold_needed_from` &lt; estimate → one `load_data` widen.

WRO: meta comes from `load_envelope` (not a prior get). Read-only.  
`EXCLUDE CURRENT ROW` ⇒ store through `T`; skip request-row args only.

---

## 5. Load (estimate owns IO bounds)

| Case | Estimated envelope (no meta) |
|------|------------------------------|
| WO advance | `[wm − max_wl − pad, wm]` (`pad` ≈ lateness) |
| WRO rebuild-only | `[min(T) − wl, max(T)]` |
| WRO slide-capable | `[min(T) − 2·wl, max(T)]` |
| Ingest | N/A (meta + touched tiles → WriteBatch) |

Post-load, CPU still uses tight leave/add / rebuild units inside the loaded view.

### Data flow

```text
load_envelope → PartitionData { key_state, data: WindowData }
  └─ for_window(id, expr) → WindowView { RowNav, Arc<[Tile]> }
```

---

## 6. Edge cases / invariants (watch these)

1. **Cold / `first_ingested`:** Only closes lower bound; unused after first advance. Don’t sync with prune.
2. **Envelope overfetch:** Store loads full bucket range + tile interior; eval uses coverage for CPU merge/retract only.
3. **Retractable cold full raw:** Keep full raw under tiles for seed+slide leave mid-tile.
4. **Missing tile:** Empty bucket, not error.
5. **`processed_pos` shared:** All windows on a key advance together; emit cursors from loaded chunks.
6. **Duplicate ts:** Seq order matters; oracle/matrix fixture includes `(60_000, 2.0)` and `(60_000, 2.5)`.
7. **Streaming late vs retention:** Ingest drops `ts <= processed_pos`; `lateness` only for prune.
8. **WRO past `T < P` or window fully replaced (`T − wl > P`):** Rebuild, don’t slide.
9. **Tile slide only if** `window_supports_tile_slide` and leave span ≥ min granularity; else row retract.
10. **Concurrency:** WO sole writer per key. Eval uses `eval/envelope` + `load_envelope` so meta+data share one `snapshot_partition` — no window fencing.
10. **Raw is one row per KV key** under `bucket_ts`; load may return many 1-row `EventChunk`s — `RowNav` sorts by cursor.
11. **Vocabulary:** update/retract into acc (`update_row`, `update_acc_from_plan`, `update_raw_window`) — no “fold” / “apply”.
12. **WRO exclude:** store window always through request `T`; exclude only skips virtual request args. Include can double-count if store already has rows at `T`.
13. **WO multi-emit leave:** re-plans per emit inside `retract_leaving`; load already has full leave span — re-plan ⊂ loaded (else row retract).

---

## 7. Ingest helpers (perf notes, not done)

- `cursor_range_from_batch`: keep O(n) scan (composite cursor); don’t use `sort_to_indices`.
- `drop_late_entries`: still builds index vec + `take` — good candidate for `gt` + `filter_record_batch` later.
---

## 8. Current tests (what exists)

### High-signal (tiling / WO / WRO correctness)

| Location | What |
|----------|------|
| `tests/matrix.rs` | Oracle vs WO: sum/count/avg + min/max; tiling off / 1m / 1m+5m; single WM; two-WM warm slide; short window all-raw |
| `tests/matrix.rs` `matrix_wro_points_match_oracle` | WRO vs same fixture: tiling off + 1m; points before / at / after `processed_pos`; exclude on/off |
| `tests/smoke.rs` | Basic WO / late drop / WO↔WRO smoke |
| `tests/harness.rs` | Shared WO harness + `WoWroHarness` (shared KV) |
| `state/tile/plan.rs` `plan_tests` | Geometry: coarse tiles, raw runs, same-ts, wl ≪ gran, update runs |
| `state/tile/plan` | Geometry contracts |
| `state/tile/tests.rs` | Tile ingest/update integration |

### Aggregate unit (not full WO/WRO matrix)

- `top/tests/*`, `top/accumulators/*` unit tests  
- `cate/tests/*`  

### Explicitly thin / missing

- No combinatorial TOP/CATE × tiling × WO/WRO suite (keep at accumulator layer until tile-slide exists).
- Deferred layout `tests/window/{oracle,fixtures,...}` not created yet.

---

## 9. Next testing steps (agreed priority)

1. ~~**WRO points vs same oracle** on `matrix` fixture~~ **done** (`matrix_wro_points_match_oracle`)

2. Keep planner/load unit tests **small** — only geometry/load contracts; don’t restate oracle.

3. Later stress (README): wl ≪/≫ gran, sparse empty buckets, multi-key, optional torn-read.

4. **Do not** add low-signal tests that only assert “plan_coverage was called.”

### Agg × path policy

| What | Approach |
|------|----------|
| sum/count/avg + min/max | Oracle + tiling variants (matrix) — keep/extend |
| WRO | Thin point oracle on matrix — done |
| TOP/CATE | Accumulator unit tests; optional one WO smoke later |
| Skip | Full cartesian agg × path × tiling × WM |

---

## 10. Open follow-ups (impl, not urgent)

From README + recent discussion:

1. Optional multi-row packing inside a bucket ([#155](https://github.com/volga-project/volga/issues/155)).
2. **WO retractable leave:** per-emit re-plan is intentional (see §6.13); optional precompute of per-emit leave coverages only if profiling shows plan_coverage cost.
3. Hop emit: plug calendar into emit schedule + [`window_range_for_end`] (aligned bounds); tile-aligned hops → tiles-only load.
4. Plain per-emit full recompute: measure; prefer retractable+tiles over caching.
5. CDC / true late data.
6. Durable backends (Rocks/Slate/Scylla).
7. `drop_late_entries` → Arrow `gt` + `filter_record_batch`.

---

## 11. File cheat sheet

```text
eval/                 envelope, advance, slide, rebuild, wro, primitives, output
config.rs
state/…  store/…  tests/…  README.md / HANDOFF.md
```

---

## 12. Suggested first prompt in a new chat

> Continue window work from `HANDOFF.md`. WO advance is `eval/`. Don’t run tests unless asked.

Or pick another item from §10.
