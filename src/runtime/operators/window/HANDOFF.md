# WO / WRO SortedKV — handoff doc (continue from here)

**Purpose:** Full design dump so a new chat can continue without prior transcript.  
**Branch context:** RANGE-only tiling + evaluate refactor; recent work = cold via `first_ingested`, gap-only plain load, `KeyBatch`/`WindowView`, WO/WRO module split.  
**Canonical short notes:** also keep [`README.md`](README.md) in sync for roadmap; this file is the detailed handoff.

**Do not** run tests/compile unless the user asks (recent preference).

---

## 1. Operators and roles

| Operator | File | Role |
|----------|------|------|
| **WO** `WindowOperator` | `window_operator.rs` | Sole writer: ingest + watermark advance (`evaluate::advance_key`) |
| **WRO** `WindowRequestOperator` | `window_request_operator.rs` | Read-only point eval (`evaluate::evaluate_range_points`) |

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
| `raw/` | `EventStore` | Per-cursor event rows (today: 1-row IPC `RecordBatch` per put) |
| `tile/` | `TileStore` | `(granularity, tile_start)` → `WindowTiles` (multi-window packed) |
| `meta/` | `MetaStore` | Per stream-key `KeyState` |

### `KeyState` (`store/meta_store.rs`)

| Field | Meaning |
|-------|---------|
| `max_seen` | Last ingested `Cursor`; `next_seq = max_seen.seq + 1` |
| `processed_pos` | WO advance frontier (**shared across all windows** on the key) |
| `first_ingested` | First accepted ingest cursor — **cold coverage lower bound only**. Set on ingest as min; **not** maintained on prune/retract. `#[serde(default)]` for old blobs |
| `accumulators` | Per-`WindowId` retractable acc state (plain windows: absent) |

### Cursor

`Cursor { ts: i64, seq_no: u64 }` — lexicographic order. Seq assigned on ingest after late-drop (`append_seq_no_column`).

### Ingest (`state/window_operator_state.rs`)

1. `drop_late_entries`: drop `ts <= processed_pos` (streaming late; **no** lateness slack).
2. Assign seqs; update `max_seen` / `first_ingested` via `cursor_range_from_batch` (row O(n) min/max — Arrow sort not worth it for composite cursor).
3. Plan dirty tiles → parallel get → `apply_batch_to_tiles` in mem.
4. One atomic `WriteBatch`: raw rows + tiles + meta → `kv.write`.

### Prune

`spec.lateness` = retention only: drop raw/tiles older than `processed − max_wl − lateness`. Does **not** affect ingest acceptance.

---

## 3. Tiling geometry (`state/tile/`)

| Piece | Role |
|-------|------|
| `plan_coverage` / `CoveragePlan` | Pure geometry: `tile_runs` + `raw_gaps`. Missing KV tile after a covered scan ⇒ **empty bucket** (not an error) |
| `plan_update_runs` | Ingest: which tile keys a batch dirties (per gran, not coalesced like eval) |
| `project_tiles` | `TileMap` → `Vec<Tile>` for one `WindowId` |
| `window_supports_tile_slide` | sum/count/avg (etc.) can tile-retract; min/max cannot |

**Never** plan tiles from `i64::MIN` (geometry walk hangs). Cold needs a **closed** lower bound → `first_ingested.ts − wl`.

Ideal pipeline (partially achieved): **coverage → LoadPlan → load → apply same CoveragePlan**.  
Apply helpers take `&CoveragePlan` (no re-plan inside). Callers may still plan per emit for multi-emit WO.

---

## 4. Evaluate module map (`evaluate/`)

```text
evaluate/
  primitives/   plan, load, coverage
  strategies/   rebuild, slide
  wo/           advance, plan, produce
  wro/          points, load
  output.rs     assemble_window_batch
config.rs       WindowConfig + BuiltWindows::for_wo/for_wro
```

| Path | Responsibility |
|------|----------------|
| `primitives/plan` | `LoadPlan`, `plan_gap_only_ends`, `plan_slide`, bounds |
| `primitives/load` | KV IO → `KeyBatch` / `WindowView` |
| `primitives/coverage` | apply/retract `CoveragePlan` + `apply_row` / `retract_row` |
| `strategies/rebuild` | `eval_rebuild` |
| `strategies/slide` | `eval_slide_as_of` |
| `wo/advance` | WO entry |
| `wo/plan` | `plan_window_load` |
| `wo/produce` | `produce_plain_range` / `produce_retractable_range` |
| `wro/points` | WRO entry |
| `wro/load` | `load_rebuild_points` |
| `output` | `assemble_window_batch` |
| `config` | `WindowConfig`, `BuiltWindows` |

### Execution matrix

| | **Plain** | **Retractable** |
|--|-----------|-----------------|
| **WO** | Rebuild each emit in `(prev, effective]` | No acc: seed/rebuild then slide within advance; warm: leave+add slide |
| **WRO** | Rebuild each point (+ request row if !exclude) | Slide if acc present, `T >= P`, and `T − wl <= P`; else rebuild. `T == P`: acc only (no store IO) |

Cold WO = no `processed_pos`: same paths; `win_start = first_ingested.ts − wl` (else `i64::MIN` → raw-only, no tiles).

WRO: load meta **only if any window is retractable**. Read-only (never `put_key`).  
`EXCLUDE CURRENT ROW` ⇒ same store window through `T`; skip request-row args only.

---

## 5. Load planning details (`wo/plan` / `primitives/plan` / `load`)

### `plan_window_load` (WO — `wo/plan.rs`)

| Case | Plan |
|------|------|
| **Plain + tiles** | Gap-only **union** of `plan_coverage` per emit end; caller peeks emit band and `with_merged_rows` |
| **Plain, no tiles** | Full raw `[win_start, effective]` |
| **Retractable, no prev / no acc** | **Full raw** `[win_start, effective]` + tiles when closed |
| **Retractable warm** | `plan_slide`: add `(prev, effective]` raw; leave `[prev−wl, effective−wl)` tiles+gaps |
| **Open bound** `win_start == i64::MIN` | Raw only, no tiles |

`plan_slide` (shared in `primitives/plan`) returns `(LoadPlan, Option<CoveragePlan>)` — leave coverage threaded into WRO `eval_slide_as_of`.  
WO multi-emit leave still **re-plans per emit** (see §6.13).

### Data flow after load

```text
LoadPlan → load() → KeyBatch
  └─ for_window(id, expr) → WindowView { RowNav, Arc<[Tile]> }
```

Eval APIs take `&WindowView`.

---

## 6. Edge cases / invariants (watch these)

1. **Cold / `first_ingested`:** Only closes lower bound; unused after first advance. Don’t sync with prune.
2. **Plain gap-only:** Sub-window coverage ≠ parent union geometry → load = **union of per-emit coverages** + merge emit-band rows. Do not gap-only-load one big `[win_start, effective]` and re-plan smaller windows without those gaps.
3. **Retractable cold full raw:** Unlike plain, keep full raw under tiles for seed+slide leave mid-tile.
4. **Missing tile:** Empty bucket, not error.
5. **`processed_pos` shared:** All windows on a key advance together; emit cursors from first window’s nav.
6. **Duplicate ts:** Seq order matters; oracle/matrix fixture includes `(60_000, 2.0)` and `(60_000, 2.5)`.
7. **Streaming late vs retention:** Ingest drops `ts <= processed_pos`; `lateness` only for prune.
8. **WRO past `T < P` or window fully replaced (`T − wl > P`):** Rebuild, don’t slide.
9. **Tile slide only if** `window_supports_tile_slide` and leave span ≥ min granularity; else row retract.
10. **EventStore still 1-row IPC** — known perf ceiling; façade deferred (README §1).
11. **Vocabulary:** apply/retract only (`apply_row`, `apply_gap`, `apply_plan`, `apply_raw_window`) — no “fold”.
12. **WRO exclude:** store window always through request `T`; exclude only skips virtual request args. Include can double-count if store already has rows at `T`.
13. **WO multi-emit leave:** re-plans per emit inside `retract_leaving` (different leave band each emit); load already has full leave span — re-plan ⊂ loaded (else row retract).

---

## 7. Ingest helpers (perf notes, not done)

- `cursor_range_from_batch`: keep O(n) scan (composite cursor); don’t use `sort_to_indices`.
- `drop_late_entries`: still builds index vec + `take` — good candidate for `gt` + `filter_record_batch` later.
- Batch EventLog façade: documented in README; skip impl for now.

---

## 8. Current tests (what exists)

### High-signal (tiling / WO / WRO correctness)

| Location | What |
|----------|------|
| `tests/matrix.rs` | Oracle vs WO: sum/count/avg + min/max; tiling off / 1m / 1m+5m; single WM; two-WM warm slide; short window all-raw |
| `tests/matrix.rs` `matrix_wro_points_match_oracle` | WRO vs same fixture: tiling off + 1m; points before / at / after `processed_pos`; exclude on/off |
| `tests/smoke.rs` | Basic WO / late drop / WO↔WRO smoke |
| `tests/harness.rs` | Shared WO harness + `WoWroHarness` (shared KV) |
| `state/tile/plan.rs` `plan_tests` | Geometry: coarse tiles, gaps, same-ts, wl ≪ gran, update runs |
| `evaluate/wo/plan.rs` `tests` | Cold closed/open bounds; plain gap-union emit ends; slide leave tiles / leave raw |
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

1. **Window store façade** above SortedKV (typed in-proc + batch EventLog) — documented, skip.
2. **WO retractable leave:** per-emit re-plan is intentional (see §6.13); optional precompute of per-emit leave coverages only if profiling shows plan_coverage cost.
3. Hop emit + tile-add path (add band still raw-only).
4. Plain per-emit full recompute: measure; prefer retractable+tiles over caching.
5. CDC / true late data.
6. Durable backends (Rocks/Slate/Scylla).
7. `drop_late_entries` → Arrow `gt` + `filter_record_batch`.

---

## 11. File cheat sheet

```text
config.rs
evaluate/
  mod.rs / output.rs
  primitives/{plan,load,coverage}.rs
  strategies/{rebuild,slide}.rs
  wo/{advance,plan,produce}.rs
  wro/{points,load}.rs

state/…  store/…  tests/…  README.md / HANDOFF.md
```

---

## 12. Suggested first prompt in a new chat

> Continue WO/WRO SortedKV work from `src/runtime/operators/window/HANDOFF.md`.  
> Don’t run tests unless I ask. Next: pick an item from §10 (impl follow-ups) or §9 stress testing.

Or pick another item from §10.
