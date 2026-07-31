# Window operators

This module implements keyed event-time `RANGE` windows in two roles:

- `WindowOperator` (WO) ingests rows, owns mutable state, advances event-time
  frontiers, and optionally emits streaming results.
- `WindowRequestOperator` (WRO) performs read-only point lookups against
  coherently published window data.

Both roles use the same logical raw rows and optional aggregate tiles through
separate store contracts. Raw rows are authoritative. Tiles only reduce raw I/O
and accumulator CPU work.

## Core model

State is isolated by `PartitionKey { namespace, business_key }`. The namespace
identifies one logical operator state space; the business key is the keyed
stream value.

Every accepted row receives a per-key sequence number and is identified by:

```text
Cursor { ts, seq_no }
```

Cursor order is event-time first and sequence number second. It provides stable
ordering and distinguishes rows with equal timestamps. Raw and tile reads use
half-open logical ranges:

- `RawRun [from, to)` uses cursors.
- `TileRun [start_ts, end_ts_exclusive)` addresses one tile granularity.

`KeyState` is the WO metadata for one partition:

- `next_seq`: dense per-key sequence allocator.
- optional evaluation state containing the last fired trigger and saved
  retractable accumulators. State-only WO does not maintain evaluation state.

The operator separately tracks the last fully processed watermark. It is the
late-data boundary and is included in the operator checkpoint.

## Configuration

`BuiltWindows` converts a DataFusion `BoundedWindowAggExec` into shared schemas
and one `WindowConfig` per expression. Each config records the expression,
accumulator capability, optional tiling, and WRO current-row behavior.

`WindowSpec` controls:

- `lateness`: retention padding behind the largest window.
- `tiling`: default tile granularities, with optional per-window overrides.

Window state advances only on watermarks. State-only request-serving topologies
still publish ingested raw rows and tiles immediately.

The implementation currently requires timestamp-millisecond `ORDER BY` columns
and `RANGE` frames.

## WO ingest

`WindowOperatorState::insert_batch` performs one partition update:

1. Load `KeyState`.
2. Drop rows whose timestamp is at or behind the task watermark.
3. Assign the internal `__seq_no` column to accepted rows.
4. Update `next_seq`.
5. Plan and load every tile bucket touched by the accepted rows.
6. Update each configured window's accumulator state in those tiles.
7. Atomically commit raw rows, updated tiles, metadata, and durable row
   triggers through `WindowOperatorStore::commit_events`.

The input batch must not contain `__seq_no`; WO owns that column. Late rows are
dropped relative to the task watermark, including rows for previously unseen
keys.

## WO advance and streaming evaluation

Emitting WO pages durable triggers through the requested watermark:

1. Load a backend-sized page of due triggers grouped by partition.
2. Use trigger cursors as exact emit points.
3. Build one `EvalPlan` per emitted result and window expression.
4. Merge emit-row and historical coverage, then load raw rows and tiles once.
5. Evaluate each window by sliding or rebuilding.
6. Save evaluation state through the final trigger.
7. Advance and forward the watermark after every due page succeeds.

State-only WO publishes raw rows and tiles on ingest but creates no row
triggers or evaluation state.

Retractable accumulators reuse their saved state. For each result they retract
the planned leave band, add the new end row, and evaluate. Tile-state
retraction is used only for aggregates whose state is component-wise
invertible; other leave bands use raw rows.

Plain accumulators rebuild each result from its exact `CoveragePlan`. A rebuild
merges complete tile states and evaluates raw edge rows. Raw-only windows are
represented by a coverage plan with no tile runs, not by a separate fallback
path.

## WRO point evaluation

WRO is deliberately rebuild-only and does not read WO's saved sliding
accumulator:

1. Convert each requested timestamp `T` to `Cursor(T, u64::MAX)`.
2. Build rebuild plans for every requested point and window.
3. Merge all raw and tile runs across those plans.
4. Call `WindowRequestStore::load_window_data` once for a coherent view.
5. Rebuild every result from the loaded rows and tiles.

By default, the request row's evaluated arguments are added after stored
coverage. `EXCLUDE CURRENT ROW` suppresses those request arguments; stored rows
through `T` remain part of the window.

## Tiling

`TileConfig` defines sorted, nested granularities. Each tile key is
`(granularity, tile_start)`, and its value contains accumulator states for all
configured windows.

Coverage planning chooses complete interior tiles and raw boundary segments.
It never uses a tile that extends outside the requested range. Missing tiles
mean that the corresponding interval contained no rows. Evaluation remains
correct when tiling is disabled because plans then cover the whole range with
raw rows.

WO ingest updates all configured granularities so later plans can choose the
best available coverage. Adjacent and overlapping load runs are merged before
store access.

## Store contracts

`WindowOperatorStore` is the sole-writer interface:

- load partition metadata and exact raw/tile runs;
- atomically commit ingest data, metadata, and triggers;
- page due triggers and publish evaluation state after advancement;
- flush, checkpoint, and restore a namespace.

`WindowRequestStore` exposes one operation that returns raw rows and tiles from
the same coherent snapshot. A backend must not combine raw rows from one
published state with tiles from another.

The contracts expose logical ranges only. Durable backends own physical
partitioning, publication, fencing, caching, and cleanup. They must preserve:

- one active WO writer per partition;
- atomic visibility of an ingest commit;
- coherent WRO reads;
- restorable checkpoint artifacts;
- ordered, collision-safe row identity.

`InMemWindowStore` is the reference backend. It keeps partition state in memory,
uses one read lock as the WRO snapshot boundary, and embeds complete namespace
snapshots, including durable triggers, in checkpoint data. Its live state
remains process-local and is not a cross-worker backend.

## Checkpoint and restore

`WindowOperatorState::checkpoint` asks the store to flush and checkpoint the
namespace, then returns `WindowStateSnapshot` containing:

- the namespace bytes;
- the last fully processed watermark;
- backend-specific `WindowBackendSnapshot`.

The operator serializes the snapshot as `SerializedCheckpoint`. The master
stores completed checkpoints without interpreting operator payloads. On
recovery, `RestorePlanner` creates `SerializedRestore` entries for the target
assignment and sends them with worker configuration. The operator deserializes
its own snapshot before restoring raw rows, tiles, and `KeyState`. The current
planner supports unchanged task identities; key-group redistribution remains
future work.

## Materialized evaluation data

`WindowData` owns raw batches and a shared `TileMap`. `for_window` projects the
relevant tile state and constructs a `RowNav`.

`RowNav`:

- globally sorts and deduplicates loaded rows by cursor;
- maps flat row indices back to Arrow batch rows;
- evaluates aggregate arguments once per batch;
- supports cursor seeks used by coverage evaluation.

This keeps store DTOs independent of a particular aggregate expression while
giving evaluators one ordered view.

## Module map

```text
operator.rs       WO runtime operator and watermark processing
request.rs        WRO runtime operator
state.rs          WO ingest state plus checkpoint/restore bridge
spec.rs           shared window runtime settings
config.rs         DataFusion expression-to-window configuration
model.rs          cursors, keys, metadata, runs, and tile models
tile.rs           tile configuration, planning, projection, and updates
eval/
  advance.rs      trigger-driven WO planning and advancement
  eval_plan.rs    per-result slide/rebuild plans
  coverage_plan.rs exact raw/tile geometry and run merging
  accumulate.rs   apply coverage to DataFusion accumulators
  slide.rs        retract/add evaluation
  rebuild.rs      tile/raw rebuild evaluation
  wro.rs          point-request planning and evaluation
  emit.rs         streaming emit cursor/input selection
  output.rs       result batch assembly
store/
  backend/mod.rs  WO/WRO contracts and snapshot model
  backend/inmem.rs reference in-memory implementation
  data.rs         WindowData, WindowView, and RowNav
aggs/             aggregate registry and accumulator-state operations
top/, cate/       custom aggregate implementations
tests/            WO/WRO semantics, tiling, planning, and matrix coverage
```

## Invariants to preserve

- WO is the only mutator for a partition.
- Cursor identity and ordering include both timestamp and sequence number.
- Rows at or behind the task watermark are not accepted by streaming ingest.
- Raw rows remain sufficient to produce a correct result.
- Tiles cover only complete aligned intervals selected by the plan.
- Sliding accumulator state corresponds exactly to its saved trigger cursor.
- WRO evaluates one coherent published snapshot and never depends on WO's
  private sliding accumulator.
- Retention publication is metadata; physical deletion is backend work.
- Physical backend details do not leak into operator-facing run or key models.
