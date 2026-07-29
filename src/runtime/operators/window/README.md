# Window operators

The window stack implements RANGE windows through two roles:

- `WindowOperator` owns ingest and watermark advancement for each key.
- `WindowRequestOperator` performs read-only point lookups.

Both use the domain store described in [`STORE_DESIGN.md`](STORE_DESIGN.md).

## Data flow

```text
WO ingest
  meta → dirty tiles → commit events + tiles + meta

WO advance
  meta → exact raw/tile runs → slide or rebuild → store meta

WRO request
  exact rebuild plans → merge runs → coherent snapshot read → rebuild
```

Envelope estimation and generic byte-key storage are not part of the design.
`RawRun` and `TileRun` are logical; physical bucketing belongs to durable
backends.

## Layout

```text
eval/       advance, slide, rebuild, WRO points, output
state/      WO state and tile planning/update
store/      contracts, in-memory backend, WindowData, RowNav
aggregates/ DataFusion accumulator integration
top/, cate/ custom aggregates
```

## Invariants

- One fenced WO owns a partition.
- `Cursor { ts, seq_no }` is the raw event identity and ordering.
- Streaming ingest drops rows at or behind `processed_pos`.
- Raw rows are the source of truth; tiles are acceleration.
- Missing tiles represent empty intervals.
- WRO is rebuild-only and never reads WO accumulator state.
- Store-returned raw batches are globally cursor ordered and deduplicated.
- Retention is published in meta and cleaned asynchronously by the backend.

## Backends

`InMemWindowStore` is the reference implementation and uses one lock for a
coherent WRO read. The planned Scylla backend uses physical time buckets,
per-key MVCC publication, and Foyer caching; see [`STORE_DESIGN.md`](STORE_DESIGN.md).
