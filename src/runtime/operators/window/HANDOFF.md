# Window store handoff

The current architecture and Scylla execution plan are in
[`STORE_DESIGN.md`](STORE_DESIGN.md). [`README.md`](README.md) summarizes runtime
behavior and invariants.

## Current implementation

- Generic `SortedKV`, key codecs, and envelope estimation were removed.
- `WindowOperatorStore` serves exclusive WO reads and writes.
- `WindowRequestStore` provides coherent WRO reads.
- `InMemWindowStore` implements both contracts.
- WO loads meta before planning exact data ranges.
- WRO is rebuild-only and merges exact plans before one logical snapshot read.
- Raw store values are `RecordBatch`; cursor navigation is built inside `RowNav`.
- Retention is published through `KeyState.retention_floor` and cleaned outside
  the WO advance path.
- `InMemWindowStore` is process-local. WO and WRO must be colocated while it is
  used; cross-worker lookups require the shared Scylla backend.
- Window checkpoints persist only the namespace; raw, tile, and key state
  durability is the backend's responsibility.

## Remaining backend work

1. Implement the Scylla schemas and MVCC publication protocol.
2. Add WO fencing and failure recovery.
3. Add physical time-bucket mapping.
4. Add Foyer materialized-bucket caching.
5. Add asynchronous retention, old-version, and orphan cleanup.

Do not expose physical bucket IDs or MVCC epochs through the operator-facing
store interfaces.
