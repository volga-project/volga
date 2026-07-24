# SortedKV

Forward-only byte KV: `get` / `scan` / `put` / `delete` / `write(WriteBatch)` /
`snapshot_partition` / `flush`.

## Contract

| Concern | API |
|---------|-----|
| Atomic multi-key write | `write(WriteBatch)` |
| Consistent multi-op read (one business key) | `snapshot_partition(partition_key)` → [`KvSnapshot`] |

`partition_key` is an **opaque co-location id** (window: `{ns}/{key_hash}`), not a
SortedKV scan prefix. KV keys are kind-first (`raw|tile|meta/...`); co-location is
`(ns, hash)` shared across kinds.

| Backend | `write` | `snapshot_partition` |
|---------|---------|----------------------|
| **InMem** | One lock | Full-map epoch (`partition_key` unused) |
| **RocksDB** | `WriteBatch` | `GetSnapshot` (may ignore id) |
| **SlateDB** | Atomic batch | `db.snapshot()` (may ignore id) |
| **Scylla** | Logged batch, one PK | One `SELECT` for partition ← `partition_key` |

Window operators do not carry generation/retry tokens.

Window layout / ingest:

→ [`runtime/operators/window/README.md`](../../runtime/operators/window/README.md)

This directory: [`trait.rs`](trait.rs), [`in_mem.rs`](in_mem.rs).
