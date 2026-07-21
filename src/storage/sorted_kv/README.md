# SortedKV

Forward-only byte KV: `get` / `scan` / `put` / `delete` / `write(WriteBatch)` / `flush`.

Window ingest, atomicity, backend matrix (InMem / Rocks / Slate / Scylla), and roadmap:

→ [`runtime/operators/window/README.md`](../../runtime/operators/window/README.md)

This directory: [`trait.rs`](trait.rs), [`in_mem.rs`](in_mem.rs).
