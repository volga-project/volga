# Fault-Tolerance Follow-ups

## Checkpoint lifecycle

- [x] Master-driven, monotonic, single-in-flight checkpoint trigger (`force` RPC + interval from `runtime_consts`; `0s` interval = force-only).
- [x] Validate checkpoint acknowledgements against the exact expected task set.
- [x] Fail restoration when a checkpointable task has no checkpoint blobs.
- [x] Fence checkpoint triggers and reports with `execution_attempt_id`.
- [x] Prevent overlapping checkpoints (single-in-flight; timeout aborts and recovers).
- [x] Lifecycle events: `CheckpointStarted` / `CheckpointCompleted` / `CheckpointFailed`.
- [x] Push barrier-propagation reports (`BarrierInjected` / `Aligned`) — diagnosis only, not completion.
- [x] Harness e2e: indefinite replayable datagen + dedup sink + stop/stats (local + kube).

## Remaining

- Window operator checkpoint correctness (operator not CP-ready yet).
- Exactly-once sink commit protocols (Kafka transactions / lake table commits).
- Metrics for checkpoint in-flight age / ack lag.
- Use propagation events for timeout localization (which task is stuck).
- [x] Barrier-path test: Started → BarrierInjected → Aligned → Completed (local + kube ignore).
