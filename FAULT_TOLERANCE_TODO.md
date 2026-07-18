# Fault-Tolerance Follow-ups

## Checkpoint lifecycle

- [x] Master-driven, monotonic, single-in-flight checkpoint trigger (interval from `runtime_consts`; `0s` disables).
- [x] Validate checkpoint acknowledgements against the exact expected task set.
- [x] Fail restoration when a checkpointable task has no checkpoint blobs.
- [x] Fence checkpoint triggers and reports with `execution_attempt_id`.
- [x] Prevent overlapping checkpoints (single-in-flight; timeout aborts and recovers).
- [x] Lifecycle events: `CheckpointStarted` / `CheckpointCompleted` / `CheckpointFailed`.
- [x] Push barrier-propagation reports (`BarrierInjected` / `Aligned`) — diagnosis only, not completion.
- [x] Harness e2e: finite datagen + interval CP + keyed upsert sink; oracle via task metadata `records_generated` + offline datagen set equality.

## Remaining

- Window operator checkpoint correctness (operator not CP-ready yet).
- Exactly-once sink commit protocols (Kafka transactions / lake table commits).
- Metrics for checkpoint in-flight age / ack lag.
- Use propagation events for timeout localization (which task is stuck).
- [x] Barrier-path test: Started + BarrierInjected before Completed; Aligned observed (diagnosis-only, order vs Completed free; local + kube ignore).
- [x] Generic source interrupt (`SourceInterrupt` sleep/race + `FetchResult::Interrupted`); registered for checkpointable sources; datagen + Kafka wired.
- [x] Shared source emit stats (`SourceStats.records_generated`) owned by registry / incremented by `SourceOperator` for all sources.
