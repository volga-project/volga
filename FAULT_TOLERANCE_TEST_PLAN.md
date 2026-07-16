# Fault-Tolerance Test Plan

Checkpoint work is last. Finish-versus-failure races are deferred.

## 1. Stabilize the harness foundation

- Compile and run the current local assignment and crash-recovery tests.
- Keep `master_worker_execution_test.rs` as backward-compatible coverage.
- Keep automatic replacement as the only restart path in the local crash-recovery test.
- Rename the remaining physical `resources/` files behind the `environments/` module when convenient.

## 2. Complete local recovery lifecycle coverage

Use `TestCluster`, lifecycle events, worker handles, and storage snapshots.

- Worker-control-server crash:
  - `AttemptRunning(0)`
  - target `WorkerFailure`
  - replacement request
  - replacement registration
  - `AttemptRunning(1)`
  - pipeline finish

- Multi-worker isolation:
  - run at least three workers;
  - kill one worker;
  - assert only the target worker fails and is replaced;
  - assert remaining workers do not fail;
  - assert recovered output is valid.

- Recovery-budget exhaustion:
  - repeatedly fail replacement workers;
  - assert bounded `PipelineFailed`;
  - move the recovery budget from a hard-coded lifecycle constant to test-configurable runtime settings first.

- Replacement unavailable:
  - add local replacement modes such as `Unavailable` and `FailNextReplacement`;
  - assert recovery fails or retries according to the intended policy, never by hanging.

- Scheduling/opening failure:
  - add a worker-server test hook that rejects configure/start;
  - assert the attempt does not reach `AttemptRunning`;
  - assert a recoverable scheduling path reaches the next attempt.

## 3. Add worker-health fault coverage

- Add a test-only `ReportWorkerFatal { worker_id, reason }` fault action.
- Trigger it for one worker in a multi-worker pipeline.
- Assert only that worker fails, recovery starts, and peers remain unaffected.

## 4. Add attempt-fencing tests

Use focused WorkerServer/master integration tests where possible.

- Reject old-attempt configure/start/run/close requests after attempt `N + 1` starts.
- Ensure an old heartbeat cannot fail the active attempt.
- Define and test duplicate/reordered control-request behavior.
- Add late-checkpoint-report rejection after checkpoint reports carry an attempt ID.

Use exact attempt IDs and lifecycle cursors; do not use sleeps as correctness signals.

## 5. Add network-fault infrastructure and scenarios

- Add control-link fault actions for transient errors, delays, and sustained partitions.
- Use a real gRPC proxy such as Toxiproxy for process-level faults.
- Test a short retryable control blip: pipeline completes without required recovery.
- Test a sustained heartbeat/control partition: targeted failure, replacement, next attempt.
- Test worker-to-worker transport disconnect recovery and output correctness.
- Make heartbeat, retry, and close timeouts test-configurable before adding these tests.

## 6. Confirm deployment behavior

Keep deployment tests small and CI-tiered.

- Docker worker-process kill, replacement, and successful next attempt.
- Kubernetes worker-pod deletion, replacement, registration, and successful completion.
- One sustained control-link fault in Docker and Kubernetes after proxy injection exists.

## 7. Checkpoint correctness and recovery

Product v1 + harness e2e are implemented (see `FAULT_TOLERANCE_TODO.md` and
`src/runtime/tests/checkpoint_tests/`). Remaining cases:

- partial checkpoint followed by crash is ignored;
- duplicate/unexpected acknowledgements never complete a checkpoint (unit-covered; harness case optional);
- stale attempt reports are rejected (fenced; harness case optional);
- missing checkpoint state fails recovery safely (restore path fails; harness case optional);
- window-operator restore correctness once window CP is ready;
- Kafka source harness e2e.

## Deferred

- Finish-versus-failure race tests.
- Exactly-once output assertions until source progress, checkpoint completion, and sink commit are transactional.
