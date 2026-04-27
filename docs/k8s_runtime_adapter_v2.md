# K8s Runtime Adapter V2 Notes

This document summarizes the V2 stack intent and current layering:

- `RuntimeAdapter` owns lifecycle (`start_attempt`, `stop_attempt`) for adapter-backed modes.
- Placement concerns live in `executor::placement` and expose a minimal strategy surface.
- Resource surface is intentionally minimal (`ResourceStrategy::PerWorker`).
- Bootstrap handoff is served by master via `GetWorkerBootstrap`.
- Runtime handoff is represented by `runtime::execution_plan::ExecutionPlan`.

## Current execution split

- In-process profile keeps direct worker execution.
- Adapter-backed profiles go through `PipelineContext` adapter helper path.
- Local adapter is the reference path for placement mapping and attempt lifecycle.

## Follow-up focus

- Complete K8s lifecycle internals behind existing adapter/module boundaries.
- Wire full worker bootstrap consumption into worker runtime startup path.
- Extend integration tests to cover K8s bootstrap and stop lifecycle end-to-end.
