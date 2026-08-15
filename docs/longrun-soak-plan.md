# Long-run soak (v1) and Flink bench (later)

Central planner for Volga soak + a later engine compare. Update this file as PRs land; do not dual-maintain a Cursor canvas.

**Base**: `master`  
**Metrics already on master**: stack #231 (PRs #223–#227; flake-fix #228)  
**Unrelated**: [#232](https://github.com/volga-project/volga/issues/232) split `stream_task.rs` — not this work  
**Tests**: `./scripts/test` (not raw `cargo test`)

| Cut | Goal | Not this cut |
| --- | --- | --- |
| **v1** (this file’s PR map) | Volga soak on kube: Steady + KillAfterCheckpoint, Grafana, dump, oracles | Flink, Kafka, EKS pool provisioning |
| **v2** | Fair Volga vs Flink on the same runner, Kafka bus, isolated worker pools | Nexmark, tumbling, RocksDB, custom UI |
| **Later** | Heavier workloads and a durable-state compare | See [Later / possible](#later--possible) |

Honest limit until a durable store lands: operator state is in-memory. v1/v2 numbers for lag / throughput / checkpoint time / restore catch-up are valid. **Not** a RocksDB compare.

Conventions from the metrics stack (keep):

- No WO metrics identity on `WindowOperatorState` (`task_id` on `OperatorTaskState`; labels on the store).
- Job checkpoint duration histogram is success-only.
- `TaskTimeMetrics` / `BackpressureTracker` (no “budget” wording).

---

## Status

| PR | Steps | Track | State |
| --- | --- | --- | --- |
| A `/metrics` | 1 | Engine | [#233](https://github.com/volga-project/volga/pull/233) |
| B Count sink | 2 | Engine | [#234](https://github.com/volga-project/volga/pull/234) |
| C CRD + Kind topology | 3 | Operator | [#235](https://github.com/volga-project/volga/pull/235) |
| D `test_utils` | 0 | Refactor | [#236](https://github.com/volga-project/volga/pull/236) |
| E `volga-longrun` + both scenarios | 4+5 | Longrun | [#237](https://github.com/volga-project/volga/pull/237) |
| F Prom + Grafana + dashboard JSON | 6 | Observability | [#238](https://github.com/volga-project/volga/pull/238) |
| G dump + oracles | 7 | Longrun | [#239](https://github.com/volga-project/volga/pull/239) |
| H operator-owned InMemory store | — | Operator | [#240](https://github.com/volga-project/volga/pull/240) |
| I `state.checkpoint` on `PipelineSpec` | — | Engine | follow-up (sibling of longrun; base master) |
| J soak `--config` YAML | — | Longrun | [#241](https://github.com/volga-project/volga/pull/241) |

A, B, C, H have no mutual dependency — land whenever. Stack longrun **D → E → G → J**. F needs A + C, not the binary. **I** is job API, not soak — do not stack under E.

Do not merge: D with B; B with A; G with E; **I with G**; **J with E**.

---

## v1 in / out

**In**

- Kubernetes: Kind (CI/dev) or existing real cluster (overnight). `--env kube` uses kubeconfig. Same CR, Prom, Grafana, oracles.
- Datagen (soak loop owns wall-clock duration; `run_for_s` unset so kill restore does not restart a full ingest window). `SinkSpec::Count`. Sliding RANGE window SQL on `PipelineSpec` (source → query → sink). Default: existing 10s checkpoint window SQL in `src/test_utils/checkpoint/launch.rs`.
- Scenarios on **the same window job**: Steady (hours, **30s** checkpoints — not `kube_test` 2s) and KillAfterCheckpoint.
- Live Grafana (board JSON does not exist yet — `kubevolga/hack/bench/`). Teardown `query_range` + oracle JSON.
- **Whole-codebase** `src/test_utils/` move: every shared test helper (not just soak). Harness + checkpoint waits always-compiled so `volga-longrun` can link them.
- Optional separate PR **H**: operator owns the InMemory gRPC store when the sink is InMemory; Count omits it.

**Out**

- `FlinkCluster`, Flink SQL/JAR, Flink dashboards, shared Kafka.
- Provisioning EKS NodePools (CR fields yes; create pools no).
- `EngineAdapter`, `RunSpec`, custom Volga UI, durable state backend.
- `pipeline_exec` / `word_count_benchmark`, docker-compose demo.
- Tumbling windows (roadmap). PassThrough soak job. Retain flag on the gRPC InMemory store.

**After v1**: [v2 — Flink bench](#v2--flink-bench). Not scheduled: [Later / possible](#later--possible).

---

## Reuse — do not add a second cluster API

```text
SoakSpec { env, launch, scenario, duration, oracles, dump }
        │
        ▼
PipelineLaunchSpec { pipeline: PipelineSpec, worker_count, expected_output_rows? }
        │
        ▼
VolgaCluster  (today TestCluster)
  ClusterBackend: Local | Docker | Kube
  FaultAction::KillWorker → abort process / kubectl delete pod
```

| Type | Owns | v1 | Flink later |
| --- | --- | --- | --- |
| `PipelineSpec` | SQL, Datagen, Count sink, parallelism | Already serializable; query lives here | Paired Flink SQL/JAR — not this struct |
| `PipelineLaunchSpec` | `PipelineSpec` + `worker_count` | `expected_output_rows` optional | Unused; FlinkCluster has its own submit |
| `SoakSpec` | What launch lacks | env, launch, scenario, duration, oracles, dump | Same scenario + oracles; different job ref |
| `VolgaCluster` | submit / wait / kill / snapshot / teardown | Rename `TestCluster` when promoting | `FlinkCluster` **sibling** — do not wrap |
| `FaultAction::KillWorker` | Kill one compute replica | Local abort / kube delete worker pod | Kube delete TM pod (same action, different target) |
| `RuntimeEnv` | Where it runs | Local \| Docker \| Kube | Kube, same Prom/Grafana namespace |

Kill is cluster-typed, not an engine-adapter method. The scenario says “kill after checkpoint N”; `VolgaCluster` maps that to a worker pod, `FlinkCluster` later to a TM.

Invocation (same cluster, different success criteria):

| | `scripts/test kube` | `volga-longrun soak --env kube` |
| --- | --- | --- |
| Length | seconds | hours |
| Sink | InMemory (operator-owned store after **H**) | Count (no store pod) |
| Pass | `OutputOracle` / sink equality | Prom oracles fail the process |
| Runner | nextest timeout | CI job / laptop binary |

---

## Environment

| Env | Use for | Skip for |
| --- | --- | --- |
| Local `VolgaCluster` | Iterate oracles; minutes | Published numbers; multi-hour soak |
| Kind | CI soak, scrape wiring, prove placement YAML | Published numbers (shared host CPU) |
| Existing real cluster | v1 overnight soak; `--env kube` + that kubeconfig | Provisioning new node pools (after v1) |

If the cluster already has `infra` / `volga-worker` labels and taints, the CR uses them. If not, the job still runs without isolation. Fair Flink compare later = same worker hardware, sequential jobs.

---

## File layout

Single Cargo package (no workspace split in v1).

### `src/test_utils/` — whole-codebase refactor (PR D)

Not soak-only. Collapse every shared test helper into one folder so production modules do not carry `test_utils.rs` / harness files beside them. Soak is one consumer: it also needs `harness/` + `checkpoint/` **always compiled** (`volga-longrun` is not `cfg(test)`).

**Move in (all of these, not a subset):**

| Today | After |
| --- | --- |
| `src/common/test_utils.rs` | `src/test_utils/common.rs` (`cfg(test)`) |
| `src/transport/test_utils.rs` | `src/test_utils/transport.rs` (`cfg(test)`) |
| `src/runtime/operators/window/aggs/test_utils.rs` | `src/test_utils/window.rs` (`cfg(test)`) |
| `src/runtime/operators/window/tests/harness.rs` | `src/test_utils/window/harness.rs` (`cfg(test)`) |
| `src/tests/support/test_utils.rs` | `src/test_utils/` (merge; do not leave a second `test_utils.rs`) |
| `src/tests/support/cluster_harness/` | `src/test_utils/harness/` (**always**; rename `TestCluster` → `VolgaCluster`) |
| `src/tests/support/checkpoint/` | `src/test_utils/checkpoint/` (wait helpers **always**; finite e2e entry points `cfg(test)`) |
| `src/tests/support/{launch_specs,smoke,recovery,parquet,pipeline_exec,many_to_many_harness}.rs` | `src/test_utils/` matching names (`cfg(test)` except what soak must link) |

**Stay put:**

| Path | Why |
| --- | --- |
| `src/tests/{inprocess,docker,kube,benchmark}/` | Actual `#[tokio::test]` files (including word-count benches) — they *use* `test_utils`, they are not utilities |
| `mod tests { ... }` inside production `.rs` files | Inline unit tests; not shared helpers |

Mechanical import rewrite. No behavior change except visibility (`VolgaCluster` available to the longrun binary). Split D into 0a (moves) / 0b (always-compile + rename) only if the diff is too large to review.

### Longrun (new)

| Path | Compile | Owns |
| --- | --- | --- |
| `src/longrun/` | always | `SoakSpec`, Steady, KillAfterCheckpoint, Prom observer/oracles. **Not** a cluster. |
| `src/longrun/flink.rs` | always | Empty `FlinkCluster` stub + comment |
| `src/bin/volga-longrun.rs` | always | `volga-longrun soak \| bench`. v1 implements soak only. |
| `kubevolga/hack/bench/` | manifests | Prom + Grafana YAML, Kind labels/taints, Grafana dashboard JSON |

---

## Job and sink

Both scenarios use the **same sliding RANGE** window (not PassThrough, not tumbling). SQL is `PipelineSpec.sql` against Datagen schema `(timestamp, key, value)`. Window length lives in the SQL (`INTERVAL '10' SECOND` vs `'1' HOUR`). Named presets are just default SQL strings.

**Count sink (v1):** new `SinkSpec::Count` + `CountSinkFunction` — increment `volga_sink_records_written`, drop the batch. Do **not** spawn `volga-test-storage`. Do **not** modify the gRPC InMemory store. Kube/Local backends start storage **only** when the spec sink is InMemory.

**Kafka (after v1):** shared producer; both engines read the same topic.

| Scenario | Reuse | Do not reuse |
| --- | --- | --- |
| Steady | `VolgaCluster` + Datagen `run_for_s`; prod-like 30s CP interval | `OutputOracle`, InMemory retain-all, `wait_for_completion` as success, `kube_test` 2s interval |
| KillAfterCheckpoint | `wait_for_checkpoint_completed`, `wait_for_kill_restore`, `WorkerKillMode::Abrupt` | `run_checkpoint_worker_kill_recovery` entry (finite + `assert_sink_matches_offline_datagen`) |

---

## Placement

`VolgaPodSpec` today is only `resources` + `imagePullPolicy`. Add fields; do **not** embed a full `corev1.PodSpec`. Controller copies them onto master Pod + worker StatefulSet. `DeepCopyInto` currently only clones `Resources` — it must copy the new fields.

| Field | Type | Use |
| --- | --- | --- |
| `nodeSelector` | `map[string]string` | `volga.io/role=worker` vs infra |
| `tolerations` | `[]corev1.Toleration` | Match role taint — selector alone is not isolation |
| `affinity` | `*corev1.Affinity` | Required `podAntiAffinity` hostname = one worker per node |
| metrics port | `9090` | ContainerPort + Service + `prometheus.io` scrape annotations |

### Kind topology (logical isolation; CPU still shared)

Today `kubevolga/hack/kind-multi.yaml` is 1 CP + 2 unlabeled workers.

| Node | Runs | Taint / label |
| --- | --- | --- |
| control-plane | kube system | untouched |
| infra | Prom, Grafana, operator, master | `volga.io/role=infra`, no worker taint |
| volga-worker × N | Volga workers only | taint `NoSchedule` + selector |

CI can prove the contract with N=1. Do not size Kind to EKS. Prom must land on infra nodes.

---

## Dashboard

**Not in the repo yet.** Path: `kubevolga/hack/bench/` (Grafana JSON + Prom YAML), provisioned into in-cluster Grafana on infra. Not a Volga product UI.

Blocked on HTTP `/metrics`: `prometheus_handle` is in-process only; `VOLGA_ENABLE_TCP_METRICS=1` binds `127.0.0.1:9999` — not kube-scrapeable. Scrape ~5s for 1s busy/idle/lag ticks. Lightweight Prom + Grafana Deployments (not kube-prometheus-stack).

Grafana scrapes Prom, not `PipelineSnapshot`. Snapshot gRPC stays for typed soak oracles. Teardown dump = `query_range` of the board series (not TSDB snapshot unless Prom admin API is already on).

| Row | Prom series |
| --- | --- |
| Job health | `volga_checkpoint_completed` / `_failed`, `volga_checkpoint_duration_ms` (success-only histogram) |
| Task time | `busy` / `idle` / `backpressured_time_ms_per_second` (stack ≈ 1000) |
| Event-time | `volga_stream_task_watermark_lag_ms` p50/p99 |
| Throughput | `rate(volga_stream_task_records_sent)`, `volga_sink_records_written` |
| Window (hide if no series) | `volga_wo_ingest_ms`, `late_dropped_rows`, `maintain_pruned_rows`, `state_*_bytes` |

---

## Oracles (fail the process)

Defaults are Kind-calibrated; override on `SoakSpec` for a real cluster. Lag bound scales with window length in SQL.

| Check | Default fail if | Notes |
| --- | --- | --- |
| WM advancing | lag unchanged ≥ 30s while `records_sent` still increasing | Skip after Datagen `run_for_s` ends |
| Lag p99 | p99 > window RANGE + event-time OOO (v1: 10s + 0) for a sustained 60s | Tighten on a real cluster |
| Checkpoint health | no completed cp in 3× interval, or `failed` increase > 0 outside kill window | 30s interval; exclude ±timeout around injected kill |
| No unexpected fatal | pod `restartCount` or `WorkerPanic` / `HeartbeatUnavailable` outside KillAfterCheckpoint | `LifecycleEvent` + kube restart counts |
| Restore catch-up | `restore_checkpoint_id` below killed-after id, or lag not under bound within 60s | `wait_for_kill_restore` + lag Prom query; not sink row equality |

---

## Work steps (logical)

| Step | Work | Unlocks |
| --- | --- | --- |
| 0 | Whole-codebase `src/test_utils/`: move **all** shared helpers out of `common/` / `transport/` / `window/` / `tests/support/`. Promote harness + checkpoint waits to always-compiled (`TestCluster` → `VolgaCluster`). `src/tests/` is `#[tokio::test]` only | One import root for tests; `volga-longrun` can link the cluster |
| 1 | HTTP `/metrics` on worker + master (9090); controller port + Service + scrape annotations | Prom can scrape |
| 2 | `SinkSpec::Count` + `CountSinkFunction`. Storage pod only for InMemory. `expected_output_rows` optional | Hours of Datagen without OOM |
| 3 | CRD placement fields + `DeepCopyInto`. Kind infra + tainted volga-worker nodes | Same Pod API on Kind and real kube; Prom on infra |
| 4 | `src/longrun/` + `volga-longrun.rs`: `SoakSpec`. `flink.rs` stub. No `EngineAdapter` / `RunSpec` | `volga-longrun soak --env local\|kube` |
| 5 | Steady + KillAfterCheckpoint on the same sliding RANGE job | Long run including cp/restore |
| 6 | Prom + Grafana on infra. Dashboard JSON in `kubevolga/hack/bench/` | Live board |
| 7 | `query_range` dump of board series + oracles that fail the process | Archive + CI-failing sanity |

---

## PR map (not 1:1 with steps)

| PR | Steps | Track | Depends on |
| --- | --- | --- | --- |
| **A** `/metrics` | 1 | Engine | None. Rust HTTP + Go controller in the **same** PR or kube cannot scrape. |
| **B** Count sink | 2 | Engine | None. Do not fold into InMemory. |
| **C** CRD + Kind topology | 3 | Operator | None. YAML useless without CR fields. |
| **D** `test_utils` (whole codebase) | 0 | Refactor | None. Not soak-scoped: move every shared helper under `src/test_utils/`. Split 0a moves / 0b always-compile only if the diff is huge. |
| **E** `volga-longrun` + both scenarios | 4+5 | Longrun | **D** (must link `VolgaCluster`). **B** if the launch spec uses Count. |
| **F** Prom + Grafana + dashboard JSON | 6 | Observability | **A** (scrape) + **C** (infra placement). Not the binary. |
| **G** dump + oracles | 7 | Longrun | **E** (run loop). Ideally **F** (same series as the board). Derives waits / silence / lag from **consts** + the soak window until **I** lands. |
| **H** operator-owned InMemory store | — | Operator | None. Nice-to-have before **E** so kube tests stop `kubectl apply` of `volga-test-storage`. Not required to start A–D. |
| **I** `state.checkpoint` on `PipelineSpec` | — | Engine | None (base master / current integration). `interval` / `timeout` / `retention`. Master reads spec first, consts as fallback. Heartbeats / RPC / kube health stay on consts. Not soak. |
| **J** soak `--config` YAML | — | Longrun | **G**. `env`, duration, scenario, launch, Prom/dump, oracle multipliers. CLI overrides the file. After **I**, oracles read `launch.pipeline.state.checkpoint` and allow `3x` / `1.5x_window`. |

A binary without Steady/Kill is not worth its own PR (4+5 stay together).

### Config layers

| Layer | Owns | Who reads it |
| --- | --- | --- |
| Runtime consts (`RuntimeConstsProfile::Prod`) | Cluster-wide defaults: checkpoint interval/timeout, heartbeats, RPC, kube health | Master today; **G** derives soak waits / silence / kill grace from these |
| `PipelineSpec.state.checkpoint` (**I**) | Per-job interval / timeout / retention | Master (spec first, consts fallback). Soak oracles switch here after **I** |
| Soak/bench run YAML + `--config` (**J**) | How this *run* is invoked: env, duration, scenario, dump, Prom, oracle multipliers | `volga-longrun` only. Does not move engine knobs |

**G** does not take `--config` and does not add `StateSpec` checkpoint fields.

```
master ──► I checkpoint-on-PipelineSpec     (engine, independent)
D #236 → E #237 → G #239 (dump + oracles, derive from consts)
                    └──► J soak --config YAML  (longrun only)
                           └──► oracles read spec.checkpoint when I is in
```

---

## Operator-owned InMemory store (PR H, optional vs soak)

Today kube e2e `kubectl apply`s a **singleton** Deployment+Service `volga-test-storage` (`kubevolga/config/test-storage/`), hardcodes `http://volga-test-storage.default.svc.cluster.local:50071` into the sink, port-forwards it, and deletes it on teardown. That lifecycle is outside the CR: deleting the pipeline does not delete storage, and tests share one store (reset / wait-empty).

**Do this:** the operator creates an owned Deployment+Service when the pipeline sink is `InMemoryStorageGrpc` (empty `server_addr`). OwnerRef = the `VolgaPipeline` → GC with the CR. Inject DNS the same way as `MASTER_SERVICE_ADDR` (or put `storageServiceAddr` on status for test port-forward). Per-pipeline store, not a cluster singleton.

**Do not** add `spec.testStorage: bool`. The sink kind is the switch — a flag can disagree with `PipelineSpec.sink`. Longrun uses `SinkSpec::Count` → operator creates nothing. Standalone kube jobs that want a capture store set InMemory; production/Count/Kafka/Parquet get no extra pod.

Keep a Deployment, not a sidecar on master (independent restart, port-forward, same image `command: ["volga-test-storage"]`). Schedule it on **infra** (same placement as master) once PR C taints workers. Local/Docker still spawn storage in-process / compose — unchanged.

KubeCluster then only applies the CR; no `start_storage()`, no rewrite-every-sink-to-InMemory. That rewrite would also break Count soak if H has not landed — minimum for **E** is “leave a non-InMemory sink alone.”

---

## v2 — Flink bench

Same `volga-longrun` binary (`bench` subcommand). Same `SoakSpec` scenario timeline and Prom oracles. A second engine on the **same kube + Kafka + Prom** fabric — not a second harness, not `EngineAdapter` wrapping `VolgaCluster`.

v1 already leaves: `src/longrun/flink.rs` stub, CR placement fields, infra Prom/Grafana, Count sink (Volga-only soak stays Datagen; bench switches IO to Kafka).

### Why Kafka (and not Datagen)

Engine-internal generators are not the same workload. Fairness is **same bytes in**.

```text
external producer  →  Kafka topic (shared)
                         ├─ Volga Kafka source  →  query  →  Kafka sink topic A
                         └─ Flink Kafka source  →  same SQL/JAR  →  Kafka sink topic B
```

- Volga already has `KafkaSourceSpec` (compiler + docker tests). v2 adds `KafkaSinkSpec` (does not exist today).
- Producer is outside both engines (replayable Datagen-to-Kafka or Nexmark-style later).
- Distinct sink topics so counts do not collide. Optional sampled checksums on the sink topics; not `OutputOracle` row equality.
- Checkpoint interval aligned in `PipelineSpec` / Flink conf (v1 soak already uses prod-like 30s).

Datagen + Count remains the Volga-only soak path. Do not use it for published Flink numbers.

### `FlinkCluster` sibling

| | Volga | Flink |
| --- | --- | --- |
| Submit | `VolgaPipeline` CR via `VolgaCluster` | Flink K8s Operator `SessionJob` / application job |
| Kill | `FaultAction::KillWorker` → delete worker pod | Same action → delete **TaskManager** pod |
| Snapshot | `PipelineSnapshot` + Prom | Prom only (skip snapshot) |
| Oracles | Prom queries + optional snapshot | **Same Prom queries**; skip snapshot |
| Dashboard | Volga folder (v1) | Extra Flink folder / analogue panels |

`FlinkCluster` implements launch / wait / `apply_fault` / teardown. It does **not** implement `storage_snapshot` or Volga lifecycle gRPC. Do not force it onto today’s `ClusterBackend` trait as-is.

Job graph: one SQL subset compiled twice, or a paired Flink SQL/JAR. Start with the same sliding RANGE window as v1 soak (not word-count in-process, not tumbling until that exists in Volga).

### Placement and fairness

v1 CR fields are the Pod API. v2 **provisions** pools (Karpenter NodePool or EKS MNG) with the same labels/taints:

| Role | Runs | Notes |
| --- | --- | --- |
| `infra` | Prom, Grafana, Kafka, operators, master / JM | No worker taint |
| `volga-worker` | Volga workers only | Taint + selector; `requests == limits`; one pod per node via anti-affinity |
| `flink-worker` | Flink TMs only | Same hardware class as `volga-worker` |

**Head-to-head number:** same worker hardware, **sequential** jobs (run Volga, then Flink, or the reverse). Two pools in parallel is only for simultaneous soaks, not a fair compare. Kind remains logical isolation (shared CPU) — **publish from a real cluster**.

Prom stays on infra and scrapes both engines (~5s). Overnight: optional Prom `remote_write` to something that outlives the job (Mimir / VictoriaMetrics / Grafana Cloud); still keep a `query_range` artifact so a run is reproducible without that SaaS.

### Grafana analogues (v2 adds Flink panels)

| Row | Volga | Flink |
| --- | --- | --- |
| Job health | `volga_checkpoint_*` | `numberOfCompletedCheckpoints`, `lastCheckpointDuration` |
| Task time | busy / idle / bp ms per second | `busyTimeMsPerSecond`, `idleTimeMsPerSecond`, `backPressuredTimeMsPerSecond` |
| Event-time | `watermark_lag_ms` | `currentInputWatermark` / `outputWatermark` vs wall clock |
| Queues | path latency, tx queue, bp ratio | `inPoolUsage`, `outPoolUsage` |
| Window | WO ingest / late / pruned / state sizes | `numLateRecordsDropped`, `checkpointedSize` |

### v2 work (after G; not scheduled)

Order of magnitude only — split into PRs when v1 has landed.

1. Kafka sink + in-cluster Kafka on infra + external producer helper.
2. EKS (or other) NodePools per role; Kind `flink-worker` node only to prove YAML.
3. `FlinkCluster`: submit SessionJob, wait, kill TM, teardown. Fill `src/longrun/flink.rs`.
4. `volga-longrun bench --engine volga|flink` with the same `SoakSpec` scenario/oracles (Prom only on Flink).
5. Flink Grafana folder; dump both engines’ series at teardown.

Still no `EngineAdapter` wrapping `VolgaCluster`. Still no in-process word-count as the Flink peer.

---

## Later / possible

Not v1, not required for a first Flink number. Do not start these in the v1 stack.

| Item | Why it waits | Depends on |
| --- | --- | --- |
| Nexmark (or similar) query set | Need Kafka bus + paired Flink SQL first; v2 can start with the soak window query | v2 |
| Tumbling windows | Volga is continuous sliding RANGE today; tumbling is on the roadmap | Window work, not this runner |
| Durable state backend / RocksDB compare | In-memory state makes “backend vs RocksDB” meaningless | Engine state store |
| Prom remote_write / long-lived TSDB | Kind is ephemeral; real-cluster overnight may want Grafana Cloud / Mimir | v1 dump is enough for CI |
| Extra fault scenarios (mid-flight kill, sequential multi-fail) | Helpers exist in kube checkpoint tests; soak v1 is one kill after a completed CP | v1 KillAfterCheckpoint |
| Request / WRO path in soak | Different execution mode than streaming window soak | v1 streaming path |
| Custom Volga UI | Grafana is the bench UI | Never required for soak/bench |
| Parallel two-pool simultaneous soaks | Useful for capacity, **not** a fair engine compare | v2 pools |
| [#232](https://github.com/volga-project/volga/issues/232) split `StreamTask` | Overloaded file; unrelated to soak | Own PR |

---

## Do not reuse as-is

- `pipeline_exec` / `word_count_benchmark` — SingleWorker in-process; not what you compare to kube Flink.
- `OutputOracle` — exact finite-row equality; soak needs invariants.
- InMemory gRPC sink for hours — holds every row; kube e2e only.
- `run_checkpoint_worker_kill_recovery` as the soak entry — finite job + `assert_sink_matches_offline_datagen`. Reuse wait/kill helpers only.
- docker-compose demo — no scrape, no placement, no Flink.
