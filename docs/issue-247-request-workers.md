# Issue 247: Request workers

Follow this file. Stacked PRs on `feat/issue-247-request-workers` in `/Users/anov/IdeaProjects/volga_issue_247` (clean `origin/master`, do not touch other worktrees).

Related: [#247](https://github.com/volga-project/volga/issues/247), [#301](https://github.com/volga-project/volga/issues/301), [#291](https://github.com/volga-project/volga/issues/291). [#117](https://github.com/volga-project/volga/issues/117) is closed as irrelevant.

## Can 1–4 land before Scylla?

**Yes.** PR1–PR4 are engine / worker / metrics / deploy plumbing. They do not need a working `WindowRequestStore`.

**PR5 waits on Scylla (#291).** That is the first time a request worker in another process can read a published cut. Until then:

- Do not claim independent serving.
- Do not add a collocated production mode to paper over the missing store.
- In-process tests may inject a shared InMem store into a streaming worker **and** a request executor in the same test process. That is a test seam, not a Worker class that runs both graphs.

`open_window_request_store` is still `match *config {}` over an uninhabited `RequestStoreConfig`. WRO `open()` requires it. So a deployed request-mode pipeline that includes WRO cannot serve until #291. HTTP-only chains (source → map → sink) can.

## Target architecture

Two disjoint graphs, two process types, one store.

```
ingest → … → WO (StateOnly) → (no downstream)
                 │
                 │  store (namespace = pipeline_id + owner operator_id)
                 ▼
HTTP → keyby → WRO → projection… → HTTP response
```

- **Streaming workers** own the write path. Assignment, attempts, checkpoints, recovery stay as they are, on the streaming graph only.
- **Request workers** are stateless replicas of the **whole** request chain. Any replica can answer any key. No key-group range, no attempt, no fencing, no checkpoint, no cross-worker shuffle.
- WO and WRO already talk only through the store. This issue is placement and runtime shape, not a new protocol.
- **No collocation.** One `Worker` does not bind HTTP because it received a request-source vertex. Collocation as a later optimization is a separate issue; do not add a shim.

### Request worker: not StreamTask

Today the request path is shoehorned into the streaming runtime:

- `to_request_mode` copies WO parallelism onto every request vertex and leaves them in the **same** `LogicalGraph`.
- One `Worker` starts N `StreamTask`s plus a bolted-on `RequestSourceProcessor` (HTTP + `pending_requests` + mpsc + oneshot).
- Source tasks compete on a **shared `Mutex<Receiver>`** (ingest is effectively serialized).
- `RequestRoutePartition` + `_source_task_index` send the response back to the matching sink task.
- The processor **accumulates** sink batches until `expected_record_count` because one HTTP request was fanned out across parallel tasks.
- `stop_request_source_processor_if_needed` exists and is never called.

That machinery exists to fake dataflow parallelism for a stateless RPC. Drop it.

**Per request = one tokio task running the whole operator chain.** No `StreamTask`, no transport channels, no `task_index`, no source/sink pair, no keyed input/output map.

```
HTTP handler
  acquire semaphore (max_pending)
  RecordBatch = json_to_batch(body)
  for op in chain { batch = op.process(batch).await }   // same tokio task
  return json
on timeout / error: release semaphore, fail the HTTP request
```

Shared on the worker (not per request):

- HTTP server
- `Semaphore` (`max_pending_requests`)
- `WindowRequestStore` client (session given at configure; #291)
- operator chain template (stateless; WRO holds the store handle)

Not shared:

- No `pending_requests` DashMap. The HTTP handler `.await`s the chain; the oneshot **is** the task stack.
- No `RequestRoutePartition`, no `_source_task_index`, no `_source_request_id` unless a request-id is useful for logs/metrics (generate it in the handler, do not route with it).
- No per-operator tokio runtimes.

Worker-local concurrency is the semaphore (and the tokio runtime), **not** graph `parallelism`. Horizontal scale is replica count behind a Service / load balancer.

A bounded worker-pool (`mpsc` of requests → N tasks) is equivalent; prefer spawn-per-request + semaphore unless we measure otherwise.

### Lifecycle and master

Request workers **do** talk to the master. They are part of the job. They do **not** join the streaming attempt.

| Event | Streaming worker | Request worker |
| --- | --- | --- |
| Register | yes | yes (role = request) |
| Configure | vertex slice + attempt + restore | full request chain + store session + bind; **no attempt** |
| Start | tasks + transport | HTTP listen |
| Checkpoint / barrier | yes | **never** |
| Streaming `recover()` | reset + new attempt | **leave running** |
| Request pod death | unchanged | kube/local replaces pod; new process registers + configure; streaming attempt unchanged |
| Pipeline stop / delete | close | close HTTP, unregister |

**No generation / configure epoch.** A request worker is configured once per process lifetime. Replacement is a new process. Streaming recovery must not `ResetWorker` the request pool. Spec changes are a job replace (new pods), not a live reconfigure. The earlier “generation for which configure is live” idea was for reused streaming `require_attempt` RPCs; request workers should not share that protocol.

Master still needs a **replica count from the orchestrator** (same place as today’s `get_num_expected_workers`), so it can wait until HTTP replicas are Ready before the job is serving. That number is **not** engine parallelism and does **not** belong on `PipelineSpec`.

### What the engine does *not* need

- `request_parallelism` / graph parallelism on the request chain.
- A request assignment strategy. One replica **is** the whole subgraph; there is no per-operator placement and no cross-worker edge.
- `OperatorPerWorker` / `Pipelined` ever seeing HTTP vertices.

Replica count lives on the launch spec / CRD (`spec.requestWorkers.replicas`, docker env). Master reads it from the orchestrator.

## PR stack

Merge order. Each PR is reviewable alone. PR1 and PR2 should follow each other quickly because PR1 removes the mixed graph that today’s request runtime tests depend on.

```
PR1  compile: two graphs + in-process executor
PR2  request worker type + master two pools
PR3  request metrics
PR4  kube / docker HTTP endpoint
PR5  cluster e2e                               → after #291
```

[#301](https://github.com/volga-project/volga/issues/301) does not need its own PR. The hang was the read path sitting in `expected_aligns`. PR1 takes those vertices off the streaming graph, so `all_tasks()` is the align set again.

---

### PR1 — Compile: two graphs + in-process request executor

**Goal:** streaming compile never emits HTTP I/O. Request path is an operator chain run per HTTP request, not a parallel `ExecutionGraph`.

Compile:

- `GraphSplitter` extracts a `RequestGraph` (`keyby → WRO → followers`) instead of splicing vertices into the streaming graph.
- Streaming graph: ingest → … → WO `StateOnly`, no outgoing edge to the read path.
- Request chain: `keyby → WRO` (`state_owner_operator_id` unchanged) `→ followers`. No HTTP source or request sink vertices, no copied WO parallelism, no `to_execution_graph` with `task_index` slices. HTTP decode/encode is `RequestExecutor`.
- `RequestSpec` on the pipeline is `max_pending_requests` and `request_timeout_ms`. Listen address is per process, not spec. Input schema is the window input schema.
- `compile_*` returns `{ streaming, request: Option<RequestGraph> }`.
- `MasterConfig` holds the streaming graph only.
- `PipelineSpec`: **no** `request_parallelism`. Replica count is not an engine field.

In-process executor (replaces `pipeline_exec` for request mode):

- `RequestExecutor`: build operator instances from the chain, `open` once, per request `process` on one tokio task, HTTP via axum + semaphore.
- Tests that today use `pipeline_exec` + `SingleWorker` for HTTP (`tests/inprocess/request_source.rs`) move here. No master, no `StreamTask`.
- Window-request **operator** tests stay as they are (injected store).
- Window-request **pipeline** tests that need WO+WRO against InMem: start a streaming `Worker` and a `RequestExecutor` in one test process with an injected shared store. Not a collocated Worker.

Streaming assignment (`OperatorPerWorker`, `Pipelined`) is only ever called on the streaming graph. Delete `graph_has_request_io` once that is true.

**Out of this PR:** kube, master two pools, replica counts, metrics, live `WindowRequestStore`.

---

### PR2 — Request worker process + master two pools

**Goal:** a request worker is a process type. Streaming recovery does not touch it.

Worker:

- Same binary, role from env / orchestrator (`streaming` | `request`). Not “scan vertices for HTTP.”
- Request worker hosts `RequestExecutor` + HTTP. No `StreamTaskActor`, no transport backend, no `execution_attempt_id`, no `require_attempt`.
- Bind address from env (pod IP + port), not a spec literal broadcast to every replica.
- On close: stop HTTP (today’s `stop_request_source_processor_if_needed`, actually called).
- Configure payload: request chain, `pipeline_id`, store session, listen addr.

Master / orchestrator:

- `WorkerNode` class. Two discovery lists. `get_num_expected_streaming_workers` + `get_num_expected_request_workers` from the orchestrator (local harness: two counts on `PipelineLaunchSpec`).
- Streaming `schedule` / `recover` iterate streaming sessions only. `sessions.drain()` must not include request workers.
- Request path: wait until N request workers Ready → configure each with the **full** chain → start HTTP. No mapping, no slots, no assignment strategy.
- Request worker crash: replace that process, re-register, configure; **do not** bump the streaming attempt.

Local harness:

- Two worker pools. Kill a request worker → streaming attempt unchanged, remaining replicas still serve. Kill a streaming worker → request workers stay up.

Cleanup in this PR (leftovers of the mixed worker that are not already gone in PR1):

- Request vertices in `configure_channels` / checkpoint acks

**Not in this PR:** kube Service / second STS (PR4), serving e2e against Scylla (PR5).

---

### PR3 — Request metrics

After the worker exists, before deploy. Today there are **no** request metrics (`observability/metrics` is all `volga_stream_task_*`).

Minimum:

- `in_flight`, `accepted`, `completed`, `timeout`, `rejected_429`, `failed`
- latency histogram (handler start → response)
- semaphore utilization (`in_flight / max_pending`)

Do not reuse stream-task queue / backpressure / watermark metrics. WRO store-read latency belongs with [#294](https://github.com/volga-project/volga/issues/294) if it is store-tagged; a single `request_handler_ms` here is enough to see the HTTP boundary.

---

### PR4 — Deployment: HTTP endpoint and second replica set

Does **not** need Scylla to merge. You cannot serve windows with it until #291.

- CRD: `spec.requestWorkers.replicas` (and later resources/selectors). Do not overload `spec.workers.replicas`.
- Second StatefulSet + label selector. Service for `/request` (current worker Service is control / transport / metrics only).
- Docker: `VOLGA_REQUEST_WORKER_COUNT` next to `VOLGA_WORKER_COUNT`.
- Request workers get listen addr from downward API / env, not from `RequestSourceSinkSpec.bind_address` copied identically to every replica.
- Orchestrators report two node lists to the master.

Smoke: request STS comes up, port binds, master sees N request workers Ready. Do not require WRO to read store.

---

### PR5 — Cluster tests (after #291)

- Request-mode cluster (local / docker / kube) with streaming replicas + request replicas.
- Recovery: request kill does not bump streaming attempt; streaming kill does not take down HTTP.
- Checkpoint: request-mode + non-zero interval publishes a cut the request worker can read (needs #291 + #300).
- This is the process-boundary proof. Layer C in the store plan stays collocated WO/WRO against remote Scylla until this lands; do not describe Layer C as independent serving.

## Leftover mixed-architecture code (checklist)

Delete or stop using once PR1–PR2 land:

- [x] `GraphSplitter` copying WO parallelism into one mixed graph
- [x] `graph_has_request_io` / `OperatorPerWorker` panic pointing at #247
- [x] `extract_request_source_config`
- [x] `RequestSourceProcessor` + shared `Mutex<Receiver>` fetch
- [x] `HttpRequestSourceFunction` / `RequestSinkFunction` as stream functions
- [x] `RuntimeContext.request_sink_source_*`
- [x] `RequestRoutePartition` / `SOURCE_TASK_INDEX_FIELD`
- [x] Response accumulation by `expected_record_count`
- [x] `WorkerInner.request_source_processor` + unused `stop_request_source_processor_if_needed`
- [x] `execution_graph.rs` WO+WRO same-node TODO
- [x] Single `MasterConfig.expected_workers` / `wait_for_ready_workers` over a mixed node list
- [x] `recover()` draining request sessions
- [ ] `pipeline_exec` as the request-mode runtime (`SingleWorker` mixed graph)
- [x] `RequestSourceSinkSpec` / `bind_address` as the replica listen address

## Out of scope

- Collocated streaming+request `Worker` (later issue, if ever).
- Separate request transport (#117, closed).
- Request assignment strategies / request graph parallelism.
- `WindowRequestStore` implementation (#291) and checkpoint-complete notify (#300).
- Two worker binaries (role env on `volga-worker` is enough).
