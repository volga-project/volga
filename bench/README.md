# Window bench

`volga-bench` launches a streaming window pipeline, leaves it up for `duration`, then dumps Prometheus and checks oracles. Grafana is the live view. The dump is the pass/fail check after the run.

This is not a `scripts/test` profile. Locally it uses the same Kind cluster as the kube tests.

Job specs live in this directory. The cluster install (Prometheus, Grafana, the dashboard, Scylla Helm values) stays in [`kubevolga/hack/bench`](../kubevolga/hack/bench) because `make -C kubevolga bench` applies that kustomize directory.

## Local prerequisites

Docker, [kind](https://kind.sigs.k8s.io/), kubectl, and make. `scripts/kube-test-env setup` checks those. A Rust toolchain (`cargo`) builds and runs `volga-bench`. Helm is only needed for a Scylla backend (`make -C kubevolga bench-scylla`).

## What the job is

Streaming mode only, and only a continuously sliding window query. Request mode, batch, and other SQL shapes are not covered.

Parallelism 4, two slots per worker, two workers plus a master. The source is datagen (`timestamp`, `key`, `value`). The SQL is a 10s `RANGE` window: `SUM`, `COUNT`, `AVG`, `MIN`, `MAX` partitioned by `key`. Tiles are 1s and 5s. The sink counts rows. It does not write them out.

| `launch.backend` | State |
| --- | --- |
| omitted, or `kind: in_memory` | process-local window store |
| `kind: scylla` | Scylla window store. The Count sink stays in process. |

| File | What it is |
| --- | --- |
| [`example.yaml`](example.yaml) | Default. 1h, checkpoint every 30s, one worker kill after checkpoint 1, datagen 200 rows/s. Scylla contact is the operator-chart client service. |
| [`unlimited.yaml`](unlimited.yaml) | Optional max-throughput run. 180s, no interval checkpoints, `rate: null`, `step_ms: 1`. In-memory, because it sets no backend. Lag oracles are 24h so a watermark ahead of the wall clock does not fail the run. |

## Bring-up

```bash
# 1. Kind + operator. Recreates kubevolga if the current cluster is single-node.
scripts/kube-test-env setup

# 2. Prometheus + Grafana in namespace volga-bench.
make -C kubevolga bench

# 3. Scylla, only for a Scylla backend.
#    Kind: one member on the infra node.
#    A real cluster: VOLGA_SCYLLA_VALUES=kubevolga/hack/bench/scylla/prod.yaml
make -C kubevolga bench-scylla

# 4. Laptop access to Prom (the dump and oracles) and Grafana.
export VOLGA_KUBE_CONTEXT="${VOLGA_KUBE_CONTEXT:-kind-kubevolga}"
kubectl --context "$VOLGA_KUBE_CONTEXT" -n volga-bench port-forward svc/prometheus 9090:9090 &
kubectl --context "$VOLGA_KUBE_CONTEXT" -n volga-bench port-forward svc/grafana 3000:3000 &
```

Grafana is http://localhost:3000. Anonymous viewer. The Volga bench dashboard is the home dashboard. `prom_url` in the YAML must reach Prometheus from the bench process. The shipped files use `http://127.0.0.1:9090`, which is the port-forward.

`make -C kubevolga unbench` removes Prometheus and Grafana. `scripts/kube-test-env destroy` deletes the Kind cluster.

On a remote cluster, skip Kind, set `VOLGA_KUBE_CONTEXT` if it is not the current context, and apply the same manifests. Overnight runs belong there, not on Kind.

## Run

```bash
# Default: 200 rows/s, checkpoints, one kill. Shorter than the 1h YAML duration.
cargo run --bin volga-bench -- \
  --config bench/example.yaml \
  --env kube \
  --duration-secs 120

# Optional: unlimited datagen, no interval checkpoints, in-memory, 180s.
cargo run --bin volga-bench -- \
  --config bench/unlimited.yaml \
  --env kube
```

`--duration-secs` is the only duration override. A Scylla run is either spec plus:

```yaml
launch:
  backend:
    kind: scylla
    contact_points:
      - scylla-client.scylla.svc.cluster.local:9042
    keyspace: volga_bench
    datacenter: datacenter1
```

`example.yaml` already has that block. The Kind one-pod used for local tests is `scylla.default.svc.cluster.local:9042`, not the chart's client service. Workers create the keyspace.

When the run finishes it writes `<dump>/lifecycle.json` and `<dump>/prom.json`, then evaluates oracles. A failed oracle exits non-zero after the job has already run.

## Knobs that change the shape of the graphs

`launch.datagen.rate` is the pipeline total, not per task. Omitted means 200 rows/s. `rate: null` means no sleep between batches.

`step_ms` is event-time spacing per record on a key. It is only valid when `rate` is null. A finite rate derives the step from `num_unique_keys / rate` so event time tracks the wall clock. Unlimited defaults to `step_ms: 1`.

`num_unique_keys` defaults to `parallelism * 4` (16 at parallelism 4). With `step_ms: 1`, event time stays even with the wall clock only near `1000 * num_unique_keys` rows/s. Below that, watermark lag grows for the whole run and retained raw data grows with it. `unlimited.yaml` assumes the job is fast enough that the watermark sits ahead of the wall and the lag gauge sits at 0.

`watermark.out_of_orderness_ms` and `watermark.emit_interval_ms` are the pipeline watermark settings.

`checkpoint.interval: 0s` turns interval checkpoints off. A non-zero interval asks for a barrier on that period. A tick that lands while a checkpoint is in flight is skipped. The next barrier is not queued behind it.

Checkpoint timeout in the shipped files is 60s. If the barrier does not finish by then, the checkpoint fails and the master starts another attempt. With no completed checkpoint, that attempt does not restore.

Raw Scylla rows are stored in 60s buckets. Maintain publishes live-row gauges from the live watermark. Range deletes wait for a committed checkpoint watermark, so with checkpoints off the delete graphs stay empty while the live-row graphs still move.

## Dashboard

One board, `Volga bench`. Rows top to bottom:

1. Flow
2. Task time
3. Window operator
4. In-memory store
5. Scylla store
6. Job
7. Transport
8. Process / container
9. Scylla cluster

Per-operator throughput series are the same rows counted at each hop. Do not add them together.

### Flow

**Throughput.** `rate(records_sent)` by operator, plus window receive and sink rows written. Source sent is ingest. Window sent is what the window emitted. Window receive is what the window ingested. Sink written is pipeline output. A source line well above the window-sent line means a backlog inside the window.

**Event-time lag.** `wall_now − watermark`, p50 and p99 across selected tasks. This is a gauge quantile, not a histogram. 0 means the watermark is at or ahead of the wall. The series is omitted when the watermark is unset.

### Task time

Busy, idle, and backpressured milliseconds per second. The three lines for one task add up to about 1000. Pick one task in the dropdown. All tasks at once is too many lines. A task at ~1000 backpressured is blocked on its output.

### Window operator

**Ingest / watermark process.** p50 and p99 of `insert_batch` and of processing a due watermark. The histogram is merged across selected tasks, weighted by call rate. Buckets run from 1ms to 5 minutes.

**Batches / late.** Ingest calls per second and watermark-process calls per second. Those are batches, not rows. Late drops are rows. Do not compare batches/s to sink rows/s.

### In-memory store

Empty on a Scylla job.

**State bytes** and **state counts** are the in-memory backend only: raw, tiles, triggers, key state. `key_states` is about the number of distinct keys. Trigger counts can dwarf raw rows when watermark processing falls behind.

**Pruned rows** is what the in-memory maintain tick dropped.

### Scylla store

Volga's client metrics. Empty on an in-memory job.

**Calls** and **failures.** Successful and failed store calls per second. A failure is not counted as a call.

**Call latency p99.** One histogram sample per call, including failures.

**Rows, payload bytes, statements.** Rows returned or sent, blob bytes on load and commit, and CQL statements inside the call. One unlogged batch counts as one statement.

**Live rows.** Counts from the maintain scan: live and expired key-group buckets, distinct keys, tile-version rows, key-state rows. These follow the live watermark. They are not `COUNT(*)` and not bytes on disk.

**Deletes.** Raw partitions, tile ranges, and trigger shards the tick actually deleted. Deletes use the last committed checkpoint watermark. No completed checkpoint means this row stays at zero even while live rows move.

**Maintain tick** and **maintain phases.** Wall time of a tick, and p99 of `index_scan`, `key_gc`, and `trigger_delete`. A tick is skipped if the previous one was less than 10s ago. The worker's cleaner still wakes every 1s.

### Job

**Checkpoints.** Trailing 5-minute increase of completed and failed master counters. Empty if the master was not scraped, or if no checkpoint completed.

**Checkpoint duration p99.** Successes only. The histogram jumps from 10s to 30s. A flat line near 29.8s means every sample landed in that bucket (`10s + 0.99 × 20s`). The real duration is somewhere above 10s and at or under 30s.

### Transport

**Tx queue remaining.** Credits left on an output edge. The bound is 8192 records. 0 means this task is blocked sending to that target.

**Rx queued records.** Records waiting on an input. A window input sitting on 8192 means the window is not draining and a checkpoint barrier behind that queue cannot reach it.

**Pump write-block.** Milliseconds per second spent waiting on a TCP write to a peer that is not reading.

**Transport disconnects.** Trailing 5-minute increase. A close is fatal for that edge.

### Process / container

**CPU.** cAdvisor, summed by pod, for `master` and `worker` containers. 1.0 is one core. Not filtered by pipeline.

**Memory.** Worker RSS from Volga, and cAdvisor working set for master and workers. Same shape. RSS is usually a bit under the cgroup working set.

### Scylla cluster

Scylla's own metrics, one line per pod. A multi-node cluster shows up as several lines. Empty until Prometheus is scraping the Scylla job (port 9180).

**Disk space.** Total column-family disk, live disk, and active commitlog, per node. Total should level off once maintain keeps up with ingest. A steady climb means data is accumulating on that node.

**Reactor CPU.** Busy cores are `sum(scylla_reactor_utilization) / 100` per node. The gauge is 0–100 per shard, so one saturated shard is 1 core and an 8-shard node at full tilt reads 8. Max shard % is the hottest shard on that node, still 0–100. This is Scylla's reactor, not the cAdvisor core count.

**Compaction** and **compaction backlog.** Active and waiting compactions, and the backlog in bytes, per node. Pending work that never returns to zero means compaction is behind on that node.

**Memory.** Dirty regular memory and cache bytes, per node. On the Kind one-shard pod, dirty memory should stay inside the 1G shard.
