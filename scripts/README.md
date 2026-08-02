# Test runner

Run test profiles through `scripts/test` so host and CI runs use the same
build cache, environment setup, filters, and concurrency defaults.

```bash
scripts/test unit
scripts/test grpc-based
scripts/test default
scripts/test docker
scripts/test kube
scripts/test all
```

## Profiles

| Profile | What it runs |
|---|---|
| `unit` | Fast tests with no gRPC/cluster harness, Docker, or Kube |
| `grpc-based` | Tests that use in-process gRPC / local cluster harness; default concurrency 1 to avoid interference |
| `default` | `unit` + `grpc-based` (PR/CI default) |
| `docker` | Docker / Localstack / Kafka-style ignored tests |
| `kube` | `::kube::` ignored tests on Kind |
| `all` | `default` + `docker` + `kube` |
| `benchmark` | Benchmark filter |
| `stress` | Repeat a suite or selected tests; see below |

`docker`, `kube`, and `all` prepare their required environment. Kube tests use
the Kind cluster named by `VOLGA_KIND_CLUSTER` (default: `kubevolga`).

## Options

All profiles accept:

```text
--jobs N              Test processes to run concurrently
--build-jobs N        Cargo build parallelism
--retries N           Nextest retries per test
--filter EXPR         Nextest filter expression
--nextest-profile P   Nextest profile name
```

`--filter` runs only the matching tests for the selected profile. For example:

```bash
scripts/test default --filter 'test(test_local_multi_worker_window_checkpoint_restore)'
```

## Stress runs

The stress runner repeats an environment suite or selected tests in parallel
shards. It writes one log per shard under `target/stress/` and stops scheduling
new iterations after the first failure.

`--env` selects the runtime environment (`local` = process harness, `kube` =
Kind). It is mapped to the matching run profile (`local` → `default`,
`kube` → `kube`).

```bash
# One process-harness test across four parallel shards.
scripts/test stress --env local \
  --test test_local_multi_worker_window_checkpoint_restore \
  --runs-per-shard 20 --shards 4

# Multiple Kube tests with a fresh Kind cluster per iteration.
scripts/test stress --env kube \
  --test test_kube_multi_worker_window_checkpoint_restore \
  --test test_kube_mid_flight_checkpoint_kill_after_safe_restores_prior \
  --runs-per-shard 10 --shards 3 --fresh-cluster

# Repeat the full environment profile.
scripts/test stress --env local --all --runs-per-shard 10 --shards 2
```

Stress options:

```text
--env {local|kube}       Required runtime environment
--all                    Run the complete environment profile
--test NAME              Run an exact test name; repeat for multiple tests
--filter EXPR            Supply a Nextest filter expression directly
--runs-per-shard N       Maximum iterations per shard
--shards N               Parallel host shards or isolated Kube clusters
--fresh-cluster          Recreate the Kind cluster before each Kube iteration
```

Stress uses the `stress` Nextest profile with longer timeouts: 120 seconds for
local/process tests and 180 seconds for Kube tests.
