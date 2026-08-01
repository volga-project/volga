# Test runner

Run test profiles through `scripts/test` so local and CI runs use the same
build cache, environment setup, filters, and concurrency defaults.

```bash
scripts/test unit
scripts/test local
scripts/test docker
scripts/test kube
scripts/test all
```

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
scripts/test local --filter 'test(test_local_multi_worker_window_checkpoint_restore)'
```

## Stress runs

The stress runner repeats an environment suite or selected tests in parallel
shards. It writes one log per shard under `target/stress/` and stops scheduling
new iterations after the first failure.

```bash
# One local test across four parallel shards.
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
--env {local|kube}       Required test environment
--all                    Run the complete environment profile
--test NAME              Run an exact test name; repeat for multiple tests
--filter EXPR            Supply a Nextest filter expression directly
--runs-per-shard N       Maximum iterations per shard
--shards N               Parallel local shards or isolated Kube clusters
--fresh-cluster          Recreate the Kind cluster before each Kube iteration
```

Stress uses the `stress` Nextest profile with longer timeouts: 120 seconds for
local tests and 180 seconds for Kube tests.
