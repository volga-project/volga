# Test runner

Run test profiles through `scripts/test` so host and CI runs use the same
build cache, environment setup, filters, and concurrency defaults.

```bash
scripts/test unit
scripts/test inprocess
scripts/test default
scripts/test docker
scripts/test kube
scripts/test all
```

## Profiles

| Profile | What it runs |
|---|---|
| `unit` | Fast tests with no in-process harness, Docker, or Kube |
| `inprocess` | Local gRPC/cluster harness under `tests::inprocess`; concurrency 1; nextest profile 90s |
| `default` | `unit` + `inprocess` (required PR/CI gate) |
| `docker` | `tests::docker::` ignored tests (LocalStack, Kafka, etc.) |
| `kube` | `tests::kube::` ignored tests on Kind |
| `all` | `default` + `docker` + `kube` |
| `benchmark` | Benchmark filter |
| `stress` | Repeat a suite or selected tests; see below |

CI runs `default` as the required job, plus parallel non-blocking `docker` and
`kube` jobs on PRs. Scheduled runs alternate **inproc** and **kube** stress
(even/odd UTC hour) so each can use the full Free-plan runner pool (20).

### One-off CI runs (`workflow_dispatch`)

GitHub → **Actions** → **Rust tests** → **Run workflow**: pick branch +
`profile` (`default` / `docker` / `kube` / `all` / `kube-stress` /
`inproc-stress`).

Stress knobs (shared by `kube-stress` / `inproc-stress`):

| Input | Default | Meaning |
| --- | --- | --- |
| `stress_machines` | `3` | GitHub runners (machine isolation) |
| `stress_shards_per_machine` | `1` | Parallel stress processes per runner |
| `stress_runs_per_shard` | `10` | Iterations per process |
| `stress_test` | (inproc default) | Exact test name; `inproc-stress` / `kube-stress` (`test_kube_*`) |

Scheduled stress (every 4h UTC; suites alternate each tick → each suite ~8h):

| UTC hours (cron `17 */4`) | Suite | Shape | Notes |
| --- | --- | --- | --- |
| 0, 8, 16 | inproc | 15 × 1 × 100 | `test_local_multi_worker_window_checkpoint_restore` (~25–40m) |
| 4, 12, 20 | kube | 15 × 1 × 10 | full kube suite + fresh Kind (~30–45m/machine) |

Prefer `stress_shards_per_machine=1` so parallelism comes from machines, not
co-tenancy on one runner. Sized for ~18 usable Free-plan runners (leave headroom).

Or from the CLI (same inputs):

```bash
# Docker-only job on the current branch tip
gh workflow run rust-tests.yml --ref "$(git branch --show-current)" -f profile=docker

# Kube-only
gh workflow run rust-tests.yml --ref "$(git branch --show-current)" -f profile=kube

# On-demand kube stress (same shape as the schedule)
gh workflow run rust-tests.yml --ref "$(git branch --show-current)" \
  -f profile=kube-stress \
  -f stress_machines=3 \
  -f stress_shards_per_machine=1 \
  -f stress_runs_per_shard=10

# On-demand inprocess stress for one test (4 isolated runners)
gh workflow run rust-tests.yml --ref "$(git branch --show-current)" \
  -f profile=inproc-stress \
  -f stress_test=test_local_multi_worker_window_checkpoint_restore \
  -f stress_machines=4 \
  -f stress_shards_per_machine=1 \
  -f stress_runs_per_shard=10
```

`kube-stress` / `inproc-stress` skip the normal test/docker/kube jobs.
`inproc-stress` runs `scripts/test stress --env local` for `stress_test`.
Logs upload per machine from `target/stress/`.
`docker` / `kube` run just that env job. `unit` / `inprocess` / `default` run
the required `test` job with that profile (and may still start sibling
docker/kube jobs unless you pick an env-only profile).

`docker` and `kube` prepare their environment immediately before that suite.
`all` runs `unit` + `inprocess` first, then docker/Kind setup only when those
suites start. `scripts/docker-test-env setup` prefetches testcontainers images
(LocalStack, Redpanda) before building `volga:latest`. Kube uses the Kind
cluster named by `VOLGA_KIND_CLUSTER` (default: `kubevolga`), created from
`kubevolga/hack/kind-multi.yaml` (control-plane + infra + 2 tainted worker
nodes). A cluster without that topology is deleted and recreated.

### CI image cache

On GitHub Actions, `volga:latest` (and `kubevolga:dev` for kube) build through
Buildx with `cache-from` / `cache-to` `type=gha`. Docker and kube jobs share
scope `volga-image`, so a warm cook layer from one job speeds up the other on
later runs. Locally the scripts keep plain `docker build` unless you set
`VOLGA_DOCKER_CACHE=gha` (requires Buildx + Actions cache credentials).

## Options

All profiles accept:

```text
--jobs N              Test processes to run concurrently
--build-jobs N        Cargo build parallelism
--retries N           Nextest retries per test
--filter EXPR         Nextest filter expression
--nextest-profile P   Nextest profile name
--keep-going          After a suite fails, continue remaining suites (default/all)
```

`--filter` runs only the matching tests for the selected profile. For example:

```bash
scripts/test default --filter 'test(test_local_multi_worker_window_checkpoint_restore)'
scripts/test all --keep-going
```

With `--keep-going`, a final **failed suites** list is printed and each suite is
teed to `target/test-runs/<suite>.*.log`. Nextest is configured for
`--failure-output final` / `--final-status-level fail` so failure details and
names show at the end of each suite without drowning the scrollback.

Debug a single failure without replaying the whole `all` run:

```bash
rg '^\\s*FAIL ' target/test-runs/*.log
scripts/test inprocess --filter 'test(test_local_single_worker_window_checkpoint_restore)'
# or: cargo nextest run --lib --no-capture -E 'test(<exact_name>)'
```

## Stress runs

The stress runner repeats the **kube** suite (or selected kube tests) across
parallel shards. It writes one log per shard under `target/stress/` and stops
scheduling new iterations after the first failure. Prefer `--env kube` (Kind);
CI schedule and on-demand `profile=kube-stress` use the same path.

```bash
# Full kube profile, fresh Kind cluster each iteration.
scripts/test stress --env kube --all \
  --runs-per-shard 10 --shards 3 --fresh-cluster

# Selected kube tests.
scripts/test stress --env kube \
  --test test_kube_multi_worker_window_checkpoint_restore \
  --test test_kube_mid_flight_checkpoint_kill_after_safe_restores_prior \
  --runs-per-shard 10 --shards 3 --fresh-cluster
```

Stress options:

```text
--env kube               Kind runtime (required for the supported stress path)
--all                    Run the complete kube profile
--test NAME              Run an exact test name; repeat for multiple tests
--filter EXPR            Supply a Nextest filter expression directly
--runs-per-shard N       Maximum iterations per shard
--shards N               Parallel isolated Kind clusters
--fresh-cluster          Recreate the Kind cluster before each iteration
```

Timeouts are suite-scoped nextest profiles only (`default` 30s, `inprocess` 90s,
`docker` 120s, `kube` 90s, `stress` 180s) — no per-test overrides.

## Bench (`volga-bench`)

Long-run job on the same Kind (or remote) cluster as kube tests. Not a
`scripts/test` profile: YAML is the run spec; CLI only overrides `--env` and
`--duration-secs`. Count sink + Datagen are fixed in code.

```bash
# 1) Kind + operator (infra + 2 tainted worker nodes). Recreates kubevolga if
#    the current cluster is single-node.
scripts/kube-test-env setup

# 2) Prom + Grafana in namespace volga-bench (infra nodeSelector).
make -C kubevolga bench

# 3) Reach Prom (oracles/dump) and the Grafana board from the laptop.
export VOLGA_KUBE_CONTEXT="${VOLGA_KUBE_CONTEXT:-kind-kubevolga}"
kubectl --context "$VOLGA_KUBE_CONTEXT" -n volga-bench port-forward svc/prometheus 9090:9090 &
kubectl --context "$VOLGA_KUBE_CONTEXT" -n volga-bench port-forward svc/grafana 3000:3000 &

# 4) Short Kind smoke (example.yaml is a 1h kill scenario; override duration).
cargo run --bin volga-bench -- \
  --config kubevolga/hack/bench/example.yaml \
  --env kube \
  --duration-secs 120
```

Grafana: http://localhost:3000 (anonymous viewer; Volga bench is the home
dashboard). Prom dump/oracles need `prom_url` in the YAML (example uses
`http://127.0.0.1:9090` after the port-forward).

| | Kind (local / CI wiring) | Remote cluster |
| --- | --- | --- |
| Cluster | `scripts/kube-test-env setup` | existing kubeconfig; skip Kind |
| Context | `VOLGA_KUBE_CONTEXT=kind-kubevolga` | omit, or set to that context |
| Observability | `make -C kubevolga bench` + port-forward | same manifests; port-forward or in-cluster `prom_url` |
| Duration | `--duration-secs 120` to see the board | YAML `duration` (example: 1h) |
| Numbers | scrape/oracles wiring only | overnight; not GitHub-hosted CI |

`make -C kubevolga unbench` removes Prom/Grafana. `scripts/kube-test-env destroy`
deletes the Kind cluster. Overnight soaks belong on a real cluster, not Kind.
