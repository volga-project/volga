# kubevolga

Kubernetes operator for Volga pipelines.

## What It Reconciles

- CRD: `VolgaPipeline` (`volga.io/v1alpha1`)
- Per-pipeline resources:
  - master Service: `<pipeline>-master`
  - master Pod (restart policy `Never`): `<pipeline>-master`
  - worker headless Service: `<pipeline>-workers`
  - worker StatefulSet: `<pipeline>-worker`
  - runtime ServiceAccount / Role / RoleBinding
- Status fields:
  - `status.pipelineID`
  - `status.masterServiceAddr`
  - `status.phase` (`Starting`, `Running`, `Failed`, `InvalidSpec`)

## Runtime Behavior Notes

- Master is reconciled as a plain Pod with `restartPolicy: Never`.
- On failure, operator keeps the failed master Pod (no auto recreate loop).
- Workers are managed by StatefulSet; master discovery/configuration waits for worker registration and node visibility before configure.

## Dev Flow

From repo root. The sample CR pins master to `volga.io/role=infra` and workers to
tainted `volga.io/role=worker` nodes, so the cluster must match
`kubevolga/hack/kind-multi.yaml` (default name `kubevolga`).

```bash
# Kind (infra + 2 worker nodes), images, CRD, operator
scripts/kube-test-env setup

cd kubevolga
make sample    # sample VolgaPipeline
```

`scripts/kube-test-env setup` is `kind create --name kubevolga --config kubevolga/hack/kind-multi.yaml`
plus image load and `make install deploy`. A single-node cluster with that name
is recreated. Cleanup: `make unsample` / `scripts/kube-test-env destroy`.

## Image/Target Notes

- Operator image is `kubevolga:dev` (`kubevolga/config/operator/operator.yaml`).
- Pipeline runtime image defaults to `volga:latest` (`spec.image` in sample + controller default).
- There is no separate `volga-test-worker` image target today. `volga-master`, `volga-worker`, and `volga-test-storage` are built from the root `Dockerfile` into the same `volga:*` image.

## Schema Generation Flow

The operator validates `spec.pipelineSpec` using:

- `kubevolga/internal/controller/kube_pipeline_spec.schema.json`

Regenerate from Rust type (`KubePipelineSpec`) with:

```bash
cargo run --bin generate-kube-pipeline-schema
```

Validation test:

```bash
cd kubevolga
go test ./internal/controller -v
```

## Rust Kube E2E Test Flow

Test file:

- `src/tests/kube/smoke.rs`

Behavior summary:

- Reads `kubevolga/config/samples/volga_v1alpha1_pipeline.yaml` (master on `volga.io/role=infra`, workers on tainted `volga.io/role=worker` nodes).
- Patches at runtime:
  - pipeline `metadata.name` (unique UUID-based name)
  - CR sink `InMemoryStorageGrpc.create: true` plus `server_addr` `http://{name}-storage.default.svc.cluster.local:50071` when the job needs the store; Count left alone
  - datagen `limit` and `batch_size` (embedded JSON string under `sources[0].source.Datagen`)
  - worker replicas (computed from logical graph)
- Applies generated CR JSON with `kubectl`. Does not apply `kubevolga/config/test-storage` (kept for Docker/manual).
- Kube operator starts `{name}-storage` when `create: true` and `server_addr` is that Service DNS.
  Engine `SinkSpec` has no `create` field (`get_spec` is plain deserialize).
- Waits for phase `Running`, port-forwards `svc/{pipeline}-storage` when a store was created, and asserts sink records.

Run manually:

```bash
cargo test test_kube_master_and_workers_smoke -- --ignored --nocapture
```

## Sample `pipelineSpec` YAML Style

Sample file:

- `kubevolga/config/samples/volga_v1alpha1_pipeline.yaml`

It uses embedded JSON strings for JSON-like fields:

- `sources[].schema_json`
- `sources[].source.<SourceKind>` (for example `Datagen`)
- `sink.<SinkKind>` (for example `InMemoryStorageGrpc`)

`src/api/spec/kube.rs` normalizes embedded JSON strings recursively into objects/arrays before converting to runtime `PipelineSpec`.
