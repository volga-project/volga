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

From repo root:

```bash
# 1) Build/load operator image
docker build -t kubevolga:dev ./kubevolga
kind load docker-image kubevolga:dev --name kubevolga

# 2) Build/load Volga runtime image used by master/worker/test-storage
docker build -t volga:latest .
kind load docker-image volga:latest --name kubevolga

# 3) Deploy operator stack and sample
cd kubevolga
make install   # CRD
make deploy    # namespace + RBAC + operator
make sample    # sample VolgaPipeline
```

Cleanup:

```bash
make unsample
make undeploy
make uninstall
```

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

- `src/runtime/tests/kube_entrypoints_test.rs`

Behavior summary:

- Applies `kubevolga/config/test-storage` (`volga-test-storage` deployment/service).
- Reads `kubevolga/config/samples/volga_v1alpha1_pipeline.yaml`.
- Patches at runtime:
  - pipeline `metadata.name` (unique UUID-based name)
  - sink `server_addr` (embedded JSON string under `sink.InMemoryStorageGrpc`)
  - datagen `limit` and `batch_size` (embedded JSON string under `sources[0].source.Datagen`)
  - worker replicas (computed from logical graph)
- Applies generated CR JSON with `kubectl`.
- Waits for phase `Running`, port-forwards test storage, and asserts sink records.

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
