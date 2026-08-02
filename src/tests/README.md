# Suite tests

Profiled suite tests live under `src/tests/` and compile only with `cargo test --lib`.

## Layout

| Directory | Purpose |
|---|---|
| `inprocess/` | Local harness tests (gRPC workers, cluster harness, transport matrix, SQL) |
| `docker/` | Docker / testcontainers tests — always `#[ignore]` |
| `kube/` | Kind / Kubernetes tests — always `#[ignore]` |
| `support/` | Shared runners, launch specs, cluster harness (not test entrypoints) |

## Authoring rules

- Put new suite tests under **`inprocess`**, **`docker`**, or **`kube`** depending on runtime env.
- **`docker`** and **`kube`** entrypoint tests must stay **`#[ignore]`** (run via `scripts/test docker` / `scripts/test kube`).
- Do not add gRPC/port-binding tests to the **unit** suite — they belong in **`inprocess`** (`test(/tests::inprocess::/)`).
- Reuse helpers from `support/` rather than duplicating harness code under product modules.
