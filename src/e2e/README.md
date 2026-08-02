# End-to-end tests

E2e tests live under `src/e2e/` and compile only with `cargo test --lib`.

## Layout

| Directory | Purpose |
|---|---|
| `inprocess/` | Local harness tests (gRPC workers, cluster harness, transport matrix, SQL) |
| `docker/` | Docker / testcontainers tests — always `#[ignore]` |
| `kube/` | Kind / Kubernetes tests — always `#[ignore]` |
| `support/` | Shared runners, launch specs, cluster harness (not test entrypoints) |

## Authoring rules

- Put new e2e tests under **`inprocess`**, **`docker`**, or **`kube`** depending on runtime env.
- **`docker`** and **`kube`** entrypoint tests must stay **`#[ignore]`** (run via `scripts/test docker` / `scripts/test kube`).
- Do not add gRPC/port-binding tests to the **unit** suite — they belong in **`inprocess`** (`test(/e2e::inprocess::/)`).
- Reuse helpers from `support/` rather than duplicating harness code under `runtime::tests`.

See `scripts/README.md` for suite filters and CI profiles.
