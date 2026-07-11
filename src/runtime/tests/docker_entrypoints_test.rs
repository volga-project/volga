use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

use anyhow::{Context, Result};
use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow_integration_test::schema_to_json;
use datafusion::common::ScalarValue;
use uuid::Uuid;

use crate::api::spec::connectors::{SinkSpec, SourceSpec, SourceSpecKind};
use crate::api::spec::pipeline::ExecutionProfile;
use crate::api::{PipelineSpecBuilder, TaskWorkerAssignmentStrategyType};
use crate::common::test_utils::gen_unique_grpc_port;
use crate::runtime::functions::source::datagen_source::{DatagenSpec, FieldGenerator};
use crate::runtime::master::server::master_service::master_service_client::MasterServiceClient;
use crate::runtime::master::server::master_service::GetLatestPipelineSnapshotRequest;
use crate::runtime::observability::{PipelineSnapshot, StreamTaskStatus};
use crate::storage::InMemoryStorageClient;

const MASTER_CONTAINER_PORT: u16 = 50051;
const WORKER_CONTROL_PORT: u16 = 50052;
const WORKER_TRANSPORT_PORT: u16 = 60052;
const WORKER_HOST_PREFIX_BASE: &str = "worker-";
const STORAGE_CONTAINER_PORT: u16 = 50071;
const NUM_WORKERS: usize = 3;
const SLOTS_PER_NODE: usize = 2;
const PARALLELISM: usize = NUM_WORKERS * SLOTS_PER_NODE;
const NUM_BATCHES: usize = 2;
const BATCH_SIZE: usize = 5;
const EXPECTED_RECORDS_PER_TASK: usize = NUM_BATCHES * BATCH_SIZE;
const EXPECTED_TOTAL_RECORDS: usize = EXPECTED_RECORDS_PER_TASK * PARALLELISM;

fn build_test_pipeline_spec_json(sink_server_addr: &str) -> Result<String> {
    let schema = Schema::new(vec![Field::new("value", DataType::Utf8, false)]);
    let mut fields = HashMap::new();
    fields.insert(
        "value".to_string(),
        FieldGenerator::Values {
            values: vec![ScalarValue::Utf8(Some("v".to_string()))],
        },
    );
    let spec = PipelineSpecBuilder::new()
        .with_parallelism(PARALLELISM)
        .with_execution_profile(ExecutionProfile::MasterWorker {
            num_threads_per_task: 4,
        })
        .with_task_assignment_strategy(TaskWorkerAssignmentStrategyType::Pipelined {
            slots_per_node: SLOTS_PER_NODE,
        })
        .with_source(
            SourceSpec::new(
                "test_table",
                SourceSpecKind::Datagen(DatagenSpec {
                rate: None,
                limit: Some(EXPECTED_TOTAL_RECORDS),
                run_for_s: None,
                batch_size: BATCH_SIZE,
                fields,
                replayable: true,
                }),
                schema_to_json(&schema),
            )
        )
        .with_sink(SinkSpec::InMemoryStorageGrpc {
            server_addr: sink_server_addr.to_string(),
        })
        .sql("SELECT value FROM test_table")
        .build();
    let spec_json =
        serde_json::to_string(&spec).context("failed to serialize test pipeline spec")?;
    Ok(spec_json)
}

#[allow(dead_code)]
fn ensure_test_image() -> Result<()> {
    let status = Command::new("docker")
        .args(["build", "-t", "volga:test", "."])
        .status()
        .context("failed to execute docker build")?;
    if !status.success() {
        anyhow::bail!("docker build failed with status: {}", status);
    }
    Ok(())
}

fn read_compose_logs(compose_file: &PathBuf, compose_env_file: &PathBuf, project_name: &str) -> String {
    match Command::new("docker")
        .args([
            "compose",
            "-f",
            compose_file.to_str().unwrap(),
            "--env-file",
            compose_env_file.to_str().unwrap(),
            "-p",
            project_name,
            "logs",
            "--tail",
            "200",
        ])
        .output()
    {
        Ok(output) => {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            format!("docker compose logs:\nstdout:\n{}\nstderr:\n{}", stdout, stderr)
        }
        Err(e) => format!("failed to read docker compose logs: {}", e),
    }
}

async fn connect_master_with_retry(
    master_port: u16,
    compose_file: &PathBuf,
    compose_env_file: &PathBuf,
    project_name: &str,
) -> Result<MasterServiceClient<tonic::transport::Channel>> {
    let endpoint = format!("http://127.0.0.1:{}", master_port);
    let start = tokio::time::Instant::now();
    let timeout = Duration::from_secs(20);
    loop {
        match MasterServiceClient::connect(endpoint.clone()).await {
            Ok(client) => {
                println!(
                    "[DOCKER_TEST] Connected to master service successfully at {}",
                    endpoint
                );
                return Ok(client);
            }
            Err(e) => {
                if start.elapsed() > timeout {
                    let logs = read_compose_logs(compose_file, compose_env_file, project_name);
                    return Err(anyhow::anyhow!(
                        "failed to connect to master service from host after retries: {}\n{}",
                        e,
                        logs
                    ));
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
}

// Manual smoke test: requires local Docker daemon.
#[tokio::test]
#[ignore]
async fn test_docker_master_and_workers_smoke() -> Result<()> {
    // ensure_test_image()?; // comment this if rebuilding the image is not needed

    let master_port = gen_unique_grpc_port();
    let storage_port = gen_unique_grpc_port();
    let pipeline_id = Uuid::new_v4().to_string();
    let worker_host_prefix = WORKER_HOST_PREFIX_BASE.to_string();
    let storage_addr_for_workers = format!("http://storage:{}", STORAGE_CONTAINER_PORT);
    let spec_json = build_test_pipeline_spec_json(&storage_addr_for_workers)?;
    let expected_workers = NUM_WORKERS;
    let expected_workers_str = expected_workers.to_string();
    let compose_project = format!("volga-smoke-{}", Uuid::new_v4().simple());
    let compose_file = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("docker-compose.test.yaml");
    let compose_env_file = std::env::temp_dir().join(format!("{}-compose.env", compose_project));
    let spec_file = std::env::temp_dir().join(format!("{}-pipeline-spec.json", compose_project));
    fs::write(&spec_file, spec_json).context("failed to write pipeline spec file for compose")?;
    let compose_env = format!(
        "MASTER_PORT={master_port}\nMASTER_CONTAINER_PORT={master_container_port}\nSPEC_FILE={spec_file}\nPIPELINE_ID={pipeline_id}\nWORKER_COUNT={worker_count}\nWORKER_HOST_PREFIX={worker_host_prefix}\nWORKER_CONTROL_PORT={worker_control_port}\nWORKER_TRANSPORT_PORT={worker_transport_port}\nSTORAGE_PORT={storage_port}\nSTORAGE_CONTAINER_PORT={storage_container_port}\n",
        master_port = master_port,
        master_container_port = MASTER_CONTAINER_PORT,
        spec_file = spec_file.display(),
        pipeline_id = pipeline_id,
        worker_count = expected_workers_str,
        worker_host_prefix = worker_host_prefix,
        worker_control_port = WORKER_CONTROL_PORT,
        worker_transport_port = WORKER_TRANSPORT_PORT,
        storage_port = storage_port,
        storage_container_port = STORAGE_CONTAINER_PORT,
    );
    fs::write(&compose_env_file, compose_env).context("failed to write compose env file")?;

    let up_output = Command::new("docker")
        .args([
            "compose",
            "-f",
            compose_file.to_str().unwrap(),
            "--env-file",
            compose_env_file.to_str().unwrap(),
            "-p",
            &compose_project,
            "up",
            "-d",
        ])
        .output()
        .context("failed to run docker compose up")?;
    if !up_output.status.success() {
        anyhow::bail!(
            "docker compose up failed: stdout={} stderr={}",
            String::from_utf8_lossy(&up_output.stdout),
            String::from_utf8_lossy(&up_output.stderr)
        );
    }

    let mut master_client =
        connect_master_with_retry(master_port, &compose_file, &compose_env_file, &compose_project)
            .await?;
    let start = tokio::time::Instant::now();
    let snapshot_timeout = Duration::from_secs(10);
    let mut last_progress_log = tokio::time::Instant::now();
    let snapshot: PipelineSnapshot = loop {
        let resp = match master_client
            .get_latest_pipeline_snapshot(tonic::Request::new(GetLatestPipelineSnapshotRequest {}))
            .await
        {
            Ok(resp) => resp.into_inner(),
            Err(e) => {
                if start.elapsed() > snapshot_timeout {
                    let logs = read_compose_logs(&compose_file, &compose_env_file, &compose_project);
                    return Err(anyhow::anyhow!(
                        "master service snapshot request failed after retries: {}\n{}",
                        e,
                        logs
                    ));
                }
                println!(
                    "[DOCKER_TEST] snapshot request failed, reconnecting to master: {}",
                    e
                );
                master_client = connect_master_with_retry(
                    master_port,
                    &compose_file,
                    &compose_env_file,
                    &compose_project,
                )
                .await?;
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
        };
        if resp.has_snapshot {
            let snapshot: PipelineSnapshot = bincode::deserialize(&resp.snapshot_bytes)
                .context("failed to deserialize pipeline snapshot")?;
            let all_workers_present = snapshot.worker_states.len() == expected_workers;
            let all_workers_have_tasks = snapshot
                .worker_states
                .values()
                .all(|w| !w.task_statuses.is_empty());
            let all_tasks_done = snapshot.worker_states.values().all(|w| {
                w.task_statuses.values().all(|status| {
                    *status == StreamTaskStatus::Finished || *status == StreamTaskStatus::Closed
                })
            });
            if all_workers_present && all_workers_have_tasks && all_tasks_done {
                break snapshot;
            }
            if last_progress_log.elapsed() >= Duration::from_secs(10) {
                println!(
                    "[DOCKER_TEST] waiting: workers={}/{} tasks_ready={} all_done={}",
                    snapshot.worker_states.len(),
                    expected_workers,
                    all_workers_have_tasks,
                    all_tasks_done
                );
                last_progress_log = tokio::time::Instant::now();
            }
        } else if last_progress_log.elapsed() >= Duration::from_secs(10) {
            println!("[DOCKER_TEST] waiting: snapshot not available yet");
            last_progress_log = tokio::time::Instant::now();
        }
        if start.elapsed() > snapshot_timeout {
            anyhow::bail!("timed out waiting for pipeline completion snapshot");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    println!(
        "[DOCKER_TEST] Snapshot completion reached: workers={} (all tasks finished/closed)",
        snapshot.worker_states.len()
    );
    assert_eq!(
        snapshot.worker_states.len(),
        expected_workers,
        "all expected workers should be connected and reporting state"
    );

    let mut storage_client =
        InMemoryStorageClient::new(format!("http://127.0.0.1:{}", storage_port)).await?;
    let storage_start = tokio::time::Instant::now();
    let storage_timeout = Duration::from_secs(10);
    let stored_messages = loop {
        let messages = storage_client.get_vector().await?;
        let total_rows: usize = messages.iter().map(|m| m.record_batch().num_rows()).sum();
        if total_rows >= EXPECTED_TOTAL_RECORDS {
            break messages;
        }
        if storage_start.elapsed() > storage_timeout {
            anyhow::bail!(
                "timed out waiting for storage records: got {} rows in {} batches, expected {} rows",
                total_rows,
                messages.len(),
                EXPECTED_TOTAL_RECORDS
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    assert!(
        !stored_messages.is_empty(),
        "storage should contain at least one output batch"
    );
    let total_rows: usize = stored_messages
        .iter()
        .map(|m| m.record_batch().num_rows())
        .sum();
    assert_eq!(
        total_rows, EXPECTED_TOTAL_RECORDS,
        "storage should contain all expected output records"
    );
    for message in stored_messages {
        let batch = message.record_batch();
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .context("expected Utf8 output column in sink batch")?;
        assert_eq!(values.value(0), "v");
    }

    let down_output = Command::new("docker")
        .args([
            "compose",
            "-f",
            compose_file.to_str().unwrap(),
            "--env-file",
            compose_env_file.to_str().unwrap(),
            "-p",
            &compose_project,
            "down",
            "--volumes",
            "--remove-orphans",
        ])
        .output()
        .context("failed to run docker compose down")?;
    if !down_output.status.success() {
        anyhow::bail!(
            "docker compose down failed: stdout={} stderr={}",
            String::from_utf8_lossy(&down_output.stdout),
            String::from_utf8_lossy(&down_output.stderr)
        );
    }
    let _ = fs::remove_file(compose_env_file);
    let _ = fs::remove_file(spec_file);
    Ok(())
}
