use std::collections::HashMap;
use std::process::Command;
use std::time::Duration;

use anyhow::{Context, Result};
use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow_integration_test::schema_to_json;
use datafusion::common::ScalarValue;
use testcontainers::clients::Cli;
use testcontainers::core::WaitFor;
use testcontainers::{GenericImage, RunnableImage};
use uuid::Uuid;

use crate::api::spec::connectors::{SinkSpec, SourceSpec, SourceSpecKind};
use crate::api::spec::pipeline::ExecutionProfile;
use crate::api::{compile_logical_graph, PipelineSpecBuilder, TaskWorkerAssignmentStrategyType};
use crate::common::test_utils::gen_unique_grpc_port;
use crate::runtime::functions::source::datagen_source::{DatagenSpec, FieldGenerator};
use crate::runtime::master_server::master_service::master_service_client::MasterServiceClient;
use crate::runtime::master_server::master_service::GetLatestPipelineSnapshotRequest;
use crate::runtime::observability::{PipelineSnapshot, StreamTaskStatus};
use crate::storage::{InMemoryStorageClient, InMemoryStorageServer};

const MASTER_CONTAINER_PORT: u16 = 50051;
const WORKER_CONTROL_PORT: u16 = 50052;
const WORKER_TRANSPORT_PORT: u16 = 60052;
const WORKER_HOST_PREFIX_BASE: &str = "volga-worker-";
const NUM_BATCHES: usize = 2;
const BATCH_SIZE: usize = 5;
const EXPECTED_RECORDS: usize = NUM_BATCHES * BATCH_SIZE;

const BIND_HOST: &str = "0.0.0.0";
const HOST_DOCKER_INTERNAL: &str = "host.docker.internal";

fn build_test_pipeline_spec_json(sink_server_addr: &str) -> Result<(String, usize)> {
    let schema = Schema::new(vec![Field::new("value", DataType::Utf8, false)]);
    let mut fields = HashMap::new();
    fields.insert(
        "value".to_string(),
        FieldGenerator::Values {
            values: vec![ScalarValue::Utf8(Some("v".to_string()))],
        },
    );
    let spec = PipelineSpecBuilder::new()
        .with_parallelism(1)
        .with_execution_profile(ExecutionProfile::MasterWorker {
            num_threads_per_task: 4,
        })
        .with_node_assignment_strategy(TaskWorkerAssignmentStrategyType::OperatorPerWorker)
        .with_source(
            SourceSpec::new(
                "test_table",
                SourceSpecKind::Datagen(DatagenSpec {
                rate: Some(1.0),
                limit: Some(EXPECTED_RECORDS),
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
    let logical_graph = compile_logical_graph(&spec, None);
    let expected_workers = logical_graph.get_nodes().count();
    // This query shape is source -> projection -> sink, so OperatorPerWorker needs 3 workers.
    assert_eq!(
        expected_workers, 3,
        "expected 3 logical nodes/workers for test query"
    );
    let spec_json =
        serde_json::to_string(&spec).context("failed to serialize test pipeline spec")?;
    Ok((spec_json, expected_workers))
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

fn read_container_logs(container_id: &str) -> String {
    match Command::new("docker")
        .args(["logs", "--tail", "200", container_id])
        .output()
    {
        Ok(output) => {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            format!(
                "docker logs for container {}:\nstdout:\n{}\nstderr:\n{}",
                container_id, stdout, stderr
            )
        }
        Err(e) => format!(
            "failed to read docker logs for container {}: {}",
            container_id, e
        ),
    }
}

async fn connect_master_with_retry(
    master_port: u16,
    master_container_id: &str,
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
                    let logs = read_container_logs(master_container_id);
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

// Manual smoke test: requires local Docker daemon and can be slow.
#[tokio::test]
#[ignore]
async fn test_docker_master_and_workers_smoke() -> Result<()> {
    ensure_test_image()?; // comment this if rebuilding the image is not needed

    let storage_port = gen_unique_grpc_port();
    let storage_addr = format!("127.0.0.1:{}", storage_port);
    let mut storage_server = InMemoryStorageServer::new();
    storage_server.start(&storage_addr).await?;

    let docker = Cli::default();
    let pipeline_id = Uuid::new_v4().to_string();
    let network_name = format!("volga-net-{}", Uuid::new_v4());
    let master_container_name = format!("volga-master-{}", Uuid::new_v4());
    let worker_host_prefix = format!("{}{}-", WORKER_HOST_PREFIX_BASE, Uuid::new_v4().simple());
    let storage_addr_for_workers = format!("http://{}:{}", HOST_DOCKER_INTERNAL, storage_port);
    let (spec_json, expected_workers) = build_test_pipeline_spec_json(&storage_addr_for_workers)?;
    let expected_workers_str = expected_workers.to_string();
    let master_bind_addr = format!("{}:{}", BIND_HOST, MASTER_CONTAINER_PORT);
    let worker_bind_addr = format!("{}:{}", BIND_HOST, WORKER_CONTROL_PORT);
    let worker_control_port_str = WORKER_CONTROL_PORT.to_string();
    let worker_transport_port_str = WORKER_TRANSPORT_PORT.to_string();

    let master_image = GenericImage::new("volga", "test")
        .with_exposed_port(MASTER_CONTAINER_PORT)
        .with_wait_for(WaitFor::seconds(3))
        .with_env_var("VOLGA_MASTER_BIND_ADDR", &master_bind_addr)
        .with_env_var("VOLGA_MASTER_HOLD_ON_FINISH", "true")
        .with_env_var("VOLGA_PIPELINE_ID", &pipeline_id)
        .with_env_var("VOLGA_WORKER_COUNT", &expected_workers_str)
        .with_env_var("VOLGA_WORKER_HOST_PREFIX", &worker_host_prefix)
        .with_env_var("VOLGA_WORKER_PORT", &worker_control_port_str)
        .with_env_var("VOLGA_WORKER_TRANSPORT_PORT", &worker_transport_port_str)
        .with_env_var("VOLGA_PIPELINE_SPEC_JSON", &spec_json);

    let master = docker.run(
        RunnableImage::from((master_image, vec!["volga-master".to_string()]))
            .with_network(network_name.clone())
            .with_container_name(master_container_name.clone()),
    );
    let master_port = master.get_host_port_ipv4(MASTER_CONTAINER_PORT);
    let master_addr_for_workers = format!("{}:{}", master_container_name, MASTER_CONTAINER_PORT);

    let worker_image = GenericImage::new("volga", "test")
        .with_wait_for(WaitFor::seconds(2))
        .with_env_var("VOLGA_WORKER_BIND_ADDR", &worker_bind_addr)
        .with_env_var("VOLGA_MASTER_SERVICE_ADDR", &master_addr_for_workers)
        .with_env_var("VOLGA_WORKER_HOLD_ON_FINISH", "true")
        .with_env_var("VOLGA_WORKER_HOST_PREFIX", &worker_host_prefix);

    let mut workers = Vec::with_capacity(expected_workers);
    for i in 0..expected_workers {
        let worker_container_name = format!("{}{}", worker_host_prefix, i);
        let container = docker.run(
            RunnableImage::from((
                worker_image
                    .clone()
                    .with_env_var("VOLGA_WORKER_INDEX", i.to_string()),
                vec!["volga-worker".to_string()],
            ))
            .with_network(network_name.clone())
            .with_container_name(worker_container_name),
        );
        workers.push(container);
    }

    let mut master_client = connect_master_with_retry(master_port, master.id()).await?;
    let start = tokio::time::Instant::now();
    let snapshot_timeout = Duration::from_secs(10);
    let mut last_progress_log = tokio::time::Instant::now();
    let snapshot: PipelineSnapshot = loop {
        let resp = master_client
            .get_latest_pipeline_snapshot(tonic::Request::new(GetLatestPipelineSnapshotRequest {}))
            .await
            .context("master service snapshot request failed")?
            .into_inner();
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

    let mut storage_client = InMemoryStorageClient::new(format!("http://{}", storage_addr)).await?;
    let storage_start = tokio::time::Instant::now();
    let storage_timeout = Duration::from_secs(10);
    let stored_messages = loop {
        let messages = storage_client.get_vector().await?;
        let total_rows: usize = messages.iter().map(|m| m.record_batch().num_rows()).sum();
        if total_rows >= EXPECTED_RECORDS {
            break messages;
        }
        if storage_start.elapsed() > storage_timeout {
            anyhow::bail!(
                "timed out waiting for storage records: got {} rows in {} batches, expected {} rows",
                total_rows,
                messages.len(),
                EXPECTED_RECORDS
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    assert_eq!(
        stored_messages.len(),
        NUM_BATCHES,
        "storage should contain expected number of output batches"
    );
    let total_rows: usize = stored_messages
        .iter()
        .map(|m| m.record_batch().num_rows())
        .sum();
    assert_eq!(
        total_rows, EXPECTED_RECORDS,
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

    drop(workers);
    drop(master);
    storage_server.stop().await;
    Ok(())
}
