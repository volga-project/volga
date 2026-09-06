//! Layer C: request-mode cluster correctness (`env × backend × profile`).
//!
//! Windows are the first scenario. APIs are write path / read path / shared state.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow_integration_test::schema_to_json;
use datafusion::common::ScalarValue;
use serde_json::Value;

use crate::api::spec::connectors::{RequestSourceSinkSpec, SinkSpec, SourceSpec, SourceSpecKind};
use crate::api::spec::pipeline::ExecutionProfile;
use crate::api::spec::state::{OperatorStateBackendConfig, RequestStoreConfig, ScyllaConfig};
use crate::api::{ExecutionMode, PipelineSpecBuilder, TaskWorkerAssignmentStrategyType};
use crate::common::ports::gen_unique_grpc_port;
use crate::runtime::functions::source::datagen_source::{DatagenSpec, FieldGenerator, KeyDistribution};
use crate::runtime::observability::StreamTaskStatus;
use crate::test_utils::harness::{
    PipelineLaunchSpec, RuntimeEnv, VolgaCluster, WorkerKillMode,
};

const WINDOW_SQL: &str = r#"
SELECT event_time, key, value, SUM(value) OVER w AS sum_value
FROM events
WINDOW w AS (
  PARTITION BY key
  ORDER BY event_time
  RANGE BETWEEN INTERVAL '1' MINUTE PRECEDING AND CURRENT ROW
)
"#;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestStoreBackend {
    InMemoryGrpc,
    Scylla,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestProfile {
    Smoke,
    Stress,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestFault {
    None,
    KillWriteWorker,
    KillReadWorker,
}

#[derive(Debug, Clone)]
struct Workload {
    num_keys: usize,
    events_per_key: usize,
    request_concurrency: usize,
    step_ms: i64,
}

impl RequestProfile {
    fn workload(self) -> Workload {
        match self {
            Self::Smoke => Workload {
                num_keys: 4,
                events_per_key: 3,
                request_concurrency: 4,
                step_ms: 1_000,
            },
            Self::Stress => Workload {
                num_keys: 16,
                events_per_key: 8,
                request_concurrency: 16,
                step_ms: 1_000,
            },
        }
    }
}

pub async fn run_request_correctness(
    env: RuntimeEnv,
    backend: RequestStoreBackend,
    profile: RequestProfile,
) -> Result<()> {
    run_request_correctness_with_fault(env, backend, profile, RequestFault::None).await
}

pub async fn run_request_correctness_with_fault(
    env: RuntimeEnv,
    backend: RequestStoreBackend,
    profile: RequestProfile,
    fault: RequestFault,
) -> Result<()> {
    let workload = profile.workload();
    let request_bind = match env {
        RuntimeEnv::Local => format!("127.0.0.1:{}", gen_unique_grpc_port()),
        RuntimeEnv::Docker | RuntimeEnv::Kube => "0.0.0.0:18080".to_string(),
    };
    let request_url = match env {
        RuntimeEnv::Local => format!("http://{}/request", request_bind),
        RuntimeEnv::Docker | RuntimeEnv::Kube => {
            format!("http://127.0.0.1:18080/request")
        }
    };

    let (assignment, worker_count, parallelism) = match env {
        RuntimeEnv::Local | RuntimeEnv::Kube => (
            TaskWorkerAssignmentStrategyType::OperatorPerWorker,
            // 7 groups: write Source/KeyBy/Window + read KeyBy/WRO/Projection
            // + colocated request source/sink.
            7,
            1,
        ),
        RuntimeEnv::Docker => (
            TaskWorkerAssignmentStrategyType::Pipelined { slots_per_node: 2 },
            3,
            1,
        ),
    };

    let schema = Schema::new(vec![
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]);
    let rows = workload.num_keys * workload.events_per_key;
    let mut fields = HashMap::new();
    fields.insert(
        "event_time".to_string(),
        FieldGenerator::IncrementalTimestamp {
            start_ms: 1_000,
            step_ms: workload.step_ms,
        },
    );
    fields.insert(
        "key".to_string(),
        FieldGenerator::Key {
            num_unique_keys: workload.num_keys,
            distribution: KeyDistribution::Shared,
        },
    );
    fields.insert(
        "value".to_string(),
        FieldGenerator::Increment {
            start: ScalarValue::Float64(Some(1.0)),
            step: ScalarValue::Float64(Some(1.0)),
        },
    );

    let placeholder = String::new();
    let (operator_backend, request_store) = match backend {
        RequestStoreBackend::InMemoryGrpc => (
            OperatorStateBackendConfig::InMemoryGrpc {
                endpoint: placeholder.clone(),
            },
            RequestStoreConfig::InMemoryGrpc {
                endpoint: placeholder,
            },
        ),
        RequestStoreBackend::Scylla => {
            let cfg = ScyllaConfig {
                contact_points: std::env::var("VOLGA_SCYLLA_CONTACT")
                    .unwrap_or_else(|_| "127.0.0.1:9042".to_string())
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .collect(),
                keyspace: std::env::var("VOLGA_SCYLLA_KEYSPACE")
                    .unwrap_or_else(|_| "volga_window".to_string()),
                datacenter: std::env::var("VOLGA_SCYLLA_DC").ok(),
                max_parallelism: Some(parallelism),
            };
            (
                OperatorStateBackendConfig::Scylla(cfg.clone()),
                RequestStoreConfig::Scylla(cfg),
            )
        }
    };

    let pipeline = PipelineSpecBuilder::new()
        .with_parallelism(parallelism)
        .with_execution_profile(ExecutionProfile::MasterWorker {
            num_threads_per_task: 2,
        })
        .with_execution_mode(ExecutionMode::Request)
        .with_task_assignment_strategy(assignment)
        .with_operator_state_backend(operator_backend)
        .with_request_store(request_store)
        .with_source(SourceSpec::new(
            "events",
            SourceSpecKind::Datagen(DatagenSpec {
                rate: None,
                limit: Some(rows),
                run_for_s: None,
                batch_size: workload.events_per_key,
                fields,
                replayable: true,
            }),
            schema_to_json(&schema),
        ))
        .with_request_source_sink(RequestSourceSinkSpec {
            bind_address: request_bind,
            max_pending_requests: 1_024,
            request_timeout_ms: 30_000,
            schema_json: Some(schema_to_json(&schema)),
            sink: Some(SinkSpec::Request),
        })
        .sql(WINDOW_SQL)
        .with_checkpoint(Some(0), Some(60_000), Some(1))
        .build();

    let cluster = VolgaCluster::launch(
        env,
        PipelineLaunchSpec::new(pipeline, worker_count, None),
    )
    .await?;
    cluster.start_execution().await?;
    wait_for_write_path(&cluster, Duration::from_secs(30)).await?;

    if fault != RequestFault::None {
        let worker_id = pick_worker(&cluster, fault)?;
        cluster
            .worker(&worker_id)
            .ok_or_else(|| anyhow!("worker {worker_id} missing"))?
            .kill_with(WorkerKillMode::Abrupt)
            .await?;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let expected = expected_sums(workload.num_keys, workload.events_per_key);
    let client = reqwest::Client::new();
    // Query just after the last per-key event so every committed row is
    // historical (request value 0 must not replace the last ingest).
    let last_event_ts = 1_000 + ((workload.events_per_key as i64) - 1) * workload.step_ms;
    let request_ts = last_event_ts + 1;
    let mut handles = Vec::new();
    for key_idx in 0..workload.num_keys {
        for _ in 0..workload.request_concurrency / workload.num_keys.max(1) {
            let client = client.clone();
            let url = request_url.clone();
            let key = format!("key-{key_idx}");
            handles.push(tokio::spawn(async move {
                post_sum(&client, &url, &key, request_ts).await
            }));
        }
    }
    for handle in handles {
        let (key, sum) = handle.await??;
        let key_idx = key
            .strip_prefix("key-")
            .and_then(|s| s.parse::<usize>().ok())
            .context("key name")?;
        anyhow::ensure!(
            (sum - expected[key_idx]).abs() < 1e-6,
            "oracle mismatch key={key} got={sum} expected={}",
            expected[key_idx]
        );
    }

    cluster.shutdown().await
}

async fn wait_for_write_path(cluster: &VolgaCluster, timeout: Duration) -> Result<()> {
    let start = std::time::Instant::now();
    loop {
        if let Some(snapshot) = cluster.master().latest_pipeline_snapshot().await? {
            let source_finished = snapshot.worker_states.values().any(|worker| {
                worker.task_statuses.iter().any(|(id, status)| {
                    id.contains("Source")
                        && matches!(
                            status,
                            StreamTaskStatus::Finished | StreamTaskStatus::Closed
                        )
                })
            });
            if source_finished {
                tokio::time::sleep(Duration::from_millis(300)).await;
                return Ok(());
            }
        }
        if start.elapsed() > timeout {
            return Err(anyhow!("timed out waiting for write-path source to finish"));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn pick_worker(cluster: &VolgaCluster, fault: RequestFault) -> Result<String> {
    let ids = cluster.worker_ids();
    anyhow::ensure!(!ids.is_empty(), "no workers");
    match fault {
        RequestFault::None => Err(anyhow!("no fault")),
        RequestFault::KillWriteWorker => Ok(ids[0].clone()),
        RequestFault::KillReadWorker => Ok(ids.last().cloned().unwrap()),
    }
}

fn expected_sums(num_keys: usize, events_per_key: usize) -> Vec<f64> {
    // Per-key Increment starts at 1. Last per-key event is in-frame for all keys.
    let per_key: f64 = (events_per_key * (events_per_key + 1)) as f64 / 2.0;
    vec![per_key; num_keys]
}

async fn post_sum(client: &reqwest::Client, url: &str, key: &str, ts: i64) -> Result<(String, f64)> {
    let body = serde_json::json!({
        "data": {
            "payload": [{
                "event_time": ts,
                "key": key,
                "value": 0.0
            }]
        }
    });
    let response = client
        .post(url)
        .json(&body)
        .send()
        .await
        .with_context(|| format!("POST {url}"))?;
    anyhow::ensure!(
        response.status().is_success(),
        "request failed: {}",
        response.status()
    );
    let payload: Value = response.json().await?;
    let rows = payload
        .pointer("/data/payload")
        .or_else(|| payload.pointer("/data"))
        .ok_or_else(|| anyhow!("response missing data: {payload}"))?;
    let row = rows
        .as_array()
        .and_then(|a| a.first())
        .unwrap_or(rows);
    let sum = row
        .get("sum_value")
        .or_else(|| row.get("SUM(value)"))
        .and_then(|v| v.as_f64())
        .ok_or_else(|| anyhow!("response missing sum_value: {payload}"))?;
    Ok((key.to_string(), sum))
}
