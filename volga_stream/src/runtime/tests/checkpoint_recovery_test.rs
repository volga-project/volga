use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use arrow::array::{Float64Array, StringArray, TimestampMillisecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::common::ScalarValue;
use crate::common::test_utils::gen_unique_grpc_port;
use crate::runtime::master_server::master_service::master_service_client::MasterServiceClient;
use crate::runtime::master_server::{MasterServer, TaskKey};
use crate::runtime::operators::operator::OperatorConfig;
use crate::runtime::operators::sink::sink_operator::SinkConfig;
use crate::runtime::operators::source::source_operator::SourceConfig;
use crate::runtime::stream_task::StreamTaskStatus;
use crate::runtime::worker::{Worker, WorkerConfig};
use crate::transport::transport_backend_actor::TransportBackendType;
use crate::api::pipeline_context::{ExecutionMode, PipelineContextBuilder};
use crate::runtime::functions::source::datagen_source::{DatagenSourceConfig, FieldGenerator};
use crate::storage::{InMemoryStorageClient, InMemoryStorageServer};
use crate::runtime::operators::operator::operator_config_requires_checkpoint;

fn create_input_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Millisecond, None), false),
        Field::new("value", DataType::Float64, false),
        Field::new("partition_key", DataType::Utf8, false),
    ]))
}

async fn wait_for_status(worker: &Worker, status: StreamTaskStatus, timeout: Duration) {
    let start = std::time::Instant::now();
    loop {
        let st = worker.get_state().await;
        if !st.task_statuses.is_empty() && st.all_tasks_have_status(status) {
            return;
        }
        if start.elapsed() > timeout {
            panic!("Timeout waiting for {:?}, state = {:?}", status, st.task_statuses);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct OutputRow {
    trace: Option<String>,
    upstream_vertex_id: Option<String>,
    upstream_task_index: Option<i32>,
    timestamp_ms: i64,
    partition_key: String,
    value: f64,
    sum: f64,
    cnt: i64,
    avg: f64,
}

fn parse_task_index_from_vertex_id(vertex_id: &str) -> Option<i32> {
    // Common format in this codebase is like "Window_1_2" where the last segment is task index.
    vertex_id.rsplit('_').next()?.parse::<i32>().ok()
}

fn rows_from_messages(messages: Vec<crate::common::message::Message>) -> Vec<OutputRow> {
    let mut out = Vec::new();
    for msg in messages {
        let extras = msg.get_extras().unwrap_or_default();
        let trace = extras.get("trace").cloned();

        let upstream_vertex_id = msg.upstream_vertex_id();
        let upstream_task_index = upstream_vertex_id
            .as_deref()
            .and_then(parse_task_index_from_vertex_id);

        let batch = msg.record_batch();
        let ts = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("timestamp col");
        let val = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("value col");
        let key = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("key col");
        let sum = batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("sum col");
        let cnt = batch
            .column(4)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("cnt col");
        let avg = batch
            .column(5)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("avg col");

        for i in 0..batch.num_rows() {
            out.push(OutputRow {
                trace: trace.clone(),
                upstream_vertex_id: upstream_vertex_id.clone(),
                upstream_task_index,
                timestamp_ms: ts.value(i),
                partition_key: key.value(i).to_string(),
                value: val.value(i),
                sum: sum.value(i),
                cnt: cnt.value(i),
                avg: avg.value(i),
            });
        }
    }
    out
}

#[tokio::test]
async fn test_manual_checkpoint_and_restore() -> Result<()> {
    crate::runtime::stream_task::MESSAGE_TRACE_ENABLED.store(true, std::sync::atomic::Ordering::Relaxed);

    let storage_server_addr = format!("127.0.0.1:{}", gen_unique_grpc_port());
    let mut storage_server = InMemoryStorageServer::new();
    storage_server.start(&storage_server_addr).await?;

    // Start master server
    let master_addr = format!("127.0.0.1:{}", gen_unique_grpc_port());
    let mut master_server = MasterServer::new();

    // Build pipeline via PipelineContextBuilder (like benchmarks): datagen -> window -> sink
    let schema = create_input_schema();
    let sql = "SELECT timestamp, value, partition_key, \
               SUM(value) OVER w as sum_val, \
               COUNT(value) OVER w as cnt_val, \
               AVG(value) OVER w as avg_val \
               FROM datagen_source \
               WINDOW w AS (PARTITION BY partition_key ORDER BY timestamp \
               RANGE BETWEEN INTERVAL '2000' MILLISECOND PRECEDING AND CURRENT ROW)";

    let parallelism: usize = 4;

    // Datagen deterministic config (replayable)
    let mut fields = HashMap::new();
    fields.insert("timestamp".to_string(), FieldGenerator::IncrementalTimestamp { start_ms: 1000, step_ms: 1 });
    // Ensure all tasks get assigned keys so num_unique > parallelism.
    fields.insert("partition_key".to_string(), FieldGenerator::Key { num_unique: parallelism * 16 });
    fields.insert("value".to_string(), FieldGenerator::Values { values: vec![ScalarValue::Float64(Some(1.0)), ScalarValue::Float64(Some(2.0))] });

    // 5k records/sec for ~5 seconds => ~25k total records (across all tasks)
    let total_records: usize = 15_000;
    let mut datagen_cfg = DatagenSourceConfig::new(
        schema.clone(),
        Some(5_000.0),
        Some(total_records),
        None,
        256,
        fields,
    );
    datagen_cfg.set_replayable(true);

    let context = PipelineContextBuilder::new()
        .with_parallelism(parallelism)
        .with_source("datagen_source".to_string(), SourceConfig::DatagenSourceConfig(datagen_cfg), schema.clone())
        .with_sink(SinkConfig::InMemoryStorageGrpcSinkConfig(format!("http://{}", storage_server_addr)))
        .with_execution_mode(ExecutionMode::Streaming)
        .sql(sql)
        .build();

    let logical_graph = context.get_logical_graph().unwrap().clone();

    let mut exec_graph1 = logical_graph.to_execution_graph();
    exec_graph1.update_channels_with_node_mapping(None);

    let vertex_ids_1: Vec<String> = exec_graph1.get_vertices().keys().cloned().collect();
    let expected_tasks = exec_graph1
        .get_vertices()
        .values()
        .filter(|v| operator_config_requires_checkpoint(&v.operator_config))
        .map(|v| TaskKey { vertex_id: v.vertex_id.clone(), task_index: v.task_index })
        .collect::<Vec<_>>();
    master_server
        .set_checkpointable_tasks(expected_tasks.clone())
        .await;
    master_server.start(&master_addr).await?;

    // Track all window vertices for state verification (parallelism > 1 => multiple task vertices)
    let window_vertex_ids: Vec<String> = exec_graph1
        .get_vertices()
        .values()
        .filter(|v| matches!(v.operator_config, OperatorConfig::WindowConfig(_)))
        .map(|v| v.vertex_id.clone())
        .collect();

    // Start worker #1
    let mut worker = Worker::new(
        WorkerConfig::new(
            "worker1".to_string(),
            exec_graph1,
            vertex_ids_1,
            2,
            TransportBackendType::InMemory,
        )
        .with_master_addr(master_addr.clone()),
    );
    worker.start().await;
    wait_for_status(&worker, StreamTaskStatus::Opened, Duration::from_secs(5)).await;
    worker.signal_tasks_run().await;

    // Trigger checkpoint while pipeline is running
    tokio::time::sleep(Duration::from_millis(1000)).await;
    worker.trigger_checkpoint(1).await;

    // Wait for checkpoint to become complete on master
    let mut master_client = MasterServiceClient::connect(format!("http://{}", master_addr)).await?;
    let start = std::time::Instant::now();
    loop {
        let resp = master_client
            .get_latest_complete_checkpoint(tonic::Request::new(
                crate::runtime::master_server::master_service::GetLatestCompleteCheckpointRequest {},
            ))
            .await?
            .into_inner();
        if resp.has_checkpoint && resp.checkpoint_id == 1 {
            break;
        }
        if start.elapsed() > Duration::from_secs(20) {
            panic!("Timeout waiting for checkpoint completion");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Fetch checkpointed state for all checkpointable vertices from master for later comparison.
    let mut checkpointed_window_key_counts: HashMap<String, usize> = HashMap::new();
    for task in &expected_tasks {
        let resp = master_client
            .get_task_checkpoint(tonic::Request::new(
                crate::runtime::master_server::master_service::GetTaskCheckpointRequest {
                    checkpoint_id: 1,
                    vertex_id: task.vertex_id.clone(),
                    task_index: task.task_index,
                },
            ))
            .await?
            .into_inner();
        assert!(
            resp.success,
            "expected GetTaskCheckpoint success for {}: {}",
            task.vertex_id,
            resp.error_message
        );

        // Ensure all checkpointable vertices actually reported *something*
        assert!(
            !resp.blobs.is_empty(),
            "expected at least one blob for checkpointable vertex {}",
            task.vertex_id
        );

        if window_vertex_ids.contains(&task.vertex_id) {
            let cp_state_bytes = resp
                .blobs
                .iter()
                .find(|b| b.name == "window_operator_state")
                .map(|b| b.bytes.as_slice())
                .expect("window_operator_state blob missing");
            let cp: crate::runtime::operators::window::window_operator_state::WindowOperatorStateCheckpoint =
                bincode::deserialize(cp_state_bytes)?;
            checkpointed_window_key_counts.insert(task.vertex_id.clone(), cp.keys.len());
        }
    }

    // "Crash" worker #1 after some time
    tokio::time::sleep(Duration::from_millis(1000)).await;
    // Best-effort: allow sink to flush buffered output before crash.
    tokio::time::sleep(Duration::from_millis(250)).await;
    worker.kill_for_testing().await;

    // Drain storage after worker #1 so worker #2 starts with a clean sink buffer.
    let mut storage_client = InMemoryStorageClient::new(format!("http://{}", storage_server_addr)).await?;
    let messages_run1 = storage_client.drain_vector().await?;

    // Worker #2 uses fresh channels
    let mut exec_graph2 = logical_graph.to_execution_graph();
    exec_graph2.update_channels_with_node_mapping(None);
    let vertex_ids_2: Vec<String> = exec_graph2.get_vertices().keys().cloned().collect();

    let mut worker2 = Worker::new(
        WorkerConfig::new(
            "worker2".to_string(),
            exec_graph2,
            vertex_ids_2,
            2,
            TransportBackendType::InMemory,
        )
        .with_master_addr(master_addr.clone())
        .with_restore_checkpoint_id(1),
    );
    worker2.start().await;
    wait_for_status(&worker2, StreamTaskStatus::Opened, Duration::from_secs(5)).await;

    // Basic restore sanity: window operator state should be registered for each window task vertex.
    let op_states = worker2.operator_states();
    for window_vertex_id in &window_vertex_ids {
        let checkpointed_key_count = checkpointed_window_key_counts
            .get(window_vertex_id)
            .unwrap_or_else(|| panic!("missing checkpointed window key count for {}", window_vertex_id));

        let state_any = op_states
            .get_operator_state(window_vertex_id)
            .unwrap_or_else(|| panic!("Expected window operator state to be registered for {}", window_vertex_id));

        let window_state = state_any
            .as_any()
            .downcast_ref::<crate::runtime::operators::window::window_operator_state::WindowOperatorState>()
            .expect("Expected WindowOperatorState");

        // Ensure we can serialize state after restore and that key count matches checkpoint.
        let restored_state = window_state.to_checkpoint();
        assert_eq!(
            restored_state.keys.len(),
            *checkpointed_key_count,
            "restored key count must match checkpoint for {}",
            window_vertex_id
        );
    }

    // Continue running worker2 to finish
    worker2.signal_tasks_run().await;
    wait_for_status(&worker2, StreamTaskStatus::Finished, Duration::from_secs(60)).await;
    worker2.signal_tasks_close().await;
    wait_for_status(&worker2, StreamTaskStatus::Closed, Duration::from_secs(10)).await;
    worker2.close().await;

    // Drain storage after worker #2.
    let mut storage_client = InMemoryStorageClient::new(format!("http://{}", storage_server_addr)).await?;
    let messages_run2 = storage_client.drain_vector().await?;

    // Convert to row-level records with metadata for debugging.
    let run1_rows = rows_from_messages(messages_run1);
    let run2_rows = rows_from_messages(messages_run2);
    println!(
        "[CHECKPOINT_TEST] drained rows: run1={} run2={}",
        run1_rows.len(),
        run2_rows.len()
    );

    // Merge both runs and validate basic correctness (dedup by (timestamp, partition_key)).
    // We intentionally do NOT validate window aggregates here; this test is meant to exercise
    // checkpointing/recovery plumbing + deterministic input replay and avoid brittle aggregate matching.
    let mut all_rows = Vec::new();
    all_rows.extend(run1_rows);
    all_rows.extend(run2_rows);

    let mut dedup_counts: BTreeMap<(i64, String), usize> = BTreeMap::new();
    for r in all_rows {
        let k = (r.timestamp_ms, r.partition_key.clone());
        *dedup_counts.entry(k).or_insert(0) += 1;

        // allow at most 2 duplicates for each (timestamp, key) - one original and one replayed entry
        let count = dedup_counts[&(r.timestamp_ms, r.partition_key.clone())];
        assert!(
            count <= 2,
            "saw more than 2 duplicates for (timestamp, key)=({},{}) count={} row={:#?}",
            r.timestamp_ms,
            r.partition_key,
            count,
            r
        );
    }
    assert_eq!(
        dedup_counts.len(),
        total_records,
        "must have exactly one unique (timestamp, key) per generated record after dedup"
    );

    master_server.stop().await;
    storage_server.stop().await;

    Ok(())
}


