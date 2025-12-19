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

#[tokio::test]
async fn test_manual_checkpoint_and_restore_window_state() -> Result<()> {
    let storage_server_addr = format!("127.0.0.1:{}", gen_unique_grpc_port());
    let mut storage_server = InMemoryStorageServer::new();
    storage_server.start(&storage_server_addr).await?;

    // Start master server
    let master_addr = format!("127.0.0.1:{}", gen_unique_grpc_port());
    let mut master_server = MasterServer::new();

    // Build pipeline via PipelineContextBuilder (like benchmarks): datagen -> window -> sink
    let schema = create_input_schema();
    let sql = "SELECT timestamp, value, partition_key, \
               SUM(value) OVER w as sum_val \
               FROM datagen_source \
               WINDOW w AS (PARTITION BY partition_key ORDER BY timestamp \
               RANGE BETWEEN INTERVAL '2000' MILLISECOND PRECEDING AND CURRENT ROW)";

    // Datagen deterministic config (replayable)
    let mut fields = HashMap::new();
    fields.insert("timestamp".to_string(), FieldGenerator::IncrementalTimestamp { start_ms: 1000, step_ms: 1 });
    fields.insert("partition_key".to_string(), FieldGenerator::Key { num_unique: 2 });
    fields.insert("value".to_string(), FieldGenerator::Values { values: vec![ScalarValue::Float64(Some(1.0)), ScalarValue::Float64(Some(2.0))] });

    let total_records: usize = 10_000;
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
        .with_parallelism(1)
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
    master_server.set_checkpointable_tasks(expected_tasks).await;
    master_server.start(&master_addr).await?;

    // Identify window vertex id for assertions later
    let window_vertex_id = exec_graph1
        .get_vertices()
        .values()
        .find(|v| matches!(v.operator_config, OperatorConfig::WindowConfig(_)))
        .unwrap()
        .vertex_id
        .clone();

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
    tokio::time::sleep(Duration::from_millis(150)).await;
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
        if start.elapsed() > Duration::from_secs(10) {
            panic!("Timeout waiting for checkpoint completion");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Fetch checkpointed window operator state from master for later comparison
    let resp = master_client
        .get_task_checkpoint(tonic::Request::new(
            crate::runtime::master_server::master_service::GetTaskCheckpointRequest {
                checkpoint_id: 1,
                vertex_id: window_vertex_id.clone(),
                task_index: 0,
            },
        ))
        .await?
        .into_inner();
    assert!(resp.success, "expected GetTaskCheckpoint success: {}", resp.error_message);
    let cp_state_bytes = resp
        .blobs
        .iter()
        .find(|b| b.name == "window_operator_state")
        .map(|b| b.bytes.clone())
        .expect("window_operator_state blob missing");
    let checkpointed_state: crate::runtime::operators::window::window_operator_state::WindowOperatorStateCheckpoint =
        bincode::deserialize(&cp_state_bytes)?;

    // "Crash" worker #1 (do not close gracefully)
    worker.kill_for_testing().await;

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

    // Assert window state restored matches checkpointed state (coarse check)
    let op_states = worker2.operator_states();
    let state_any = op_states
        .get_operator_state(&window_vertex_id)
        .expect("Expected window operator state to be registered");
    let window_state = state_any
        .as_any()
        .downcast_ref::<crate::runtime::operators::window::window_operator_state::WindowOperatorState>()
        .expect("Expected WindowOperatorState");
    let restored_state = window_state.to_checkpoint();
    assert_eq!(
        restored_state.keys.len(),
        checkpointed_state.keys.len(),
        "restored key state count must match checkpoint"
    );

    // Continue running worker2 to finish
    worker2.signal_tasks_run().await;
    wait_for_status(&worker2, StreamTaskStatus::Finished, Duration::from_secs(30)).await;
    worker2.signal_tasks_close().await;
    wait_for_status(&worker2, StreamTaskStatus::Closed, Duration::from_secs(10)).await;
    worker2.close().await;

    // Validate output correctness (dedup by timestamp)
    let mut storage_client = InMemoryStorageClient::new(format!("http://{}", storage_server_addr)).await?;
    let messages = storage_client.get_vector().await?;

    let mut rows: BTreeMap<i64, (f64, String, f64)> = BTreeMap::new();
    for msg in messages {
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

        for i in 0..batch.num_rows() {
            let t = ts.value(i);
            let entry = (val.value(i), key.value(i).to_string(), sum.value(i));
            if let Some(prev) = rows.get(&t) {
                assert_eq!(prev.0, entry.0);
                assert_eq!(prev.1, entry.1);
                assert_eq!(prev.2, entry.2);
            } else {
                rows.insert(t, entry);
            }
        }
    }
    assert_eq!(rows.len(), total_records, "must have exactly one row per timestamp after dedup");

    let mut per_key_hist: HashMap<String, Vec<(i64, f64)>> = HashMap::new();
    for (t, (v, k, sum)) in rows.iter() {
        let hist = per_key_hist.entry(k.clone()).or_default();
        hist.push((*t, *v));

        let cutoff = *t - 2000;
        let expected_sum: f64 = hist
            .iter()
            .filter(|(ts, _)| *ts >= cutoff)
            .map(|(_, vv)| *vv)
            .sum();
        assert!(
            (expected_sum - *sum).abs() < 1e-9,
            "bad sum at t={}: key={}, expected_sum={}, got={}",
            t,
            k,
            expected_sum,
            sum
        );
    }

    master_server.stop().await;
    storage_server.stop().await;

    Ok(())
}


