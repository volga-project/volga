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
use crate::runtime::utils::scalar_value_to_bytes;

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
    let total_records: usize = 25_000;
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
    // For now, we only deeply compare window operator state (it is the only operator registered in OperatorStates).
    let mut checkpointed_window_states: HashMap<String, crate::runtime::operators::window::window_operator_state::WindowOperatorStateCheckpoint> = HashMap::new();
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

        // Capture window state blobs for deep comparison after restore
        if window_vertex_ids.contains(&task.vertex_id) {
            let cp_state_bytes = resp
                .blobs
                .iter()
                .find(|b| b.name == "window_operator_state")
                .map(|b| b.bytes.clone())
                .expect("window_operator_state blob missing");
            let cp: crate::runtime::operators::window::window_operator_state::WindowOperatorStateCheckpoint =
                bincode::deserialize(&cp_state_bytes)?;
            checkpointed_window_states.insert(task.vertex_id.clone(), cp);
        }
    }

    // "Crash" worker #1 after some time
    tokio::time::sleep(Duration::from_millis(1000)).await;
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

    // Assert restored window state matches checkpoint for *each* window task vertex (parallelism > 1)
    let op_states = worker2.operator_states();
    for window_vertex_id in &window_vertex_ids {
        let checkpointed_state = checkpointed_window_states
            .get(window_vertex_id)
            .unwrap_or_else(|| panic!("missing checkpointed window state for {}", window_vertex_id));

        let state_any = op_states
            .get_operator_state(window_vertex_id)
            .unwrap_or_else(|| panic!("Expected window operator state to be registered for {}", window_vertex_id));

        let window_state = state_any
            .as_any()
            .downcast_ref::<crate::runtime::operators::window::window_operator_state::WindowOperatorState>()
            .expect("Expected WindowOperatorState");

        let restored_state = window_state.to_checkpoint();
        assert_eq!(
            restored_state.keys.len(),
            checkpointed_state.keys.len(),
            "restored key state count must match checkpoint for {}",
            window_vertex_id
        );

        // Deep-ish compare: keys set + decoded WindowsState contents, ignoring HashMap iteration order.
        let mut cp_by_key: HashMap<Vec<u8>, crate::runtime::operators::window::window_operator_state::WindowsState> = HashMap::new();
        for (key_bytes, windows_state_bytes) in &checkpointed_state.keys {
            let ws: crate::runtime::operators::window::window_operator_state::WindowsState =
                bincode::deserialize(windows_state_bytes).expect("deserialize checkpointed WindowsState");
            cp_by_key.insert(key_bytes.clone(), ws);
        }

        let mut restored_by_key: HashMap<Vec<u8>, crate::runtime::operators::window::window_operator_state::WindowsState> = HashMap::new();
        for (key_bytes, windows_state_bytes) in &restored_state.keys {
            let ws: crate::runtime::operators::window::window_operator_state::WindowsState =
                bincode::deserialize(windows_state_bytes).expect("deserialize restored WindowsState");
            restored_by_key.insert(key_bytes.clone(), ws);
        }

        assert_eq!(
            cp_by_key.len(),
            restored_by_key.len(),
            "restored key set size must match checkpoint for {}",
            window_vertex_id
        );

        for (key_bytes, cp_ws) in cp_by_key {
            let restored_ws = restored_by_key
                .get(&key_bytes)
                .unwrap_or_else(|| panic!("missing restored key for {}: {:?}", window_vertex_id, key_bytes));

            // Compare time entries
            let cp_entries = cp_ws.time_entries.entries.iter().map(|e| *e).collect::<Vec<_>>();
            let restored_entries = restored_ws.time_entries.entries.iter().map(|e| *e).collect::<Vec<_>>();
            assert_eq!(cp_entries, restored_entries, "time_entries.entries mismatch for {}", window_vertex_id);

            let cp_batch_ids = cp_ws
                .time_entries
                .batch_ids
                .iter()
                .map(|e| (*e.key(), (*e.value()).as_ref().clone()))
                .collect::<Vec<_>>();
            let restored_batch_ids = restored_ws
                .time_entries
                .batch_ids
                .iter()
                .map(|e| (*e.key(), (*e.value()).as_ref().clone()))
                .collect::<Vec<_>>();
            assert_eq!(cp_batch_ids, restored_batch_ids, "time_entries.batch_ids mismatch for {}", window_vertex_id);

            // Compare per-window state
            assert_eq!(
                cp_ws.window_states.len(),
                restored_ws.window_states.len(),
                "window_states size mismatch for {}",
                window_vertex_id
            );

            for (wid, cp_wstate) in &cp_ws.window_states {
                let restored_wstate = restored_ws
                    .window_states
                    .get(wid)
                    .unwrap_or_else(|| panic!("missing window_id {} in restored state for {}", wid, window_vertex_id));

                assert_eq!(cp_wstate.start_idx, restored_wstate.start_idx, "start_idx mismatch for {}", window_vertex_id);
                assert_eq!(cp_wstate.end_idx, restored_wstate.end_idx, "end_idx mismatch for {}", window_vertex_id);

                // Tiles: compare serialized bytes (Tiles uses BTreeMap internally, so stable)
                let cp_tiles = cp_wstate.tiles.as_ref().map(|t| bincode::serialize(t).unwrap());
                let restored_tiles = restored_wstate.tiles.as_ref().map(|t| bincode::serialize(t).unwrap());
                assert_eq!(cp_tiles, restored_tiles, "tiles mismatch for {}", window_vertex_id);

                // Accumulator state: compare by ScalarValue protobuf bytes
                let cp_acc = cp_wstate.accumulator_state.as_ref().map(|v| {
                    v.iter()
                        .map(|sv| scalar_value_to_bytes(sv).unwrap())
                        .collect::<Vec<_>>()
                });
                let restored_acc = restored_wstate.accumulator_state.as_ref().map(|v| {
                    v.iter()
                        .map(|sv| scalar_value_to_bytes(sv).unwrap())
                        .collect::<Vec<_>>()
                });
                assert_eq!(cp_acc, restored_acc, "accumulator_state mismatch for {}", window_vertex_id);
            }
        }
    }

    // Continue running worker2 to finish
    worker2.signal_tasks_run().await;
    wait_for_status(&worker2, StreamTaskStatus::Finished, Duration::from_secs(60)).await;
    worker2.signal_tasks_close().await;
    wait_for_status(&worker2, StreamTaskStatus::Closed, Duration::from_secs(10)).await;
    worker2.close().await;

    // Validate output correctness (dedup by (timestamp, partition_key))
    let mut storage_client = InMemoryStorageClient::new(format!("http://{}", storage_server_addr)).await?;
    let messages = storage_client.get_vector().await?;

    let mut rows: BTreeMap<(i64, String), (f64, f64, i64, f64, usize)> = BTreeMap::new(); // ((ts, key) -> (value, sum, cnt, avg, duplicates_count))
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
            let t = ts.value(i);
            let k = key.value(i).to_string();
            let entry = (val.value(i), sum.value(i), cnt.value(i), avg.value(i));
            let dedup_key = (t, k.clone());
            let e = rows.entry(dedup_key).or_insert((entry.0, entry.1, entry.2, entry.3, 0));
            e.4 += 1;

            // allow at most 2 duplicates for each (timestamp, key) - one original and one replayed entry
            assert!(
                e.4 <= 2,
                "saw more than 2 duplicates for (timestamp, key)=({},{}) count={}",
                t,
                k,
                e.4
            );
            assert_eq!(e.0, entry.0, "value mismatch for duplicate (timestamp, key)");
            assert_eq!(e.1, entry.1, "sum mismatch for duplicate (timestamp, key)");
            assert_eq!(e.2, entry.2, "count mismatch for duplicate (timestamp, key)");
            assert_eq!(e.3, entry.3, "avg mismatch for duplicate (timestamp, key)");
        }
    }
    assert_eq!(rows.len(), total_records, "must have exactly one row per (timestamp, key) after dedup");

    let mut per_key_hist: HashMap<String, Vec<(i64, f64)>> = HashMap::new();
    for ((t, k), (v, sum, cnt, avg, _dup_count)) in rows.iter() {
        let hist = per_key_hist.entry(k.clone()).or_default();
        hist.push((*t, *v));

        let cutoff = *t - 2000;
        let window_vals: Vec<f64> = hist
            .iter()
            .filter(|(ts, _)| *ts >= cutoff)
            .map(|(_, vv)| *vv)
            .collect();
        let expected_sum: f64 = window_vals.iter().copied().sum();
        let expected_cnt: i64 = window_vals.len() as i64;
        let expected_avg: f64 = expected_sum / expected_cnt as f64;
        assert!(
            (expected_sum - *sum).abs() < 1e-9,
            "bad sum at t={}: key={}, expected_sum={}, got={}",
            t,
            k,
            expected_sum,
            sum
        );
        assert_eq!(
            expected_cnt, *cnt,
            "bad count at t={}: key={}, expected_cnt={}, got={}",
            t, k, expected_cnt, cnt
        );
        assert!(
            (expected_avg - *avg).abs() < 1e-9,
            "bad avg at t={}: key={}, expected_avg={}, got={}",
            t,
            k,
            expected_avg,
            avg
        );
    }

    master_server.stop().await;
    storage_server.stop().await;

    Ok(())
}


