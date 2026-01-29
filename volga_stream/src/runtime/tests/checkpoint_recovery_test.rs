use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use anyhow::Result;
use datafusion::common::ScalarValue;
use crate::common::test_utils::gen_unique_grpc_port;
use crate::runtime::master_server::master_service::master_service_client::MasterServiceClient;
use crate::runtime::master_server::{MasterServer, TaskKey};
use crate::runtime::operators::operator::OperatorConfig;
use crate::runtime::operators::sink::sink_operator::SinkConfig;
use crate::runtime::operators::source::source_operator::SourceConfig;
use crate::runtime::observability::snapshot_types::StreamTaskStatus;
use crate::runtime::worker::{Worker, WorkerConfig};
use crate::transport::transport_backend_actor::TransportBackendType;
use crate::api::{compile_logical_graph, ExecutionMode, PipelineSpecBuilder};
use crate::control_plane::types::{AttemptId, PipelineExecutionContext};
use crate::runtime::functions::source::datagen_source::{DatagenSourceConfig, DatagenSpec, FieldGenerator};
use crate::storage::{InMemoryStorageClient, InMemoryStorageServer};
use crate::runtime::operators::operator::operator_config_requires_checkpoint;
// Watermark assigner placement is done by the planner (see LogicalGraph::to_execution_graph).

use crate::runtime::tests::test_utils::{create_window_input_schema, wait_for_status, window_rows_from_messages};
use crate::runtime::watermark::{TimeHint, WatermarkAssignConfig};

#[tokio::test]
async fn test_manual_checkpoint_and_restore() -> Result<()> {
    // TODO: This test passes individually but can fail when running the full suite (likely cross-test interference).
    crate::runtime::stream_task::MESSAGE_TRACE_ENABLED.store(true, std::sync::atomic::Ordering::Relaxed);

    let storage_server_addr = format!("127.0.0.1:{}", gen_unique_grpc_port());
    let mut storage_server = InMemoryStorageServer::new();
    storage_server.start(&storage_server_addr).await?;

    // Start master server
    let master_addr = format!("127.0.0.1:{}", gen_unique_grpc_port());
    let mut master_server = MasterServer::new();

    // Build pipeline via PipelineContextBuilder (like benchmarks): datagen -> window -> sink
    let schema = create_window_input_schema();
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

    let total_records: usize = 2_000;
    let datagen_cfg = DatagenSourceConfig::new(
        schema.clone(),
        DatagenSpec {
            rate: Some(1_000.0),
            limit: Some(total_records),
            run_for_s: None,
            batch_size: 256,
            fields,
            replayable: true,
        },
    );

    let out_of_orderness_ms: u64 = 100;
    let spec = PipelineSpecBuilder::new()
        .with_parallelism(parallelism)
        .with_execution_mode(ExecutionMode::Streaming)
        .with_source(
            "datagen_source".to_string(),
            SourceConfig::DatagenSourceConfig(datagen_cfg),
            schema.clone(),
        )
        .with_sink_inline(SinkConfig::InMemoryStorageGrpcSinkConfig(format!(
            "http://{}",
            storage_server_addr
        )))
        .sql(sql)
        .build();

    let mut logical_graph = compile_logical_graph(&spec);
    // Allow concurrency-induced reordering across tasks without dropping records as "late".
    // We size this based on records per task (1ms step) + small headroom.
    logical_graph.set_window_watermark_assign(WatermarkAssignConfig::new(
        out_of_orderness_ms,
        Some(TimeHint::WindowOrderByColumn),
    ));

    let mut exec_graph1 = logical_graph.to_execution_graph();
    // Apply lateness directly to window operators for this test (PipelineContext stays unaware).
    let lateness_ms: i64 = out_of_orderness_ms as i64;
    let vertex_ids_snapshot: Vec<_> = exec_graph1.get_vertices().keys().cloned().collect();
    for vid in vertex_ids_snapshot {
        if let Some(v) = exec_graph1.get_vertex_mut(vid.as_ref()) {
            if let OperatorConfig::WindowConfig(ref mut cfg) = v.operator_config {
                cfg.spec.lateness = Some(lateness_ms);
            }
        }
    }
    exec_graph1.update_channels_with_node_mapping(None);

    let vertex_ids_1 = exec_graph1.get_vertices().keys().cloned().collect();
    let expected_tasks = exec_graph1
        .get_vertices()
        .values()
        .filter(|v| operator_config_requires_checkpoint(&v.operator_config))
        .map(|v| TaskKey { vertex_id: v.vertex_id.as_ref().to_string(), task_index: v.task_index })
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
        .map(|v| v.vertex_id.as_ref().to_string())
        .collect();

    // Start worker #1
    let mut worker = Worker::new(
        WorkerConfig::new(
            "worker1".to_string(),
            PipelineExecutionContext::fresh(AttemptId(1)),
            exec_graph1,
            vertex_ids_1,
            2,
            TransportBackendType::InMemory,
        )
        .with_master_addr(master_addr.clone()),
    );
    worker.start().await;
    wait_for_status(&worker, StreamTaskStatus::Opened, Duration::from_secs(5)).await;
    // Queue checkpoint before run so sources pick it up on first iteration.
    worker.trigger_checkpoint(1).await;
    worker.signal_tasks_run().await;

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
    let vertex_ids_snapshot: Vec<_> = exec_graph2.get_vertices().keys().cloned().collect();
    for vid in vertex_ids_snapshot {
        if let Some(v) = exec_graph2.get_vertex_mut(vid.as_ref()) {
            if let OperatorConfig::WindowConfig(ref mut cfg) = v.operator_config {
                cfg.spec.lateness = Some(lateness_ms);
            }
        }
    }
    exec_graph2.update_channels_with_node_mapping(None);
    let vertex_ids_2 = exec_graph2.get_vertices().keys().cloned().collect();

    let mut worker2 = Worker::new(
        WorkerConfig::new(
            "worker2".to_string(),
            PipelineExecutionContext::fresh(AttemptId(2)),
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
    let run1_rows = window_rows_from_messages(messages_run1);
    let run2_rows = window_rows_from_messages(messages_run2);
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


