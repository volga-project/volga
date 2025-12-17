use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use arrow::array::{Float64Array, StringArray, TimestampMillisecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::windows::BoundedWindowAggExec;
use datafusion::prelude::SessionContext;
use datafusion::common::ScalarValue;

use crate::api::planner::{Planner, PlanningContext};
use crate::common::message::{Message, WatermarkMessage};
use crate::common::{Key, MAX_WATERMARK_VALUE};
use crate::common::test_utils::gen_unique_grpc_port;
use crate::runtime::master_server::master_service::master_service_client::MasterServiceClient;
use crate::runtime::master_server::{MasterServer, TaskKey};
use crate::runtime::operators::operator::OperatorConfig;
use crate::runtime::operators::source::source_operator::{SourceConfig, VectorSourceConfig};
use crate::runtime::functions::source::datagen_source::{DatagenSourceConfig, FieldGenerator};
use crate::runtime::operators::window::window_operator::{UpdateMode, WindowOperatorConfig};
use crate::runtime::stream_task::StreamTaskStatus;
use crate::runtime::worker::{Worker, WorkerConfig};
use crate::transport::transport_backend_actor::TransportBackendType;
use crate::runtime::functions::key_by::key_by_function::extract_datafusion_window_exec;
use crate::runtime::functions::key_by::KeyByFunction;

fn create_input_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Millisecond, None), false),
        Field::new("value", DataType::Float64, false),
        Field::new("partition_key", DataType::Utf8, false),
    ]))
}

fn create_batch(ts: i64, value: f64, key: &str) -> RecordBatch {
    let schema = create_input_schema();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampMillisecondArray::from(vec![ts])),
            Arc::new(Float64Array::from(vec![value])),
            Arc::new(StringArray::from(vec![key])),
        ],
    )
    .unwrap()
}

async fn make_window_exec(sql: &str, schema: Arc<Schema>) -> Arc<BoundedWindowAggExec> {
    let ctx = SessionContext::new();
    let mut planner = Planner::new(PlanningContext::new(ctx));
    planner.register_source(
        "test_table".to_string(),
        SourceConfig::VectorSourceConfig(VectorSourceConfig::new(vec![])),
        schema,
    );
    extract_datafusion_window_exec(sql, &mut planner).await
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
    // Start master server
    let master_addr = format!("127.0.0.1:{}", gen_unique_grpc_port());
    let mut master_server = MasterServer::new();

    // Build a tiny pipeline: source(datagen) -> key_by -> window
    let schema = create_input_schema();
    let sql = "SELECT timestamp, value, partition_key, SUM(value) OVER w as sum_val \
               FROM test_table WINDOW w AS (PARTITION BY partition_key ORDER BY timestamp \
               RANGE BETWEEN INTERVAL '2000' MILLISECOND PRECEDING AND CURRENT ROW)";
    let window_exec = make_window_exec(sql, schema.clone()).await;

    let mut window_cfg = WindowOperatorConfig::new(window_exec);
    window_cfg.update_mode = UpdateMode::PerMessage;
    window_cfg.parallelize = false;

    // Datagen deterministic config (replayable)
    let mut fields = std::collections::HashMap::new();
    fields.insert(
        "timestamp".to_string(),
        FieldGenerator::IncrementalTimestamp {
            start_ms: 1000,
            step_ms: 1,
        },
    );
    fields.insert(
        "partition_key".to_string(),
        FieldGenerator::Key { num_unique: 2 },
    );
    fields.insert(
        "value".to_string(),
        FieldGenerator::Values {
            values: vec![
                ScalarValue::Float64(Some(1.0)),
                ScalarValue::Float64(Some(2.0)),
            ],
        },
    );

    let mut datagen_cfg = DatagenSourceConfig::new(schema.clone(), Some(20_000.0), None, Some(0.5), 256, fields);
    datagen_cfg.set_replayable(true);

    let operators = vec![
        OperatorConfig::SourceConfig(SourceConfig::DatagenSourceConfig(datagen_cfg)),
        OperatorConfig::KeyByConfig(KeyByFunction::new_arrow_key_by(vec!["partition_key".to_string()])),
        OperatorConfig::WindowConfig(window_cfg),
    ];

    let logical_graph = crate::api::logical_graph::LogicalGraph::from_linear_operators(operators, 1, false);
    let mut exec_graph = logical_graph.to_execution_graph();
    exec_graph.update_channels_with_node_mapping(None);

    let vertex_ids: Vec<String> = exec_graph.get_vertices().keys().cloned().collect();
    let expected_tasks = exec_graph
        .get_vertices()
        .values()
        .map(|v| TaskKey { vertex_id: v.vertex_id.clone(), task_index: v.task_index })
        .collect::<Vec<_>>();
    master_server.set_expected_tasks(expected_tasks).await;
    master_server.start(&master_addr).await?;

    // Identify window vertex id for assertions later
    let window_vertex_id = exec_graph
        .get_vertices()
        .values()
        .find(|v| matches!(v.operator_config, OperatorConfig::WindowConfig(_)))
        .unwrap()
        .vertex_id
        .clone();

    // Start worker #1
    let worker_config = WorkerConfig::new(
        "worker1".to_string(),
        exec_graph.clone(),
        vertex_ids.clone(),
        2,
        TransportBackendType::InMemory,
    )
    .with_master_addr(master_addr.clone());

    let mut worker = Worker::new(worker_config);
    worker.start().await;
    wait_for_status(&worker, StreamTaskStatus::Opened, Duration::from_secs(5)).await;
    worker.signal_tasks_run().await;

    // Let some records flow into window, then trigger checkpoint
    tokio::time::sleep(Duration::from_millis(100)).await;
    worker.trigger_checkpoint(1).await;

    // Wait for checkpoint to become complete on master
    let mut client = MasterServiceClient::connect(format!("http://{}", master_addr)).await?;
    let start = std::time::Instant::now();
    loop {
        let resp = client
            .get_latest_complete_checkpoint(tonic::Request::new(
                crate::runtime::master_server::master_service::GetLatestCompleteCheckpointRequest {},
            ))
            .await?
            .into_inner();
        if resp.has_checkpoint && resp.checkpoint_id == 1 {
            break;
        }
        if start.elapsed() > Duration::from_secs(5) {
            panic!("Timeout waiting for checkpoint completion");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Let worker finish naturally (watermark max)
    wait_for_status(&worker, StreamTaskStatus::Finished, Duration::from_secs(10)).await;
    worker.signal_tasks_close().await;
    wait_for_status(&worker, StreamTaskStatus::Closed, Duration::from_secs(10)).await;
    worker.close().await;

    // Start worker #2 with restore_checkpoint_id=1, but do not run tasks (we only need open+restore)
    let worker2_config = WorkerConfig::new(
        "worker2".to_string(),
        exec_graph,
        vertex_ids,
        2,
        TransportBackendType::InMemory,
    )
    .with_master_addr(master_addr.clone())
    .with_restore_checkpoint_id(1);

    let mut worker2 = Worker::new(worker2_config);
    worker2.start().await;
    wait_for_status(&worker2, StreamTaskStatus::Opened, Duration::from_secs(5)).await;

    // Assert window state restored (time index non-empty)
    let op_states = worker2.operator_states();
    let state_any = op_states
        .get_operator_state(&window_vertex_id)
        .expect("Expected window operator state to be registered");
    let window_state = state_any
        .as_any()
        .downcast_ref::<crate::runtime::operators::window::window_operator_state::WindowOperatorState>()
        .expect("Expected WindowOperatorState");

    // The restored window state should have at least some entries for some key
    assert!(
        window_state.to_checkpoint().keys.iter().any(|k| !k.time_entries.entries.is_empty()),
        "Expected restored window state to contain time entries"
    );

    worker2.signal_tasks_close().await;
    worker2.close().await;
    master_server.stop().await;

    Ok(())
}


