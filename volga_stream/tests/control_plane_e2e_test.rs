use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use datafusion::common::ScalarValue;
use http_body_util::BodyExt;
use tower::ServiceExt;

use base64::Engine as _;
use volga_stream::api::spec::connectors::{schema_to_ipc, DatagenSpec, SinkSpec, SourceBindingSpec, SourceSpec};
use volga_stream::api::{ExecutionMode, ExecutionProfile, PipelineSpecBuilder};
use volga_stream::common::test_utils::gen_unique_grpc_port;
use volga_stream::control_plane::controller::ControlPlaneController;
use volga_stream::control_plane::http::{router, ControlPlaneAppState};
use volga_stream::control_plane::store::InMemoryStore;
use volga_stream::control_plane::store::PipelineSnapshotStore;
use volga_stream::control_plane::types::{PipelineDesiredState, PipelineId, PipelineSpecId};
use volga_stream::runtime::functions::source::datagen_source::FieldGenerator;
use volga_stream::runtime::observability::PipelineSnapshotEntry;
use volga_stream::runtime::observability::snapshot_types::TaskOperatorMetrics;
use volga_stream::storage::InMemoryStorageServer;

#[derive(Debug, serde::Deserialize)]
struct CreatePipelineSpecResponse {
    pipeline_spec_id: PipelineSpecId,
}

#[derive(Debug, serde::Deserialize)]
struct StartPipelineResponse {
    execution_ids: volga_stream::control_plane::types::ExecutionIds,
}

#[tokio::test]
async fn test_control_plane_e2e_snapshots_and_metrics() {
    // Start sink target (in-memory storage gRPC server).
    let storage_addr = format!("127.0.0.1:{}", gen_unique_grpc_port());
    let mut storage_server = InMemoryStorageServer::new();
    storage_server.start(&storage_addr).await.unwrap();

    // Build a PipelineSpec that is fully serializable (no inline configs).
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new(
            "event_time",
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            false,
        ),
        arrow::datatypes::Field::new("key", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new("value", arrow::datatypes::DataType::Int64, false),
    ]));

    let mut fields = HashMap::new();
    fields.insert(
        "event_time".to_string(),
        FieldGenerator::IncrementalTimestamp {
            start_ms: 0,
            step_ms: 1,
        },
    );
    fields.insert("key".to_string(), FieldGenerator::Key { num_unique: 4 });
    fields.insert(
        "value".to_string(),
        FieldGenerator::Increment {
            start: ScalarValue::Int64(Some(1)),
            step: ScalarValue::Int64(Some(1)),
        },
    );

    let datagen = DatagenSpec {
        rate: None, // TODO should use real rate, this is prone to races
        limit: Some(500),
        run_for_s: None,
        batch_size: 50,
        fields: fields.into_iter().collect(),
        replayable: false,
    };

    let sql = r#"
        SELECT
            key,
            SUM(value) OVER (
                PARTITION BY key
                ORDER BY event_time
                RANGE BETWEEN INTERVAL '1000' MILLISECOND PRECEDING AND CURRENT ROW
            ) AS sum_v
        FROM t
    "#;

    let spec = PipelineSpecBuilder::new()
        .with_execution_profile(ExecutionProfile::Orchestrated { num_workers_per_operator: 1 })
        .with_execution_mode(ExecutionMode::Streaming)
        .with_parallelism(1)
        .with_snapshot_history_retention_window_ms(10_000)
        .add_source_binding(SourceBindingSpec {
            table_name: "t".to_string(),
            schema_ipc: schema_to_ipc(&schema),
            source: SourceSpec::Datagen(datagen),
        })
        .with_sink(SinkSpec::InMemoryStorageGrpc {
            server_addr: storage_addr.clone(),
        })
        .sql(sql)
        .build();

    let spec_bytes = serde_json::to_vec(&spec).unwrap();
    let spec_b64 = base64::engine::general_purpose::STANDARD.encode(spec_bytes);

    // Bring up Control Plane state (in-proc).
    let store = InMemoryStore::new();
    let controller = Arc::new(ControlPlaneController::new(
        Arc::new(store.clone()),
        Arc::new(volga_stream::executor::local_runtime_adapter::LocalRuntimeAdapter::new()),
    ));
    let app_state = ControlPlaneAppState::new(store.clone(), controller.clone());
    let app = router(app_state);

    // 1) Create spec.
    let create_resp = http_post_json::<CreatePipelineSpecResponse>(
        &app,
        "/pipeline_specs",
        &serde_json::json!({ "spec_bytes_b64": spec_b64 }),
    )
    .await;

    // 2) Start pipeline (registers run + desired state).
    let start_resp = http_post_json::<StartPipelineResponse>(
        &app,
        "/pipelines/start",
        &serde_json::json!({
            "pipeline_spec_id": create_resp.pipeline_spec_id,
            "desired_state": PipelineDesiredState::Running
        }),
    )
    .await;

    // 3) Reconcile once to launch attempt + start snapshot poller.
    controller.reconcile_once().await.unwrap();

    // 4) Wait until CP has at least one snapshot.
    let pipeline_id = start_resp.execution_ids.pipeline_id;
    let _latest = wait_for_snapshot(&store, pipeline_id, Duration::from_secs(15)).await;

    // 5) Validate CP HTTP snapshot endpoints.
    let latest: PipelineSnapshotEntry = http_get_json(
        &app,
        &format!("/pipelines/{}/snapshot", pipeline_id.0),
    )
    .await;
    assert!(latest.seq > 0);
    assert!(latest.ts_ms > 0);
    assert!(!latest.snapshot.worker_states.is_empty());

    // Ensure we see window task operator metrics (storage snapshot) in worker snapshots.
    let mut saw_window_metrics = false;
    for (_worker_id, ws) in latest.snapshot.worker_states.iter() {
        if !ws.task_operator_metrics.is_empty() {
            for (_v, m) in ws.task_operator_metrics.iter() {
                if matches!(m, TaskOperatorMetrics::Window { .. }) {
                    saw_window_metrics = true;
                }
            }
        }
        // Also ensure common aggregated metrics are present once polling begins.
        assert!(ws.worker_metrics.is_some());
    }
    assert!(saw_window_metrics, "expected window task operator metrics to be present");

    // History endpoint should return non-empty history.
    let history: Vec<PipelineSnapshotEntry> = http_get_json(
        &app,
        &format!("/pipelines/{}/snapshots?limit=10", pipeline_id.0),
    )
    .await;
    assert!(!history.is_empty());

    storage_server.stop().await;
}

async fn http_post_json<T: serde::de::DeserializeOwned>(
    app: &axum::Router,
    path: &str,
    body: &impl serde::Serialize,
) -> T {
    let req = Request::builder()
        .method("POST")
        .uri(path)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(body).unwrap()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

async fn http_get_json<T: serde::de::DeserializeOwned>(app: &axum::Router, path: &str) -> T {
    let req = Request::builder()
        .method("GET")
        .uri(path)
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

async fn wait_for_snapshot(
    store: &InMemoryStore,
    pipeline_id: PipelineId,
    timeout: Duration,
) -> PipelineSnapshotEntry {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(s) = store.latest_snapshot(pipeline_id).await {
            return s;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("timeout waiting for CP snapshot");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

