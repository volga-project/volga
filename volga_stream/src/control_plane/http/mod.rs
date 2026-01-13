use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use base64::{engine::general_purpose, Engine as _};

use crate::api::PipelineSpec as UserPipelineSpec;
use crate::api::compile_logical_graph;
use crate::control_plane::controller::ControlPlaneController;
use crate::control_plane::store::{InMemoryStore, PipelineEventStore, PipelineRunStore, PipelineSpecStore, PipelineSnapshotStore};
use crate::control_plane::types::{
    AttemptId, ExecutionIds, PipelineDesiredState, PipelineEvent, PipelineEventKind, PipelineId,
    PipelineLifecycleState, PipelineRun, PipelineSpec, PipelineSpecId, PipelineStatus,
};
use crate::runtime::observability::{PipelineDerivedStats, PipelineSnapshotEntry};

#[derive(Clone)]
pub struct ControlPlaneAppState {
    pub store: Arc<InMemoryStore>,
    pub controller: Arc<ControlPlaneController>,
}

impl ControlPlaneAppState {
    pub fn new(store: InMemoryStore, controller: Arc<ControlPlaneController>) -> Self {
        Self {
            store: Arc::new(store),
            controller,
        }
    }
}

pub fn router(state: ControlPlaneAppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/pipeline_specs", post(create_pipeline_spec))
        .route("/pipelines/start", post(start_pipeline))
        .route("/pipelines/:pipeline_id/status", get(get_status))
        .route("/pipelines/:pipeline_id/snapshot", get(get_latest_snapshot))
        .route("/pipelines/:pipeline_id/snapshots", get(get_snapshot_history))
        .route("/pipelines/:pipeline_id/stats", get(get_snapshot_stats))
        .with_state(state)
}

async fn health() -> impl IntoResponse {
    StatusCode::OK
}

#[derive(Debug, Deserialize)]
pub struct CreatePipelineSpecRequest {
    pub spec_bytes_b64: String,
}

#[derive(Debug, Serialize)]
pub struct CreatePipelineSpecResponse {
    pub pipeline_spec_id: PipelineSpecId,
}

async fn create_pipeline_spec(
    State(state): State<ControlPlaneAppState>,
    Json(req): Json<CreatePipelineSpecRequest>,
) -> Result<Json<CreatePipelineSpecResponse>, StatusCode> {
    let pipeline_spec_id = PipelineSpecId(Uuid::new_v4());
    let bytes = general_purpose::STANDARD
        .decode(req.spec_bytes_b64)
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    let spec: UserPipelineSpec =
        serde_json::from_slice(&bytes).map_err(|_| StatusCode::BAD_REQUEST)?;
    let bytes = serde_json::to_vec(&spec).map_err(|_| StatusCode::BAD_REQUEST)?;

    state
        .store
        .put_spec(PipelineSpec {
            pipeline_spec_id,
            created_at: Utc::now(),
            spec_bytes: bytes,
        })
        .await;
    Ok(Json(CreatePipelineSpecResponse { pipeline_spec_id }))
}

#[derive(Debug, Deserialize)]
pub struct StartPipelineRequest {
    pub pipeline_spec_id: PipelineSpecId,
    pub desired_state: Option<PipelineDesiredState>,
}

#[derive(Debug, Serialize)]
pub struct StartPipelineResponse {
    pub execution_ids: ExecutionIds,
}

async fn start_pipeline(
    State(state): State<ControlPlaneAppState>,
    Json(req): Json<StartPipelineRequest>,
) -> Result<Json<StartPipelineResponse>, StatusCode> {
    let attempt_id = AttemptId(1);
    let execution_ids = ExecutionIds::new(req.pipeline_spec_id, PipelineId(Uuid::new_v4()), attempt_id);
    let pipeline_id = execution_ids.pipeline_id;

    let stored = state
        .store
        .get_spec(req.pipeline_spec_id)
        .await
        .ok_or(StatusCode::NOT_FOUND)?;
    let spec: UserPipelineSpec =
        serde_json::from_slice(&stored.spec_bytes).map_err(|_| StatusCode::BAD_REQUEST)?;

    let logical = compile_logical_graph(&spec);
    let execution_graph = logical.to_execution_graph();
    state
        .controller
        .register_execution_graph(pipeline_id, execution_graph)
        .await;
    state.controller.register_pipeline_spec(pipeline_id, spec).await;

    state.store.put_execution_ids(pipeline_id, execution_ids.clone()).await;

    state
        .store
        .put_run(PipelineRun {
            execution_ids: execution_ids.clone(),
            created_at: Utc::now(),
            desired_state: req.desired_state.unwrap_or(PipelineDesiredState::Running),
        })
        .await;

    state
        .store
        .set_desired_state(
            pipeline_id,
            req.desired_state.unwrap_or(PipelineDesiredState::Running),
        )
        .await;

    let status = PipelineStatus {
        execution_ids: execution_ids.clone(),
        state: PipelineLifecycleState::Creating,
        updated_at: Utc::now(),
        worker_count: 0,
        task_count: 0,
        last_checkpoint_id: None,
    };
    state.store.put_status(pipeline_id, status).await;

    let event = PipelineEvent {
        execution_ids: execution_ids.clone(),
        at: Utc::now(),
        kind: PipelineEventKind::StateChanged {
            state: PipelineLifecycleState::Creating,
        },
    };
    state.store.append_event(pipeline_id, event).await;

    Ok(Json(StartPipelineResponse { execution_ids }))
}

async fn get_status(
    State(state): State<ControlPlaneAppState>,
    Path(pipeline_id): Path<String>,
) -> Result<Json<PipelineStatus>, StatusCode> {
    let pipeline_id =
        PipelineId(Uuid::parse_str(&pipeline_id).map_err(|_| StatusCode::BAD_REQUEST)?);
    state
        .store
        .get_status(pipeline_id)
        .await
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

async fn get_latest_snapshot(
    State(state): State<ControlPlaneAppState>,
    Path(pipeline_id): Path<String>,
) -> Result<Json<PipelineSnapshotEntry>, StatusCode> {
    let pipeline_id =
        PipelineId(Uuid::parse_str(&pipeline_id).map_err(|_| StatusCode::BAD_REQUEST)?);
    state
        .store
        .latest_snapshot(pipeline_id)
        .await
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

#[derive(Debug, Deserialize)]
pub struct SnapshotHistoryQuery {
    pub limit: Option<usize>,
}

async fn get_snapshot_history(
    State(state): State<ControlPlaneAppState>,
    Path(pipeline_id): Path<String>,
    axum::extract::Query(q): axum::extract::Query<SnapshotHistoryQuery>,
) -> Result<Json<Vec<PipelineSnapshotEntry>>, StatusCode> {
    let pipeline_id =
        PipelineId(Uuid::parse_str(&pipeline_id).map_err(|_| StatusCode::BAD_REQUEST)?);
    let mut snaps = state.store.list_snapshots(pipeline_id).await;
    if let Some(limit) = q.limit {
        if snaps.len() > limit {
            snaps = snaps.split_off(snaps.len() - limit);
        }
    }
    Ok(Json(snaps))
}

async fn get_snapshot_stats(
    State(state): State<ControlPlaneAppState>,
    Path(pipeline_id): Path<String>,
) -> Result<Json<PipelineDerivedStats>, StatusCode> {
    let pipeline_id =
        PipelineId(Uuid::parse_str(&pipeline_id).map_err(|_| StatusCode::BAD_REQUEST)?);
    let snaps = state.store.list_snapshots(pipeline_id).await;
    if snaps.is_empty() {
        return Ok(Json(PipelineDerivedStats::default()));
    }
    Ok(Json(PipelineDerivedStats {
        snapshot_count: snaps.len(),
        first_ts_ms: snaps.first().map(|e| e.ts_ms),
        last_ts_ms: snaps.last().map(|e| e.ts_ms),
    }))
}

