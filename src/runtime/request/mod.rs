//! In-process request chain: one HTTP request runs the whole operator graph in one task.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use axum::{http::StatusCode, response::Json, routing::post, Router};
use tokio::sync::{mpsc, oneshot, Semaphore};

use serde::{Deserialize, Serialize};

use crate::api::{LogicalGraph, RequestChain};
use crate::common::message::Message;
use crate::common::types::PipelineId;
use crate::runtime::functions::source::json_utils::{json_to_record_batch, record_batch_to_json};
use crate::runtime::operators::operator::{
    create_operator, Operator, OperatorConfig, StreamOperator, VecOutput,
};
use crate::runtime::operators::window::request::WindowRequestOperator;
use crate::runtime::operators::window::store::{StateNamespace, WindowRequestStore};
use crate::runtime::runtime_context::RuntimeContext;

use petgraph::Direction;

/// JSON body posted to `/request`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestPayload {
    pub data: serde_json::Value,
}

/// JSON body returned by `/request`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponsePayload {
    pub data: serde_json::Value,
}

struct WorkItem {
    batch: RecordBatch,
    reply: oneshot::Sender<Result<RecordBatch, String>>,
}

pub struct RequestExecutor {
    bind_address: String,
    work_tx: mpsc::Sender<WorkItem>,
    worker: Option<tokio::task::JoinHandle<()>>,
    server: Option<tokio::task::JoinHandle<()>>,
}

pub struct RequestExecutorOptions {
    pub pipeline_id: PipelineId,
    pub wro_store: Option<(Arc<dyn WindowRequestStore>, StateNamespace)>,
    pub bind_address: String,
}

impl RequestExecutor {
    pub async fn start(chain: RequestChain, options: RequestExecutorOptions) -> Result<Self> {
        let schema = chain.schema.clone();
        let max_pending = chain.max_pending_requests.max(1);
        let timeout_ms = chain.request_timeout_ms;
        let operators = build_chain(&chain.graph, &options)?;
        let (work_tx, work_rx) = mpsc::channel(max_pending);
        let pipeline_id = options.pipeline_id.clone();
        let graph_for_ctx = chain.graph.clone();
        let worker = tokio::spawn(run_chain(graph_for_ctx, pipeline_id, operators, work_rx));

        let mut exec = Self {
            bind_address: options.bind_address.clone(),
            work_tx,
            worker: Some(worker),
            server: None,
        };
        exec.bind_http(schema, timeout_ms, max_pending).await?;
        Ok(exec)
    }

    pub fn bind_address(&self) -> &str {
        &self.bind_address
    }

    pub async fn stop(&mut self) {
        if let Some(server) = self.server.take() {
            server.abort();
        }
        if let Some(worker) = self.worker.take() {
            worker.abort();
        }
    }

    async fn bind_http(
        &mut self,
        schema: SchemaRef,
        timeout_ms: u64,
        max_pending: usize,
    ) -> Result<()> {
        let bind_address = self.bind_address.clone();
        let work_tx = self.work_tx.clone();
        let semaphore = Arc::new(Semaphore::new(max_pending));

        let app = Router::new().route(
            "/request",
            post(move |payload| {
                handle_request(
                    work_tx.clone(),
                    schema.clone(),
                    semaphore.clone(),
                    timeout_ms,
                    payload,
                )
            }),
        );

        let listener = tokio::net::TcpListener::bind(&bind_address).await?;
        self.server = Some(tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("request HTTP server failed");
        }));
        Ok(())
    }
}

async fn handle_request(
    work_tx: mpsc::Sender<WorkItem>,
    schema: SchemaRef,
    semaphore: Arc<Semaphore>,
    timeout_ms: u64,
    Json(payload): Json<RequestPayload>,
) -> Result<Json<ResponsePayload>, StatusCode> {
    let Ok(_permit) = semaphore.try_acquire() else {
        return Err(StatusCode::TOO_MANY_REQUESTS);
    };

    let payload_array = match &payload.data {
        serde_json::Value::Object(obj) => match obj.get("payload") {
            Some(serde_json::Value::Array(arr)) => serde_json::Value::Array(arr.clone()),
            _ => return Err(StatusCode::BAD_REQUEST),
        },
        _ => return Err(StatusCode::BAD_REQUEST),
    };
    let batch = json_to_record_batch(&payload_array, schema).map_err(|_| StatusCode::BAD_REQUEST)?;

    let (reply, rx) = oneshot::channel();
    if work_tx.send(WorkItem { batch, reply }).await.is_err() {
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }

    match tokio::time::timeout(Duration::from_millis(timeout_ms), rx).await {
        Ok(Ok(Ok(result))) => {
            let data =
                record_batch_to_json(&result).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            Ok(Json(ResponsePayload { data }))
        }
        Ok(Ok(Err(_))) | Ok(Err(_)) => Err(StatusCode::INTERNAL_SERVER_ERROR),
        Err(_) => Err(StatusCode::REQUEST_TIMEOUT),
    }
}

fn build_chain(
    graph: &LogicalGraph,
    options: &RequestExecutorOptions,
) -> Result<Vec<Box<dyn StreamOperator>>> {
    let mut operators: Vec<Box<dyn StreamOperator>> = Vec::new();
    for config in chain_configs(graph) {
        match &config {
            OperatorConfig::WindowRequestConfig(_) => {
                let mut wro = WindowRequestOperator::new(config);
                if let Some((store, namespace)) = &options.wro_store {
                    wro.set_state_with_store_and_ns(store.clone(), namespace.clone());
                }
                operators.push(Box::new(wro));
            }
            _ => match create_operator(config) {
                Operator::Stream(op) => operators.push(op),
                Operator::Source(_) => {
                    return Err(anyhow::anyhow!("request chain contains a source operator"));
                }
            },
        }
    }
    Ok(operators)
}

fn chain_configs(graph: &LogicalGraph) -> Vec<OperatorConfig> {
    let Some(start) = graph.get_all_node_indices().into_iter().find(|&idx| {
        graph
            .get_neighbors(idx, Direction::Incoming)
            .is_empty()
    }) else {
        return Vec::new();
    };

    let mut configs = Vec::new();
    let mut current = Some(start);
    while let Some(idx) = current {
        configs.push(graph.get_node_by_index(idx).operator_config.clone());
        let outgoing = graph.get_neighbors(idx, petgraph::Direction::Outgoing);
        current = outgoing.into_iter().next();
    }
    configs
}

fn operator_context(graph: &LogicalGraph, pipeline_id: &PipelineId, operator_id: &str) -> RuntimeContext {
    let exec = graph.to_execution_graph();
    RuntimeContext::new(
        Arc::<str>::from(operator_id),
        0,
        1,
        None,
        None,
        Some(exec),
    )
    .with_state_config(
        crate::api::OperatorStateBackendConfig::InMemory,
        None,
        pipeline_id.clone(),
        operator_id.to_string(),
    )
}

async fn run_chain(
    graph: LogicalGraph,
    pipeline_id: PipelineId,
    mut operators: Vec<Box<dyn StreamOperator>>,
    mut work_rx: mpsc::Receiver<WorkItem>,
) {
    for op in operators.iter_mut() {
        let id = op.operator_config().to_string();
        let ctx = operator_context(&graph, &pipeline_id, &id);
        if let Err(error) = op.open(&ctx).await {
            panic!("request operator open failed: {error}");
        }
    }

    while let Some(item) = work_rx.recv().await {
        let result = process_one(&mut operators, item.batch).await;
        let _ = item.reply.send(result);
    }

    for op in operators.iter_mut() {
        let _ = op.close().await;
    }
}

async fn process_one(
    operators: &mut [Box<dyn StreamOperator>],
    batch: RecordBatch,
) -> Result<RecordBatch, String> {
    let mut messages = vec![Message::new(None, batch, None, None)];
    for op in operators.iter_mut() {
        let mut out = VecOutput::default();
        op.process_data(messages, &mut out)
            .await
            .map_err(|e| e.to_string())?;
        messages = out.messages;
        if messages.is_empty() {
            break;
        }
    }
    let batches: Vec<RecordBatch> = messages
        .into_iter()
        .map(|m| m.record_batch().clone())
        .collect();
    if batches.is_empty() {
        return Err("request chain produced no batches".to_string());
    }
    let schema = batches[0].schema();
    concat_batches(&schema, &batches).map_err(|e| e.to_string())
}
