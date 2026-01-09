use std::collections::HashMap;
use std::sync::Arc;
use std::fmt;
use datafusion::prelude::SessionContext;
use arrow::datatypes::Schema;
use crate::runtime::master::PipelineState;
use crate::runtime::operators::source::source_operator::SourceConfig;
use crate::runtime::operators::sink::sink_operator::SinkConfig;

use crate::api::planner::{Planner, PlanningContext};
use crate::api::logical_graph::LogicalGraph;
use crate::control_plane::types::{AttemptId, ExecutionIds};
use crate::executor::local_runtime_adapter::LocalRuntimeAdapter;
use crate::executor::runtime_adapter::{RuntimeAdapter, StartAttemptRequest};
use crate::runtime::worker::{Worker, WorkerConfig, WorkerState};
use crate::transport::transport_backend_actor::TransportBackendType;
use tokio::sync::mpsc;
use anyhow::Result;
use std::collections::HashMap as StdHashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionMode {
    Request,
    Streaming,
    Batch,
}

#[derive(Clone)]
pub enum ExecutionProfile {
    /// Fast single-process harness: one Worker, no Master orchestration loop.
    /// Intended for unit/integration tests that don't require master-driven coordination.
    SingleWorkerNoMaster { num_threads_per_task: usize },
    /// Full orchestration on one machine using loopback gRPC: Master polls WorkerServer(s).
    LocalOrchestrated { num_workers_per_operator: usize },
    /// Full orchestration using a provided RuntimeAdapter (for real distributed environments).
    Orchestrated {
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        num_workers_per_operator: usize,
    },
}

impl Default for ExecutionProfile {
    fn default() -> Self {
        Self::SingleWorkerNoMaster { num_threads_per_task: 4 }
    }
}

/// Context for pipeline execution containing sources, sinks, and execution parameters
#[derive(Clone)]
pub struct PipelineContext {
    /// DataFusion session context
    df_session_context: SessionContext,
    /// Source table configurations (table_name -> (source_config, schema))
    sources: HashMap<String, (SourceConfig, Arc<Schema>)>,
    /// Request source/sink configuration
    request_source_config: Option<SourceConfig>,
    request_sink_config: Option<SinkConfig>,
    /// Optional sink configuration
    sink_config: Option<SinkConfig>,
    /// Parallelism level for each operator
    parallelism: usize,
    /// Current SQL query (set by sql() method)
    sql: Option<String>,
    /// Logical graph (set by with_logical_graph() method)
    logical_graph: Option<LogicalGraph>,
    /// Execution mode
    execution_mode: ExecutionMode,
    /// Execution profile (how to run the pipeline)
    execution_profile: ExecutionProfile,
}

/// Builder for constructing a PipelineContext
#[derive(Clone)]
pub struct PipelineContextBuilder {
    context: PipelineContext,
}

impl fmt::Debug for PipelineContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PipelineContext")
            .field("sources", &self.sources)
            .field("sink_config", &self.sink_config)
            .field("parallelism", &self.parallelism)
            .field("sql", &self.sql)
            .field("df_session_context", &"<SessionContext>")
            .field("logical_graph", &self.logical_graph)
            .field("execution_mode", &self.execution_mode)
            .field("execution_profile", &"<ExecutionProfile>")
            .finish()
    }
}

impl fmt::Debug for PipelineContextBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PipelineContextBuilder")
            .field("context", &self.context)
            .finish()
    }
}

impl PipelineContextBuilder {
    pub fn new() -> Self {
        Self {
            context: PipelineContext {
                df_session_context: SessionContext::new(),
                sources: HashMap::new(),
                request_source_config: None,
                request_sink_config: None,
                sink_config: None,
                parallelism: 1,
                sql: None,
                logical_graph: None,
                execution_mode: ExecutionMode::Streaming,
                execution_profile: ExecutionProfile::default(),
            },
        }
    }

    pub fn with_source(mut self, table_name: String, source_config: SourceConfig, schema: Arc<Schema>) -> Self {
        self.context.sources.insert(table_name, (source_config, schema));
        self
    }

    pub fn with_request_source_sink(mut self, source_config: SourceConfig, sink_config: Option<SinkConfig>) -> Self {
        self.context.request_source_config = Some(source_config);
        self.context.request_sink_config = sink_config;
        self
    }

    pub fn with_sink(mut self, sink_config: SinkConfig) -> Self {
        self.context.sink_config = Some(sink_config);
        self
    }

    pub fn with_parallelism(mut self, parallelism: usize) -> Self {
        self.context.parallelism = parallelism;
        self
    }

    /// Set SQL query for execution
    pub fn sql(mut self, sql: &str) -> Self {
        self.context.sql = Some(sql.to_string());
        self
    }

    /// Set logical graph directly
    pub fn with_logical_graph(mut self, logical_graph: LogicalGraph) -> Self {
        // Clear SQL since we're setting the graph directly
        self.context.sql = None;
        self.context.logical_graph = Some(logical_graph);
        self
    }

    pub fn with_execution_profile(mut self, profile: ExecutionProfile) -> Self {
        self.context.execution_profile = profile;
        self
    }

    pub fn with_execution_mode(mut self, execution_mode: ExecutionMode) -> Self {
        self.context.execution_mode = execution_mode;
        self
    }

    /// Build the final PipelineContext
    pub fn build(mut self) -> PipelineContext {
        if self.context.sql.is_some() {
            let logical_graph = self.context.build_logical_graph();
            self.context.logical_graph = Some(logical_graph);
        }
        self.context
    }
}

impl PipelineContext {
    pub fn get_logical_graph(&self) -> Option<&LogicalGraph> {
        self.logical_graph.as_ref()
    }

    pub fn get_logical_graph_mut(&mut self) -> Option<&mut LogicalGraph> {
        self.logical_graph.as_mut()
    }

    /// Build logical graph from the current SQL query or return existing graph
    fn build_logical_graph(&self) -> LogicalGraph {
        if let Some(ref graph) = self.logical_graph {
            return graph.clone();
        }

        let sql = self.sql.as_ref().expect("No SQL query or logical graph set. Call sql() or with_logical_graph() first.");
        
        let mut planner = Planner::new(PlanningContext::new(self.df_session_context.clone()).with_parallelism(self.parallelism).with_execution_mode(self.execution_mode));

        // Register source tables
        for (table_name, (source_config, schema)) in &self.sources {
            planner.register_source(table_name.clone(), source_config.clone(), schema.clone());
        }

        if let Some(request_source_config) = &self.request_source_config {
            planner.register_request_source_sink(request_source_config.clone(), self.request_sink_config.clone());
        }

        // Register sink if provided
        if let Some(sink_config) = &self.sink_config {
            planner.register_sink(sink_config.clone());
        }

        // Convert SQL to logical graph
        planner.sql_to_graph(sql).expect("Failed to create logical graph from SQL")
    }

    /// Execute the job with optional state updates broadcasting
    /// Returns the final execution state
    pub async fn execute_with_state_updates(self, state_updates_sender: Option<mpsc::Sender<PipelineState>>) -> Result<PipelineState> {
        let logical_graph = self.logical_graph.expect("Logical graph not set. Call sql() or with_logical_graph() first.");
        
        println!("logical_graph: {:?}", logical_graph.to_dot());

        // Convert to execution graph
        let mut execution_graph = logical_graph.to_execution_graph();
        let execution_ids = ExecutionIds::fresh(AttemptId(1));

        match self.execution_profile {
            ExecutionProfile::SingleWorkerNoMaster { num_threads_per_task } => {
                execution_graph.update_channels_with_node_mapping(None);
                let vertex_ids = execution_graph.get_vertices().keys().cloned().collect();
                let worker_id = "single_worker".to_string();

                let worker_config = WorkerConfig::new(
                    worker_id.clone(),
                    execution_ids,
                    execution_graph,
                    vertex_ids,
                    num_threads_per_task,
                    TransportBackendType::InMemory,
                );
                let mut worker = Worker::new(worker_config);

                if let Some(pipeline_state_sender) = state_updates_sender {
                    let (worker_state_sender, mut worker_state_receiver) = mpsc::channel::<WorkerState>(100);
                    let pipeline_sender = pipeline_state_sender.clone();
                    let worker_id_clone = worker_id.clone();
                    tokio::spawn(async move {
                        while let Some(worker_state) = worker_state_receiver.recv().await {
                            let mut worker_states = StdHashMap::new();
                            worker_states.insert(worker_id_clone.clone(), worker_state);
                            let pipeline_state = PipelineState::new(worker_states);
                            let _ = pipeline_sender.send(pipeline_state).await;
                        }
                    });

                    worker
                        .execute_worker_lifecycle_for_testing_with_state_updates(worker_state_sender)
                        .await;
                } else {
                    worker.execute_worker_lifecycle_for_testing().await;
                }

                let worker_state = worker.get_state().await;
                worker.close().await;

                let mut worker_states = StdHashMap::new();
                worker_states.insert(worker_id, worker_state);
                Ok(PipelineState::new(worker_states))
            }
            ExecutionProfile::LocalOrchestrated { num_workers_per_operator } => {
                let adapter = Arc::new(LocalRuntimeAdapter::new());
                let handle = adapter
                    .start_attempt(StartAttemptRequest {
                        execution_ids,
                        execution_graph,
                        num_workers_per_operator,
                    })
                    .await?;
                let final_state = handle.wait().await?;
                if let Some(sender) = state_updates_sender {
                    let _ = sender.send(final_state.clone()).await;
                }
                Ok(final_state)
            }
            ExecutionProfile::Orchestrated { runtime_adapter, num_workers_per_operator } => {
                let handle = runtime_adapter
                    .start_attempt(StartAttemptRequest {
                        execution_ids,
                        execution_graph,
                        num_workers_per_operator,
                    })
                    .await?;
                let final_state = handle.wait().await?;
                if let Some(sender) = state_updates_sender {
                    let _ = sender.send(final_state.clone()).await;
                }
                Ok(final_state)
            }
        }
    }

    /// Execute the job and return only the final execution state
    pub async fn execute(self) -> Result<PipelineState> {
        self.execute_with_state_updates(None).await
    }
}

impl Default for PipelineContextBuilder {
    fn default() -> Self {
        Self::new()
    }
}