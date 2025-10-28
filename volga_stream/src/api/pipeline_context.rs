use std::collections::HashMap;
use std::sync::Arc;
use std::fmt;
use datafusion::prelude::SessionContext;
use arrow::datatypes::Schema;
use crate::cluster::cluster_provider::ClusterNode;
use crate::cluster::node_assignment::{ExecutionVertexNodeMapping, NodeAssignStrategy, OperatorPerNodeStrategy};
use crate::runtime::execution_graph::ExecutionGraph;
use crate::runtime::operators::source::source_operator::SourceConfig;
use crate::runtime::operators::sink::sink_operator::SinkConfig;

use crate::api::planner::{Planner, PlanningContext};
use crate::api::logical_graph::LogicalGraph;
use crate::executor::executor::{ExecutionState, Executor};
use crate::runtime::worker::WorkerState;
use tokio::sync::mpsc;
use anyhow::Result;

/// Context for pipeline execution containing sources, sinks, and execution parameters
#[derive(Clone)]
pub struct PipelineContext {
    /// DataFusion session context
    df_session_context: SessionContext,
    /// Source table configurations (table_name -> (source_config, schema))
    sources: HashMap<String, (SourceConfig, Arc<Schema>)>,
    /// Optional sink configuration
    sink_config: Option<SinkConfig>,
    /// Parallelism level for each operator
    parallelism: usize,
    /// Current SQL query (set by sql() method)
    sql: Option<String>,
    /// Logical graph (set by with_logical_graph() method)
    logical_graph: Option<LogicalGraph>,
    /// Executor for running the job
    executor: Arc<Option<Box<dyn Executor>>>,
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
            .field("executor", &"<Executor>")
            .finish()
    }
}

impl PipelineContext {
    pub fn new() -> Self {
        Self {
            df_session_context: SessionContext::new(),
            sources: HashMap::new(),
            sink_config: None,
            parallelism: 1,
            sql: None,
            logical_graph: None,
            executor: Arc::new(None),
        }
    }

    pub fn with_source(mut self, table_name: String, source_config: SourceConfig, schema: Arc<Schema>) -> Self {
        self.sources.insert(table_name, (source_config, schema));
        self
    }

    pub fn with_sink(mut self, sink_config: SinkConfig) -> Self {
        self.sink_config = Some(sink_config);
        self
    }

    pub fn with_parallelism(mut self, parallelism: usize) -> Self {
        self.parallelism = parallelism;
        self
    }

    /// Set SQL query for execution
    pub fn sql(mut self, sql: &str) -> Self {
        self.sql = Some(sql.to_string());
        self
    }

    /// Set logical graph directly
    pub fn with_logical_graph(mut self, logical_graph: LogicalGraph) -> Self {
        // Clear SQL since we're setting the graph directly
        self.sql = None;
        self.logical_graph = Some(logical_graph);
        self
    }

    /// Set executor for running the job
    pub fn with_executor(mut self, executor: Box<dyn Executor>) -> Self {
        self.executor = Arc::new(Some(executor));
        self
    }

    /// Build logical graph from the current SQL query or return existing graph
    pub async fn build_logical_graph(&self) -> LogicalGraph {
        if let Some(ref graph) = self.logical_graph {
            return graph.clone();
        }

        let sql = self.sql.as_ref().expect("No SQL query or logical graph set. Call sql() or with_logical_graph() first.");
        
        let mut planner = Planner::new(PlanningContext::new(self.df_session_context.clone()).with_parallelism(self.parallelism));

        // Register source tables
        for (table_name, (source_config, schema)) in &self.sources {
            planner.register_source(table_name.clone(), source_config.clone(), schema.clone());
        }

        // Register sink if provided
        if let Some(sink_config) = &self.sink_config {
            planner.register_sink(sink_config.clone());
        }

        // Convert SQL to logical graph
        planner.sql_to_graph(sql).await.expect("Failed to create logical graph from SQL")
    }

    /// Execute the job with optional state updates broadcasting
    /// Returns the final execution state
    pub async fn execute_with_state_updates(self, state_sender: Option<mpsc::Sender<WorkerState>>) -> Result<ExecutionState> {
        // Build logical graph first
        let logical_graph = self.build_logical_graph().await;

        println!("logical_graph: {:?}", logical_graph.to_dot());

        // Convert to execution graph
        let execution_graph = logical_graph.to_execution_graph();

        // Get executor or panic if not set
        let executor_option = Arc::try_unwrap(self.executor)
            .map_err(|_| anyhow::anyhow!("PipelineContext is still being referenced elsewhere"))?;
        let mut executor = executor_option.expect("No executor set. Call with_executor() first.");

        // Execute using the configured executor
        executor.execute(execution_graph, state_sender).await
    }

    /// Execute the job and return only the final execution state
    pub async fn execute(self) -> Result<ExecutionState> {
        self.execute_with_state_updates(None).await
    }


}

impl Default for PipelineContext {
    fn default() -> Self {
        Self::new()
    }
}