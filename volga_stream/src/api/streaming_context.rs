use std::collections::HashMap;
use std::sync::Arc;
use std::fmt;
use datafusion::prelude::SessionContext;
use arrow::datatypes::Schema;
use crate::runtime::operators::source::source_operator::SourceConfig;
use crate::runtime::operators::sink::sink_operator::SinkConfig;
use crate::cluster::node_assignment::{node_to_vertex_ids, ExecutionVertexNodeMapping, NodeAssignStrategy, OperatorPerNodeStrategy};
use crate::cluster::cluster_provider::ClusterNode;
use crate::runtime::execution_graph::ExecutionGraph;
use crate::api::planner::{Planner, PlanningContext};
use crate::api::logical_graph::LogicalGraph;

/// Context for streaming query execution containing sources, sinks, and execution parameters
#[derive(Clone)]
pub struct StreamingContext {
    /// DataFusion session context
    df_session_context: SessionContext,
    /// Source table configurations (table_name -> (source_config, schema))
    sources: HashMap<String, (SourceConfig, Arc<Schema>)>,
    /// Optional sink configuration
    sink_config: Option<SinkConfig>,
    /// Parallelism level for each operator
    parallelism: usize,
    /// Current SQL query (set by sql() method)
    current_sql: Option<String>,
    /// Logical graph (set by with_logical_graph() method)
    logical_graph: Option<LogicalGraph>,
}

impl fmt::Debug for StreamingContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamingContext")
            .field("sources", &self.sources)
            .field("sink_config", &self.sink_config)
            .field("parallelism", &self.parallelism)
            .field("current_sql", &self.current_sql)
            .field("df_session_context", &"<SessionContext>")
            .field("logical_graph", &self.logical_graph)
            .finish()
    }
}

impl StreamingContext {
    pub fn new() -> Self {
        Self {
            df_session_context: SessionContext::new(),
            sources: HashMap::new(),
            sink_config: None,
            parallelism: 1,
            current_sql: None,
            logical_graph: None,
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

    /// Set SQL query for execution (returns self for chaining)
    pub fn sql(mut self, sql: &str) -> Self {
        self.current_sql = Some(sql.to_string());
        self
    }

    /// Set logical graph directly (returns self for chaining)
    pub fn with_logical_graph(mut self, logical_graph: LogicalGraph) -> Self {
        // Clear SQL since we're setting the graph directly
        self.current_sql = None;
        self.logical_graph = Some(logical_graph);
        self
    }

    /// Build logical graph from the current SQL query or return existing graph
    pub async fn build_logical_graph(&self) -> LogicalGraph {
        if let Some(ref graph) = self.logical_graph {
            return graph.clone();
        }

        let sql = self.current_sql.as_ref().expect("No SQL query or logical graph set. Call sql() or with_logical_graph() first.");
        
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

    /// Build execution graph with optional cluster nodes and node assignment strategy
    pub async fn build_execution_graph(
        &self,
        cluster_nodes: Option<&[ClusterNode]>,
        node_assignment_strategy: Option<&dyn NodeAssignStrategy>,
    ) -> (ExecutionGraph, Option<ExecutionVertexNodeMapping>) {
        // Build logical graph first
        let logical_graph = self.build_logical_graph().await;

        // Convert to execution graph
        let mut execution_graph = logical_graph.to_execution_graph();

        // Handle clustering and channel configuration
        let node_mapping = if let Some(nodes) = cluster_nodes {
            if nodes.len() > 1 {
                // Remote execution - use provided strategy or default
                let strategy = node_assignment_strategy.unwrap_or(&OperatorPerNodeStrategy);
                let mapping = strategy.assign_nodes(&execution_graph, nodes);
                let node_to_vertex_ids = node_to_vertex_ids(&mapping);
                
                // println!("mapping: {:?}", mapping);
                // println!("node_to_vertex_ids: {:?}", node_to_vertex_ids);
                // Configure channels with node mapping
                execution_graph.update_channels_with_node_mapping(Some(&mapping));

                Some(mapping)
            } else {
                // Single node - local execution
                execution_graph.update_channels_with_node_mapping(None);
                None
            }
        } else {
            // No cluster nodes provided - local execution
            execution_graph.update_channels_with_node_mapping(None);
            None
        };

        (execution_graph, node_mapping)
    }
}

impl Default for StreamingContext {
    fn default() -> Self {
        Self::new()
    }
}