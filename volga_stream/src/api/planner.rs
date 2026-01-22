use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::datasource::TableProvider;
use datafusion::physical_plan::{ExecutionPlan};
use datafusion::physical_plan::windows::BoundedWindowAggExec;
use datafusion::physical_plan::memory::{LazyMemoryExec, LazyBatchGenerator};
use arrow::record_batch::RecordBatch;
use parking_lot::RwLock;
use std::sync::Arc as StdArc;
use std::fmt;
use datafusion::logical_expr::{TableType, Window};
use datafusion::catalog::Session;
use async_trait::async_trait;
use std::any::Any;
use datafusion::logical_expr::{
    LogicalPlan, Projection, Filter, Aggregate, Join, TableScan, Expr
};
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::common::{Result, DataFusionError};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::tree_node::TreeNodeVisitor;
use datafusion::prelude::SessionContext;
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datafusion::sql::TableReference;
use datafusion::optimizer::analyzer::type_coercion::TypeCoercion;
use datafusion::optimizer::analyzer::AnalyzerRule;
use petgraph::graph::NodeIndex;

use super::logical_graph::{LogicalNode, LogicalGraph};
use crate::api::ExecutionMode;
use crate::runtime::functions::key_by::key_by_function::{DataFusionKeyFunction};
use crate::runtime::functions::key_by::KeyByFunction;
use crate::runtime::operators::operator::OperatorConfig;
use crate::runtime::functions::map::{FilterFunction, MapFunction, ProjectionFunction};
use crate::runtime::functions::join::join_function::JoinFunction;
use crate::runtime::operators::source::source_operator::SourceConfig;
use crate::runtime::operators::sink::sink_operator::SinkConfig;
use crate::runtime::operators::aggregate::aggregate_operator::AggregateConfig;
use crate::runtime::operators::window::window_operator::WindowOperatorConfig;

// pub static REQUEST_SOURCE_NAME: &str = "request_source";

/// Custom table provider creating dummy tables with no execution logic
#[derive(Debug, Clone)]
pub struct VolgaTableProvider {
    _table_name: String,
    schema: SchemaRef,
}

impl VolgaTableProvider {
    pub fn new(table_name: String, schema: SchemaRef) -> Self {
        Self { _table_name: table_name, schema }
    }
}

#[async_trait]
impl TableProvider for VolgaTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Create a LazyMemoryExec with 1 empty partition instead of 0 partitions
        // This prevents the "Plan does not satisfy distribution requirements" error
        
        // Simple generator that produces no batches
        #[derive(Debug)]
        struct EmptyBatchGenerator {
            _schema: SchemaRef,
        }
        
        impl fmt::Display for EmptyBatchGenerator {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "EmptyBatchGenerator")
            }
        }
        
        impl LazyBatchGenerator for EmptyBatchGenerator {
            fn generate_next_batch(&mut self) -> Result<Option<RecordBatch>> {
                Ok(None) // Always return None to indicate no more batches
            }
        }
        
        let generator = StdArc::new(RwLock::new(EmptyBatchGenerator {
            _schema: self.schema.clone(),
        }));
        
        Ok(Arc::new(LazyMemoryExec::try_new(self.schema.clone(), vec![generator])?))
    }
}


/// Converts DataFusion logical plans to Volga logical graphs
pub struct Planner {
    logical_graph: LogicalGraph,
    node_stack: Vec<NodeIndex>,
    context: PlanningContext,
}

#[derive(Clone)]
pub struct PlanningContext {
    pub df_session_context: SessionContext,
    pub connector_configs: HashMap<String, SourceConfig>,
    pub sink_config: Option<SinkConfig>,

    pub request_source_config: Option<SourceConfig>,
    pub request_sink_config: Option<SinkConfig>,

    pub df_planner: Arc<DefaultPhysicalPlanner>,
    pub execution_mode: ExecutionMode,

    // TODO figure out how to set parallelism per node
    pub parallelism: usize,

}

impl PlanningContext {
    pub fn new(df_session_context: SessionContext) -> Self {
        crate::runtime::operators::window::cate::register_cate_udafs(&df_session_context);
        Self {
            df_session_context,
            connector_configs: HashMap::new(),
            sink_config: None,
            request_source_config: None,
            request_sink_config: None,
            df_planner: Arc::new(DefaultPhysicalPlanner::default()),
            execution_mode: ExecutionMode::Streaming,
            parallelism: 1, // Default parallelism
        }
    }

    pub fn with_parallelism(mut self, parallelism: usize) -> Self {
        self.parallelism = parallelism;
        self
    }

    pub fn with_execution_mode(mut self, execution_mode: ExecutionMode) -> Self {
        self.execution_mode = execution_mode;
        self
    }

    
}

impl Planner {
    pub fn new(context: PlanningContext) -> Self {
        Self {
            logical_graph: LogicalGraph::new(),
            node_stack: Vec::new(),
            context,
        }
    }

    pub fn register_request_source_sink(&mut self, source_config: SourceConfig, sink_config: Option<SinkConfig>) {
        self.context.request_source_config = Some(source_config);
        self.context.request_sink_config = sink_config;
    }

    pub fn register_source(&mut self, table_name: String, config: SourceConfig, schema: SchemaRef) {
        self.context.connector_configs.insert(table_name.clone(), config);

        let table = VolgaTableProvider::new(table_name.clone(), schema);
        self.context.df_session_context.register_table(
            TableReference::Bare {
                table: table_name.as_str().into(),
            },
            Arc::new(table),
        ).unwrap();
    }

    pub fn register_sink(&mut self, config: SinkConfig) {
        self.context.sink_config = Some(config);
    }

    pub fn logical_plan_to_graph(&mut self, logical_plan: &LogicalPlan) -> Result<LogicalGraph> {
        self.node_stack.clear();

        let optimized_plan = self.optimize_plan(logical_plan.clone())?;
        optimized_plan.visit_with_subqueries(self)?;
        
        Ok(self.logical_graph.clone())
    }

    pub fn sql_to_graph(&mut self, sql: &str) -> Result<LogicalGraph> {
        let logical_plan = std::thread::scope(|s| {
            let handle = s.spawn(|| {
                let rt = tokio::runtime::Runtime::new()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                rt.block_on(self.context.df_session_context.state().create_logical_plan(sql))
            });
            handle.join().unwrap()
        })?;
        
        // println!("{}", logical_plan.display_indent());

        let mut graph = self.logical_plan_to_graph(&logical_plan)?;
        if self.context.execution_mode == ExecutionMode::Request {
            // TODO is it an ok logic?
            if self.context.request_source_config.is_some() {
                graph.to_request_mode(
                    self.context.request_source_config.clone().expect("Request source configuration not found"), 
                    self.context.request_sink_config.clone()
                ).map_err(|e| DataFusionError::Plan(e))?;
            } else {
                // warn that request mode is without request source - most likely for window operator debug
                println!("Warning: Request mode is without request source - most likely for window operator debug");
            }
        }
        Ok(graph)
    }

    fn create_source_node(&mut self, table_scan: &TableScan, parallelism: usize) -> Result<()> {
        let table_name = table_scan.table_name.table();
        let mut source_config = self.context.connector_configs.get(table_name)
            .ok_or_else(|| DataFusionError::Plan(format!("No source configuration found for table '{}'", table_name)))?
            .clone();

        if table_scan.projection.is_some() {
            let projection = table_scan.projection.as_ref().unwrap().clone();
            let schema = table_scan.projected_schema.inner().clone();
            // TODO: push projection into source functions to drop unused columns before record batches are materialized.
            source_config.set_projection(projection, schema);
        }
        
        let node = LogicalNode::new(
            OperatorConfig::SourceConfig(source_config),
            parallelism,
            None, // TODO set schemas
            None,
        );

        let node_index = self.logical_graph.add_node(node);
        self.node_stack.push(node_index);
        Ok(())
    }

    fn create_projection_node(&mut self, projection: &Projection, parallelism: usize) -> Result<()> {
        let projection_function = ProjectionFunction::new(
            projection.input.schema().clone(), 
            projection.schema.clone(),
            projection.expr.clone(), 
            self.context.df_session_context.clone()
        );
        let node = LogicalNode::new(
            // OperatorConfig::MapConfig(MapFunction::DataFusionProjection(DataFusionProjectionFunction::new(Arc::new(projection_exec)))),
            OperatorConfig::MapConfig(MapFunction::Projection(projection_function)),
            parallelism,
            None, // TODO set schemas
            None,
        );

        let node_index = self.logical_graph.add_node(node);
        self.node_stack.push(node_index);
        Ok(())
    }

    fn create_filter_node(&mut self, filter: &Filter, parallelism: usize) -> Result<()> {
        let filter_function = FilterFunction::new(
            filter.input.schema().clone(), 
            filter.predicate.clone(), 
            self.context.df_session_context.clone(), 
        );
        
        let node = LogicalNode::new(
            OperatorConfig::MapConfig(MapFunction::Filter(filter_function)),
            parallelism,
            None,
            None,
        );

        let node_index = self.logical_graph.add_node(node);
        self.node_stack.push(node_index);
        Ok(())
    }

    fn create_join_node(&mut self, _join: &Join, parallelism: usize) -> Result<(), DataFusionError> {
        let join_function = JoinFunction::new();
        
        let node = LogicalNode::new(
            OperatorConfig::JoinConfig(join_function),
            parallelism,
            None,
            None,
        );
        
        let node_index = self.logical_graph.add_node(node);
        self.node_stack.push(node_index);
        Ok(())
    }

    fn create_sink_node(&mut self, sink_config: SinkConfig, root_node_index: NodeIndex, parallelism: usize) -> Result<()> {
        // Create sink node
        let sink_node = LogicalNode::new(
            OperatorConfig::SinkConfig(sink_config),
            parallelism,
            None,
            None,
        );
        let sink_node_index = self.logical_graph.add_node(sink_node);
        
        // Data flows from root_node to sink_node
        self.logical_graph.add_edge(root_node_index, sink_node_index);
        Ok(())
    }

    fn handle_aggregate(&mut self, aggregate: &Aggregate, parallelism: usize) -> Result<()> {
       let aggregate_plan = LogicalPlan::Aggregate(aggregate.clone());
        
        // Use scoped thread to avoid runtime conflicts
        let physical_plan = std::thread::scope(|s| {
            let handle = s.spawn(|| {
                let rt = tokio::runtime::Runtime::new()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                rt.block_on(self.context.df_planner.create_physical_plan(&aggregate_plan, &self.context.df_session_context.state()))
            });
            handle.join().unwrap()
        })?;

        fn find_partial_aggregate(exec: &Arc<dyn ExecutionPlan>) -> Option<AggregateExec> {
            if let Some(agg) = exec.as_any().downcast_ref::<AggregateExec>() {
                use datafusion::physical_plan::aggregates::AggregateMode;
                if agg.mode() != &AggregateMode::Final && agg.mode() != &AggregateMode::FinalPartitioned {
                    // found a partial aggregate exec
                    return Some(agg.clone());
                }
            }
            let children = exec.children();
            for child in children {
                if let Some(found) = find_partial_aggregate(child) {
                    return Some(found);
                }
            }
            None
        }
        
        // Datafusion splits physical aggregate into 2 nodes: partial and final
        // We need the partial node to get the group by expression since it uses proper input schema
        // This groupby will be used by key by function
        let partial_aggregate_exec = find_partial_aggregate(&physical_plan).expect("should have found an aggregate exec");
        // create 2 nodes - key by + aggregate node 
        let key_by_node = LogicalNode::new(
            OperatorConfig::KeyByConfig(KeyByFunction::DataFusion(DataFusionKeyFunction::new(Arc::new(partial_aggregate_exec.clone())))),
            parallelism,
            None,
            None,
        );
        
        // Aggregate operator should use final aggregate exec
        // TODO detect aggregate type (e.g. windowed or regular) once we have windowing enabled
        let final_aggregate_exec = physical_plan
            .as_any()
            .downcast_ref::<AggregateExec>()
            .expect("should have found an aggregate exec");

        let aggreagte_node = LogicalNode::new(
            OperatorConfig::AggregateConfig(AggregateConfig {
                aggregate_exec: final_aggregate_exec.clone(),
                group_input_exprs: partial_aggregate_exec.group_expr().input_exprs(),
            }),
            parallelism,
            None,
            None,
        );

        let aggregate_node_index = self.logical_graph.add_node(aggreagte_node);
        self.node_stack.push(aggregate_node_index);

        let key_by_node_index = self.logical_graph.add_node(key_by_node);
        self.node_stack.push(key_by_node_index);
        Ok(())
    }

    fn handle_window(&mut self, window: &Window, parallelism: usize) -> Result<()> {
        // Build physical plan for this window node
        let window_plan = LogicalPlan::Window(window.clone());
        let physical_plan = std::thread::scope(|s| {
            let handle = s.spawn(|| {
                let rt = tokio::runtime::Runtime::new()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                rt.block_on(self.context.df_planner.create_physical_plan(&window_plan, &self.context.df_session_context.state()))
            });
            handle.join().unwrap()
        })?;

        // TODO we should also handle regular WindowAggExec
        let name = physical_plan.name();
        let window_exec = physical_plan
            .as_any()
            .downcast_ref::<BoundedWindowAggExec>()
            .expect(&format!("should have found a window exec, found: {:?}", name));

        // KeyBy by PARTITION BY
        let key_by_node = LogicalNode::new(
            OperatorConfig::KeyByConfig(KeyByFunction::DataFusion(DataFusionKeyFunction::new_window(Arc::new(window_exec.clone())))),
            parallelism,
            None,
            None,
        );

        // Window operator node using the physical exec.
        let window_node = LogicalNode::new(
            OperatorConfig::WindowConfig(WindowOperatorConfig::new(Arc::new(window_exec.clone()))),
            parallelism,
            None,
            None,
        );

        let window_node_index = self.logical_graph.add_node(window_node);
        self.node_stack.push(window_node_index);

        let key_by_node_index = self.logical_graph.add_node(key_by_node);
        self.node_stack.push(key_by_node_index);

        Ok(())
    }
    
    fn optimize_plan(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        let session_state = self.context.df_session_context.state();
        let config = session_state.config_options();
        
        // TODO we should use DataFusion's default Analyzer instead of custom analyzer list
        // Apply type coercion analyzer
        let type_coercion = TypeCoercion::new();
        let coerced_plan = type_coercion.analyze(plan, config)?;
        
        Ok(coerced_plan)
    }
}

impl<'a> TreeNodeVisitor<'a> for Planner {
    type Node = LogicalPlan;

    // Create node and insert into graph traversing top-to-bottom, 
    // store node indexes in temp stack to add edges later, going bottom-to-top
    fn f_down(&mut self, node: &'a Self::Node) -> Result<TreeNodeRecursion> {
        // Process node and create operator
        match node {
            LogicalPlan::TableScan(table_scan) => {
                self.create_source_node(table_scan, self.context.parallelism)?
            }
            
            LogicalPlan::Projection(projection) => {
                self.create_projection_node(projection, self.context.parallelism)?
            }
            
            LogicalPlan::Filter(filter) => {
                self.create_filter_node(filter, self.context.parallelism)?
            }
            
            LogicalPlan::Join(join) => {    
                self.create_join_node(join, self.context.parallelism)?
            }

            LogicalPlan::Aggregate(aggregate) => {
                self.handle_aggregate(aggregate, self.context.parallelism)?;
            }
            LogicalPlan::Window(window) => {
                self.handle_window(window, self.context.parallelism)?;
            }
            
            // skip subqueries as they simply wrap other plans
            LogicalPlan::Subquery(_) | LogicalPlan::SubqueryAlias(_) => {}
            
            _ => {
                return Err(DataFusionError::Plan(format!("Unsupported logical plan: {:?}", node)));
            }
        };
        
        Ok(TreeNodeRecursion::Continue)
    }

    // Add edges from temp stack to graph, going bottom-to-top
    fn f_up(&mut self, node: &'a Self::Node) -> Result<TreeNodeRecursion> {
        // skip subqueries
        if matches!(node, LogicalPlan::Subquery(_) | LogicalPlan::SubqueryAlias(_)) {
            return Ok(TreeNodeRecursion::Continue);
        }
        
        // TODO handle join separately to set shuffle edges

        // handle aggregate separately
        if matches!(node, LogicalPlan::Aggregate(_)) {

            // TODO verify node types at indices
            let key_by_node_index = self.node_stack.pop().unwrap();
            let aggregate_node_index = self.node_stack.pop().unwrap();

            // add edge between key by and aggregate
            self.logical_graph.add_edge(key_by_node_index, aggregate_node_index);

            // add edge between prev node and aggregate
            let prev_node_index = self.node_stack.last().expect("key by should have a node before it");
            self.logical_graph.add_edge(aggregate_node_index, *prev_node_index);

            return Ok(TreeNodeRecursion::Continue);
        }

        // handle window separately
        if matches!(node, LogicalPlan::Window(_)) {
            let key_by_node_index = self.node_stack.pop().unwrap();
            let window_node_index = self.node_stack.pop().unwrap();

            // key_by -> window
            self.logical_graph.add_edge(key_by_node_index, window_node_index);

            // window -> prev
            let prev_node_index = self.node_stack.last().expect("key by should have a node before it");
            self.logical_graph.add_edge(window_node_index, *prev_node_index);

            return Ok(TreeNodeRecursion::Continue);
        }

        let node_index = self.node_stack.pop().unwrap();
        if let Some(prev_node_index) = self.node_stack.last() {
            // Data flows from current node to previous node (since we're going bottom-to-top)
            self.logical_graph.add_edge(node_index, *prev_node_index);
        } else {
            // no prev node - this is the root of the plan
            // Mark this as the root node
            self.logical_graph.set_root_node(node_index);
            
            // if sink is configured add here (not in request mode), for request mode sink is set later
            if self.context.sink_config.is_some() {
                if self.context.execution_mode == ExecutionMode::Request {
                    panic!("Request mode should not set sinks directly")
                }
                
                let sink_config = self.context.sink_config.clone().unwrap();
                self.create_sink_node(sink_config.clone(), node_index, self.context.parallelism)?;
            }
        }
        
        Ok(TreeNodeRecursion::Continue)
    }
}

#[cfg(test)]
mod tests {
    use crate::runtime::{functions::source::RequestSourceConfig, operators::source::source_operator::VectorSourceConfig, partition::PartitionType};

    use super::*;
    use arrow::datatypes::{Schema, Field, DataType};
    use petgraph::Direction;
    use std::sync::Arc;
    use crate::runtime::operators::sink::sink_operator::SinkConfig;
    /// Node type enum for logical graph nodes (used in planner tests)
    #[derive(Debug, Clone, Copy, PartialEq)]
    pub enum NodeType {
        Source,
        Sink,
        Projection,
        Filter,
        KeyBy,
        Aggregate,
        Join,
        Window,
        WindowRequest,
    }

    /// Helper function to get node type from operator config (used in planner tests)
    pub fn get_node_type(operator_config: &OperatorConfig) -> NodeType {
        use crate::runtime::functions::map::MapFunction;
        match operator_config {
            OperatorConfig::SourceConfig(_) => NodeType::Source,
            OperatorConfig::SinkConfig(_) => NodeType::Sink,
            OperatorConfig::MapConfig(MapFunction::Projection(_)) => NodeType::Projection,
            OperatorConfig::MapConfig(MapFunction::Filter(_)) => NodeType::Filter,
            OperatorConfig::KeyByConfig(_) => NodeType::KeyBy,
            OperatorConfig::AggregateConfig(_) => NodeType::Aggregate,
            OperatorConfig::JoinConfig(_) => NodeType::Join,
            OperatorConfig::WindowConfig(_) => NodeType::Window,
            OperatorConfig::WindowRequestConfig(_) => NodeType::WindowRequest,
            _ => panic!("Unsupported operator config: {:?}", operator_config),
        }
    }

    fn create_planner() -> Planner {
        let ctx = SessionContext::new();
        let mut planner = Planner::new(PlanningContext::new(ctx));
        
        // Create test schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));
        // Register source
        planner.register_source("test_table".to_string(), SourceConfig::VectorSourceConfig(VectorSourceConfig::new(vec![])), schema.clone());
        planner.register_source("test_table2".to_string(), SourceConfig::VectorSourceConfig(VectorSourceConfig::new(vec![])), schema);
        
        planner
    }

    /// Find nodes by type using local get_node_type function
    fn find_nodes_by_type(graph: &LogicalGraph, node_type: NodeType) -> Vec<usize> {
        graph.get_nodes()
            .enumerate()
            .filter_map(|(idx, node)| {
                if get_node_type(&node.operator_config) == node_type {
                    Some(idx)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Helper function to verify edge connectivity between specific node types
    fn verify_edge_connectivity(
        graph: &LogicalGraph, 
        expected_edges: &[(NodeType, NodeType, PartitionType, usize)]
    ) {
        let edges: Vec<_> = graph.get_edges().collect();
        
        for (expected_from, expected_to, expected_partition_type, expected_count) in expected_edges {
            let from_nodes = find_nodes_by_type(graph, *expected_from);
            let to_nodes = find_nodes_by_type(graph, *expected_to);
            
            assert!(!from_nodes.is_empty(), "Could not find {:?} node", expected_from);
            assert!(!to_nodes.is_empty(), "Could not find {:?} node", expected_to);
            
            // Count actual edges of this type
            let mut actual_count = 0;
            for &from_idx in &from_nodes {
                for &to_idx in &to_nodes {
                    let edge_count = edges.iter().filter(|(from, to, edge)| {
                        from.index() == from_idx && 
                        to.index() == to_idx && 
                        edge.partition_type == *expected_partition_type
                    }).count();
                    actual_count += edge_count;
                }
            }
            
            assert_eq!(actual_count, *expected_count, 
                "Expected {} edges from {:?} to {:?} with partition type {:?}, found {}", 
                expected_count, expected_from, expected_to, expected_partition_type, actual_count);
        }
    }
    #[tokio::test]
    async fn test_simple_select() {
        let mut planner = create_planner();
        
        let sql = "SELECT id, name FROM test_table";
        let graph = planner.sql_to_graph(sql).unwrap();

        // println!("{}", graph);
        
        // Should have 2 nodes: source and projection
        let nodes: Vec<_> = graph.get_nodes().collect();
        assert_eq!(nodes.len(), 2);

        let edges: Vec<_> = graph.get_edges().collect();
        assert_eq!(edges.len(), 1);
        
        // First node should be projection
        assert!(matches!(nodes[0].operator_config, OperatorConfig::MapConfig(MapFunction::Projection(_))));
        // Second node should be source
        assert!(matches!(nodes[1].operator_config, OperatorConfig::SourceConfig(_)));

        // Verify edge connectivity: source -> projection
        verify_edge_connectivity(&graph, &[
            (NodeType::Source, NodeType::Projection, PartitionType::RoundRobin, 1),
        ]);
    }

    #[tokio::test]
    async fn test_simple_select_with_sink() {
        let mut planner = create_planner();
        
        // Register sink
        planner.register_sink(SinkConfig::InMemoryStorageGrpcSinkConfig("http://127.0.0.1:8080".to_string()));
        
        let sql = "SELECT id, name FROM test_table";
        let graph = planner.sql_to_graph(sql).unwrap();
        
        // Should have 3 nodes: source, projection, and sink
        let nodes: Vec<_> = graph.get_nodes().collect();
        assert_eq!(nodes.len(), 3);

        let edges: Vec<_> = graph.get_edges().collect();
        assert_eq!(edges.len(), 2); // source->projection, projection->sink
        
        // Check node types
        assert!(matches!(nodes[0].operator_config, OperatorConfig::MapConfig(MapFunction::Projection(_))));
        assert!(matches!(nodes[1].operator_config, OperatorConfig::SourceConfig(_)));

        // sink should be last
        assert!(matches!(nodes[2].operator_config, OperatorConfig::SinkConfig(_)));

        // Verify edge connectivity: source -> projection -> sink
        verify_edge_connectivity(&graph, &[
            (NodeType::Source, NodeType::Projection, PartitionType::RoundRobin, 1),
            (NodeType::Projection, NodeType::Sink, PartitionType::RoundRobin, 1),
        ]);
    }

    #[tokio::test]
    async fn test_select_with_filter() {
        let mut planner = create_planner();
        
        let sql = "SELECT id, name FROM test_table WHERE value > 3.0";
        let graph = planner.sql_to_graph(sql).unwrap();

        // println!("{}", graph);
        
        // Should have 3 nodes: source, filter, projection
        let nodes: Vec<_> = graph.get_nodes().collect();
        assert_eq!(nodes.len(), 3);

        let edges: Vec<_> = graph.get_edges().collect();
        assert_eq!(edges.len(), 2);
        
        // Check node types
        assert!(matches!(nodes[0].operator_config, OperatorConfig::MapConfig(MapFunction::Projection(_))));
        assert!(matches!(nodes[1].operator_config, OperatorConfig::MapConfig(MapFunction::Filter(_))));
        assert!(matches!(nodes[2].operator_config, OperatorConfig::SourceConfig(_)));

        // Verify edge connectivity: source -> filter -> projection
        verify_edge_connectivity(&graph, &[
            (NodeType::Source, NodeType::Filter, PartitionType::RoundRobin, 1),
            (NodeType::Filter, NodeType::Projection, PartitionType::RoundRobin, 1),
        ]);
    }


    #[tokio::test]
    async fn test_join_tables() {
        let mut planner = create_planner();
        
        let sql = "SELECT t1.id, t1.name, t2.value FROM test_table t1 JOIN test_table2 t2 ON t1.id = t2.id";
        
        let graph = planner.sql_to_graph(sql).unwrap();
        // println!("{}", graph);
        
        // Should have 4 nodes: 2 sources, join, projection
        let nodes: Vec<_> = graph.get_nodes().collect();
        assert_eq!(nodes.len(), 4);

        let edges: Vec<_> = graph.get_edges().collect();
        assert_eq!(edges.len(), 3);
        
        // Check node types
        assert!(matches!(nodes[0].operator_config, OperatorConfig::MapConfig(MapFunction::Projection(_))));
        assert!(matches!(nodes[1].operator_config, OperatorConfig::JoinConfig(_)));
        assert!(matches!(nodes[2].operator_config, OperatorConfig::SourceConfig(_)));
        assert!(matches!(nodes[3].operator_config, OperatorConfig::SourceConfig(_)));
    }

    #[tokio::test]
    async fn test_group_by() {
        let mut planner = create_planner();
        
        let sql = "SELECT name, COUNT(*) as count FROM test_table GROUP BY name";
        let graph = planner.sql_to_graph(sql).unwrap();

        // Should have 4 nodes: source -> key_by -> aggregate -> projection
        let nodes: Vec<_> = graph.get_nodes().collect();
        assert_eq!(nodes.len(), 4, "Expected 4 nodes (source, key_by, aggregate, projection), found {}", nodes.len());

        // println!("Node operator_ids: {}", nodes.iter()
        //     .map(|n| n.operator_id.as_str())
        //     .collect::<Vec<_>>()
        //     .join(", "));

        // Check node types in reverse order (projection is first in stack)
        assert!(matches!(nodes[0].operator_config, OperatorConfig::MapConfig(MapFunction::Projection(_))));
        assert!(matches!(nodes[1].operator_config, OperatorConfig::AggregateConfig(_)));
        assert!(matches!(nodes[2].operator_config, OperatorConfig::KeyByConfig(KeyByFunction::DataFusion(_))));
        assert!(matches!(nodes[3].operator_config, OperatorConfig::SourceConfig(_)));
        
        // Check edges - should have 3 edges
        let edges: Vec<_> = graph.get_edges().collect();

        // print!("{:?}", edges);
        assert_eq!(edges.len(), 3, "Expected 3 edges, found {}", edges.len());
        
        // Verify edge connectivity: source -> keyby -> aggregate -> projection
        // keyby -> aggregate should be Hash, others RoundRobin
        verify_edge_connectivity(&graph, &[
            (NodeType::Source, NodeType::KeyBy, PartitionType::RoundRobin, 1),
            (NodeType::KeyBy, NodeType::Aggregate, PartitionType::Hash, 1),
            (NodeType::Aggregate, NodeType::Projection, PartitionType::RoundRobin, 1),
        ]);
    }
    
    #[tokio::test]
    async fn test_group_by_multiple_clauses() {
        let mut planner = create_planner();
        
        // Register additional test table with more columns
        let schema = Arc::new(Schema::new(vec![
            Field::new("department", DataType::Utf8, false),
            Field::new("role", DataType::Utf8, false),
            Field::new("salary", DataType::Int32, false),
            Field::new("year", DataType::Int32, false),
            Field::new("month", DataType::Int32, false),
        ]));
        planner.register_source(
            "employees".to_string(), 
            SourceConfig::VectorSourceConfig(VectorSourceConfig::new(vec![])), 
            schema
        );
        
        // Query with multiple GROUP BY clauses:
        // 1. First groups by department, role to get avg salary per dept/role
        // 2. Then groups by department to get total salary per department
        let sql = "WITH dept_stats AS (
                    SELECT department, role, AVG(salary) as avg_salary
                    FROM employees
                    GROUP BY department, role
                  )
                  SELECT department, COUNT(*) as role_count, SUM(avg_salary) as total_salary
                  FROM dept_stats
                  GROUP BY department";
                  
        let graph = planner.sql_to_graph(sql).unwrap();
        
        // Should have structure:
        // source -> key_by1 -> aggregate1 -> projection1 -> key_by2 -> aggregate2 -> projection2
        let nodes: Vec<_> = graph.get_nodes().collect();

        // println!("Node operator_ids: {}", nodes.iter()
        //     .map(|n| n.operator_id.as_str())
        //     .collect::<Vec<_>>()
        //     .join(", "));
        
        assert_eq!(nodes.len(), 7, "Expected 7 nodes (source, key_by1, aggregate1, projection1, key_by2, aggregate2, projection2), found {}", nodes.len());
        
        // Check node types in reverse order (projection2 is first in stack)
        assert!(matches!(nodes[0].operator_config, OperatorConfig::MapConfig(MapFunction::Projection(_))), "Expected final projection");
        assert!(matches!(nodes[1].operator_config, OperatorConfig::AggregateConfig(_)), "Expected second aggregate");
        assert!(matches!(nodes[2].operator_config, OperatorConfig::KeyByConfig(KeyByFunction::DataFusion(_))), "Expected second key_by");
        assert!(matches!(nodes[3].operator_config, OperatorConfig::MapConfig(MapFunction::Projection(_))), "Expected first projection");
        assert!(matches!(nodes[4].operator_config, OperatorConfig::AggregateConfig(_)), "Expected first aggregate");
        assert!(matches!(nodes[5].operator_config, OperatorConfig::KeyByConfig(KeyByFunction::DataFusion(_))), "Expected first key_by");
        assert!(matches!(nodes[6].operator_config, OperatorConfig::SourceConfig(_)), "Expected source");
        
        // Verify first GROUP BY (department, role) has 2 group expressions and 1 aggregate (AVG)
        if let OperatorConfig::AggregateConfig(config) = &nodes[4].operator_config {
            let group_exprs = config.aggregate_exec.group_expr().output_exprs();
            let aggr_exprs = config.aggregate_exec.aggr_expr();
            assert_eq!(group_exprs.len(), 2, "First GROUP BY should have 2 expressions (department, role)");
            assert_eq!(aggr_exprs.len(), 1, "First aggregate should have 1 expression (AVG)");
        }
        
        // Verify second GROUP BY (department) has 1 group expression and 2 aggregates (COUNT, SUM)
        if let OperatorConfig::AggregateConfig(config) = &nodes[1].operator_config {
            let group_exprs = config.aggregate_exec.group_expr().output_exprs();
            let aggr_exprs = config.aggregate_exec.aggr_expr();
            assert_eq!(group_exprs.len(), 1, "Second GROUP BY should have 1 expression (department)");
            assert_eq!(aggr_exprs.len(), 2, "Second aggregate should have 2 expressions (COUNT, SUM)");
        }
        
        // Check edges - should have 6 edges (connecting 7 nodes)
        let edges: Vec<_> = graph.get_edges().collect();
        // println!("edges {:?}", edges);
        assert_eq!(edges.len(), 6, "Expected 6 edges, found {}", edges.len());

        // Verify edge connectivity for complex multi-GROUP BY query
        // Structure: source -> keyby1 -> aggregate1 -> projection1 -> keyby2 -> aggregate2 -> projection2
        // 2 Hash edges (keyby->aggregate), 4 RoundRobin edges (all others)
        verify_edge_connectivity(&graph, &[
            (NodeType::Source, NodeType::KeyBy, PartitionType::RoundRobin, 1),
            (NodeType::KeyBy, NodeType::Aggregate, PartitionType::Hash, 2),
            (NodeType::Aggregate, NodeType::Projection, PartitionType::RoundRobin, 2),
            (NodeType::Projection, NodeType::KeyBy, PartitionType::RoundRobin, 1),
        ]);
    }

    #[tokio::test]
    async fn test_window_query() {
        let mut planner = create_planner();
        
        // Register additional test table with timestamp column for window functions
        let schema = Arc::new(Schema::new(vec![
            Field::new("event_time", DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None), false),
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));
        planner.register_source(
            "events".to_string(), 
            SourceConfig::VectorSourceConfig(VectorSourceConfig::new(vec![])), 
            schema
        );
        
        // Window query with SUM over a time-based window partitioned by key
        let sql = "SELECT 
                    event_time, 
                    key, 
                    value, 
                    SUM(value) OVER (
                        PARTITION BY key 
                        ORDER BY event_time 
                        RANGE BETWEEN INTERVAL '1000' MILLISECOND PRECEDING AND CURRENT ROW
                    ) as sum_value
                   FROM events";
                   
        let graph = planner.sql_to_graph(sql).unwrap();
        
        // Should have structure: source -> key_by -> window -> projection
        // DataFusion adds a projection node after window expressions
        let nodes: Vec<_> = graph.get_nodes().collect();
        assert_eq!(nodes.len(), 4, "Expected 4 nodes (source, key_by, window, projection), found {}", nodes.len());
        
        // Check node types in reverse order (projection is first in stack)
        assert!(matches!(nodes[0].operator_config, OperatorConfig::MapConfig(MapFunction::Projection(_))), "Expected final projection after window");
        assert!(matches!(nodes[1].operator_config, OperatorConfig::WindowConfig(_)), "Expected window operator");
        assert!(matches!(nodes[2].operator_config, OperatorConfig::KeyByConfig(KeyByFunction::DataFusion(_))), "Expected key_by for PARTITION BY");
        assert!(matches!(nodes[3].operator_config, OperatorConfig::SourceConfig(_)), "Expected source");
        
        // Check edges - should have 3 edges connecting 4 nodes
        let edges: Vec<_> = graph.get_edges().collect();
        println!("Edges: {:?}", edges);
        assert_eq!(edges.len(), 3, "Expected 3 edges, found {}", edges.len());
        
        // Verify edge connectivity: source -> keyby -> window -> projection
        // keyby -> window should be Hash (for partitioning), others RoundRobin
        verify_edge_connectivity(&graph, &[
            (NodeType::Source, NodeType::KeyBy, PartitionType::RoundRobin, 1),
            (NodeType::KeyBy, NodeType::Window, PartitionType::Hash, 1),
            (NodeType::Window, NodeType::Projection, PartitionType::RoundRobin, 1),
        ]);
    }

    #[tokio::test]
    async fn test_window_query_to_request_mode() {
        let mut planner = create_planner();

        // set execution mode to request
        planner.context.execution_mode = ExecutionMode::Request;
        
        // Register additional test table with timestamp column for window functions
        let schema = Arc::new(Schema::new(vec![
            Field::new("event_time", DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None), false),
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));
        planner.register_source(
            "events".to_string(), 
            SourceConfig::VectorSourceConfig(VectorSourceConfig::new(vec![])), 
            schema.clone()
        );
        let request_source_config = SourceConfig::HttpRequestSourceConfig(RequestSourceConfig::new(
            crate::runtime::functions::source::RequestSourceSinkSpec {
                bind_address: "127.0.0.1:8080".to_string(),
                max_pending_requests: 100,
                request_timeout_ms: 5000,
                schema_ipc: vec![],
                sink: None,
            }
        ));
        // planner.register_source(
        //     REQUEST_SOURCE_NAME.to_string(), 
        //     request_source_config, 
        //     schema.clone()
        // );
        // planner.register_sink(SinkConfig::RequestSinkConfig);
        planner.register_request_source_sink(request_source_config, Some(SinkConfig::RequestSinkConfig));
        
        // Window query with SUM over a time-based window partitioned by key
        let sql = "SELECT 
                    event_time, 
                    key, 
                    value, 
                    SUM(value) OVER (
                        PARTITION BY key 
                        ORDER BY event_time 
                        RANGE BETWEEN INTERVAL '1000' MILLISECOND PRECEDING AND CURRENT ROW
                    ) as sum_value
                   FROM events";
        
        let graph = planner.sql_to_graph(sql).unwrap();
        
        // Verify structure after conversion
        let nodes_after: Vec<_> = graph.get_nodes().collect();
        assert_eq!(nodes_after.len(), 8, "Should have 8 nodes after conversion (original 4 + request_source + keyby + window_request + request_sink)");
        
        // Verify edge connectivity using verify_edge_connectivity helper
        // Expected structure after conversion:
        //   - source -> keyby -> window (window has no outgoing edges)
        //   - request_source -> keyby -> window_request -> projection (root)
        //   - root -> request_sink
        verify_edge_connectivity(&graph, &[
            (NodeType::Source, NodeType::KeyBy, PartitionType::RoundRobin, 2), // original source -> keyby + request_source -> keyby
            (NodeType::KeyBy, NodeType::Window, PartitionType::Hash, 1), // original keyby -> window
            (NodeType::KeyBy, NodeType::WindowRequest, PartitionType::Hash, 1), // new keyby -> window_request
            (NodeType::WindowRequest, NodeType::Projection, PartitionType::RoundRobin, 1), // window_request -> projection
            (NodeType::Projection, NodeType::Sink, PartitionType::RequestRoute, 1), // root -> request_sink (uses RequestRoute partition type)
        ]);
        
        // Verify window node has no outgoing edges
        let window_node = nodes_after.iter()
            .find(|n| matches!(n.operator_config, OperatorConfig::WindowConfig(_)))
            .expect("Should have window node");
        let window_node_idx = graph.get_node_index(&window_node.operator_id)
            .expect("Should find window node index");
        let outgoing_after = graph.get_neighbors(window_node_idx, Direction::Outgoing);
        assert_eq!(outgoing_after.len(), 0, "Window should have no outgoing edges after conversion");
    }
} 