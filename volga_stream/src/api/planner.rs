use std::collections::HashMap;
use std::sync::Arc;
use arrow::datatypes::SchemaRef;
use datafusion::datasource::TableProvider;
use datafusion::physical_plan::{ExecutionPlan};
use datafusion::physical_plan::memory::{LazyMemoryExec, LazyBatchGenerator};
use arrow::record_batch::RecordBatch;
use parking_lot::RwLock;
use std::sync::Arc as StdArc;
use std::fmt;
use datafusion::logical_expr::TableType;
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

use super::logical_graph::{LogicalNode, LogicalGraph, EdgeType};
use crate::runtime::operators::operator::OperatorConfig;
use crate::runtime::functions::map::{FilterFunction, ProjectionFunction, MapFunction};
use crate::runtime::functions::join::join_function::JoinFunction;
use crate::runtime::operators::source::source_operator::SourceConfig;
use crate::runtime::operators::sink::sink_operator::SinkConfig;
use crate::runtime::operators::aggregate::aggregate_operator::AggregateConfig;

/// Custom table provider that creates execution plans with proper partitioning
/// Similar to Arroyo's LogicalBatchInput
#[derive(Debug, Clone)]
pub struct VolgaTableProvider {
    table_name: String,
    schema: SchemaRef,
}

impl VolgaTableProvider {
    pub fn new(table_name: String, schema: SchemaRef) -> Self {
        Self { table_name, schema }
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
            schema: SchemaRef,
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
            schema: self.schema.clone(),
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

    // TODO figure out how to set parallelism per node
    pub parallelism: usize,
}

impl PlanningContext {
    pub fn new(df_session_context: SessionContext) -> Self {
        Self {
            df_session_context,
            connector_configs: HashMap::new(),
            sink_config: None,
            parallelism: 1, // Default parallelism
        }
    }

    pub fn with_parallelism(mut self, parallelism: usize) -> Self {
        self.parallelism = parallelism;
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

    pub fn register_source(&mut self, table_name: String, config: SourceConfig, schema: SchemaRef) {
        self.context.connector_configs.insert(table_name.clone(), config);

        // Use custom table provider that creates proper partitioning
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

        logical_plan.visit_with_subqueries(self)?;
        
        Ok(self.logical_graph.clone())
    }

    pub fn sql_to_graph(&mut self, sql: &str) -> Result<LogicalGraph> {
        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        
        let logical_plan = rt.block_on(
            self.context.df_session_context.state().create_logical_plan(sql)
        )?;
        println!("logical_plan: {}", logical_plan.display_indent());

        // Optimize the logical plan (apply type coercion, etc.)
        let optimized_plan = self.optimize_plan(logical_plan)?;
        println!("optimized_plan: {}", optimized_plan.display_indent());
        
        self.logical_plan_to_graph(&optimized_plan)
    }

    fn create_source_node(&mut self, table_scan: &TableScan, parallelism: usize) -> Result<NodeIndex> {
        let table_name = table_scan.table_name.table();
        let mut source_config = self.context.connector_configs.get(table_name)
            .ok_or_else(|| DataFusionError::Plan(format!("No source configuration found for table '{}'", table_name)))?
            .clone();

        if table_scan.projection.is_some() {
            let projection = table_scan.projection.as_ref().unwrap().clone();
            let schema = table_scan.projected_schema.inner().clone();
            source_config.set_projection(projection, schema);
        }
        
        let node = LogicalNode::new(
            OperatorConfig::SourceConfig(source_config),
            parallelism,
            None, // TODO set schemas
            None,
        );

        let node_index = self.logical_graph.add_node(node);

        Ok(node_index)
    }

    fn create_projection_node(&mut self, projection: &Projection, parallelism: usize) -> Result<NodeIndex> {
        let projection_function = ProjectionFunction::new(
            projection.input.schema().clone(), 
            projection.schema.clone(),
            projection.expr.clone(), 
            self.context.df_session_context.clone()
        );
        
        let node = LogicalNode::new(
            OperatorConfig::MapConfig(MapFunction::Projection(projection_function)),
            parallelism,
            None, // TODO set schemas
            None,
        );

        let node_index = self.logical_graph.add_node(node);
        Ok(node_index)
    }

    fn create_filter_node(&mut self, filter: &Filter, parallelism: usize) -> Result<NodeIndex> {
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
        Ok(node_index)
    }

    fn create_join_node(&mut self, _join: &Join, parallelism: usize) -> Result<NodeIndex> {
        let join_function = JoinFunction::new();
        
        let node = LogicalNode::new(
            OperatorConfig::JoinConfig(join_function),
            parallelism,
            None,
            None,
        );
        
        let node_index = self.logical_graph.add_node(node);
        
        Ok(node_index)
    }

    fn create_sink_node(&mut self, sink_config: SinkConfig, root_node_index: NodeIndex, parallelism: usize) {
        // Create sink node
        let sink_node = LogicalNode::new(
            OperatorConfig::SinkConfig(sink_config),
            parallelism,
            None,
            None,
        );
        let sink_node_index = self.logical_graph.add_node(sink_node);
        
        self.logical_graph.add_edge(sink_node_index, root_node_index, EdgeType::Forward);
    }

    // TODO figure out on final vs partial plan 
    fn handle_aggregate(&mut self, aggregate: &Aggregate, parallelism: usize) -> Result<NodeIndex> {
        // Create a logical plan node just for this aggregate
        let aggregate_plan = LogicalPlan::Aggregate(aggregate.clone());
        
        println!("Aggregate expressions: {:?}", aggregate.aggr_expr);

        // Use DataFusion's physical planner to convert to AggregateExec
        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        
        let planner = DefaultPhysicalPlanner::default();
        let session_state = self.context.df_session_context.state();
        let physical_plan = rt.block_on(
            planner.create_physical_plan(&aggregate_plan, &session_state)
        ).unwrap();
        
        println!("Full physical plan tree:");
        Self::print_physical_plan(&physical_plan, 0);
        
        // DataFusion creates a two-stage plan: Partial -> Final
        // Let's examine both stages to see which has proper coercion
        let partial_aggregate_exec = Self::extract_partial_aggregate_exec(&physical_plan);
        let final_aggregate_exec = Self::extract_final_aggregate_exec(&physical_plan);
        
        println!("\n=== PARTIAL AGGREGATE ===");
        if let Some(ref partial) = partial_aggregate_exec {
            Self::print_aggregate_details("Partial", partial);
        } else {
            println!("No Partial AggregateExec found");
        }
        
        println!("\n=== FINAL AGGREGATE ===");
        if let Some(ref final_agg) = final_aggregate_exec {
            Self::print_aggregate_details("Final", final_agg);
        } else {
            println!("No Final AggregateExec found");
        }
        
        // For now, try the Final aggregate instead of Partial
        let aggregate_exec = final_aggregate_exec
            .or(partial_aggregate_exec)
            .ok_or_else(|| DataFusionError::Internal("Could not find any AggregateExec in physical plan".to_string()))?;

        // Debug: Print the selected aggregate expressions
        println!("\n=== SELECTED AGGREGATE ({:?}) ===", aggregate_exec.mode());
        Self::print_aggregate_details("Selected", &aggregate_exec);

        // Create our operator config
        let node = LogicalNode::new(
            OperatorConfig::AggregateConfig(AggregateConfig {
                aggregate_exec,
            }),
            parallelism,
            None,
            None,
        );

        let node_index = self.logical_graph.add_node(node);
        Ok(node_index)
    }

    /// Recursively search for the Partial mode AggregateExec in the physical plan tree
    fn extract_partial_aggregate_exec(plan: &Arc<dyn ExecutionPlan>) -> Option<AggregateExec> {
        use datafusion::physical_plan::aggregates::AggregateMode;
        
        // Check if this is an AggregateExec with Partial mode
        if let Some(agg_exec) = plan.as_any().downcast_ref::<AggregateExec>() {
            if matches!(agg_exec.mode(), AggregateMode::Partial) {
                return Some(agg_exec.clone());
            }
        }
        
        // Recursively search in children
        for child in plan.children() {
            if let Some(partial_agg) = Self::extract_partial_aggregate_exec(&child) {
                return Some(partial_agg);
            }
        }
        
        None
    }
    
    /// Recursively search for the Final/FinalPartitioned mode AggregateExec in the physical plan tree
    fn extract_final_aggregate_exec(plan: &Arc<dyn ExecutionPlan>) -> Option<AggregateExec> {
        use datafusion::physical_plan::aggregates::AggregateMode;
        
        // Check if this is an AggregateExec with Final/FinalPartitioned mode
        if let Some(agg_exec) = plan.as_any().downcast_ref::<AggregateExec>() {
            if matches!(agg_exec.mode(), AggregateMode::Final | AggregateMode::FinalPartitioned) {
                return Some(agg_exec.clone());
            }
        }
        
        // Recursively search in children
        for child in plan.children() {
            if let Some(final_agg) = Self::extract_final_aggregate_exec(&child) {
                return Some(final_agg);
            }
        }
        
        None
    }
    
    /// Print the physical plan tree structure
    fn print_physical_plan(plan: &Arc<dyn ExecutionPlan>, indent: usize) {
        let indent_str = "  ".repeat(indent);
        println!("{}[{}] {:?}", indent_str, plan.name(), plan);
        
        for child in plan.children() {
            Self::print_physical_plan(&child, indent + 1);
        }
    }
    
    /// Print detailed information about an AggregateExec
    fn print_aggregate_details(label: &str, agg_exec: &AggregateExec) {
        println!("{} AggregateExec mode: {:?}", label, agg_exec.mode());
        println!("{} expressions:", label);
        for (i, expr) in agg_exec.aggr_expr().iter().enumerate() {
            println!("  [{}] {}: return_field={:?}", i, expr.name(), expr.field());
            println!("      expressions: {:?}", expr.expressions().iter().map(|e| format!("{:?}", e)).collect::<Vec<_>>());
            println!("      input_fields: {:?}", expr.expressions().iter().map(|e| format!("{:?}", e.data_type(&agg_exec.input_schema()))).collect::<Vec<_>>());
        }
    }
    
    /// Apply optimization rules to a logical plan
    /// Currently only applies type coercion
    fn optimize_plan(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        let session_state = self.context.df_session_context.state();
        let config = session_state.config_options();
        
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
        let node_index = match node {
            LogicalPlan::TableScan(table_scan) => {
                Some(self.create_source_node(table_scan, self.context.parallelism)?)
            }
            
            LogicalPlan::Projection(projection) => {
                Some(self.create_projection_node(projection, self.context.parallelism)?)
            }
            
            LogicalPlan::Filter(filter) => {
                Some(self.create_filter_node(filter, self.context.parallelism)?)
            }
            
            LogicalPlan::Join(join) => {    
                Some(self.create_join_node(join, self.context.parallelism)?)
            }

            LogicalPlan::Aggregate(aggregate) => {
                println!("Aggregate node");
        
                Some(self.handle_aggregate(aggregate, self.context.parallelism)?)
            }
            
            // skip subqueries as they simply wrap other plans
            LogicalPlan::Subquery(_) => {None}
            LogicalPlan::SubqueryAlias(_) => {None}
            
            _ => {
                panic!("Unsupported logical plan: {:?}", node);
            }
        };
        
        // Add node to stack
        if let Some(node_index) = node_index {
            self.node_stack.push(node_index);
        }
        
        Ok(TreeNodeRecursion::Continue)
    }

    // Add edges from temp stack to graph, going bottom-to-top
    fn f_up(&mut self, _node: &'a Self::Node) -> Result<TreeNodeRecursion> {
        if self.node_stack.is_empty() {
            return Ok(TreeNodeRecursion::Continue);
        }

        let node_index = self.node_stack.pop().unwrap();
        if let Some(prev_node_index) = self.node_stack.last() {
            // All nodes are using forward edges for now
            // TODO figure out edge types for groubys and joins
            self.logical_graph.add_edge(*prev_node_index, node_index, EdgeType::Forward);
        } else {
            // no prev node - this is the root of the plan
            // if sink is configured add here
            if let Some(sink_config) = &self.context.sink_config {
                self.create_sink_node(sink_config.clone(), node_index, self.context.parallelism);
            }
        }
        
        Ok(TreeNodeRecursion::Continue)
    }
}

#[cfg(test)]
mod tests {
    use crate::runtime::operators::source::source_operator::VectorSourceConfig;

    use super::*;
    use arrow::datatypes::{Schema, Field, DataType};
    use std::sync::Arc;
    use crate::runtime::operators::sink::sink_operator::SinkConfig;

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

    #[test]
    fn test_simple_select() {
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
    }

    #[test]
    fn test_simple_select_with_sink() {
        let mut planner = create_planner();
        
        // Register sink
        planner.register_sink(SinkConfig::InMemoryStorageGrpcSinkConfig("http://127.0.0.1:8080".to_string()));
        
        let sql = "SELECT id, name FROM test_table";
        let graph = planner.sql_to_graph(sql).unwrap();

        // println!("{}", graph);
        
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
    }

    #[test]
    fn test_select_with_filter() {
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
    }

    #[test]
    fn test_forward_edges_connectivity() {
        let mut planner = create_planner();
        
        let sql = "SELECT id, name FROM test_table WHERE value > 3.0";
        let graph = planner.sql_to_graph(sql).unwrap();
        
        // Check edges
        let edges: Vec<_> = graph.get_edges().collect();
        assert_eq!(edges.len(), 2); // source->filter, filter->projection
        
        // Verify edge types are Forward
        for (_, _, edge) in edges {
            assert!(matches!(edge.edge_type, EdgeType::Forward));
        }
    }

    #[test]
    fn test_join_tables() {
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

    #[test]
    fn test_group_by() {
        let mut planner = create_planner();
        
        let sql = "SELECT name, COUNT(*) as count FROM test_table GROUP BY name";
        
        // This will fail until we implement GROUP BY support
        let _result = planner.sql_to_graph(sql);
        // assert!(result.is_err(), "Expected error due to unimplemented GROUP BY support");
    }
} 