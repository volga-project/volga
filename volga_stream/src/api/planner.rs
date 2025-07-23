use std::collections::HashMap;
use std::sync::Arc;
use std::mem;
use arrow::datatypes::SchemaRef;
use datafusion::catalog::MemTable;
use datafusion::logical_expr::{
    LogicalPlan, Projection, Filter, Aggregate, Join, Window, TableScan, 
    SubqueryAlias, Subquery, Sort, Union, Distinct, Limit, Repartition,
    Expr, BinaryExpr, expr::ScalarFunction
};
use datafusion::common::{Result, DataFusionError};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::tree_node::{TreeNode, TreeNodeVisitor};
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use petgraph::graph::NodeIndex;

use super::logical_graph::{LogicalNode, LogicalGraph,
    EdgeType, ConnectorConfig
};
use crate::runtime::operators::operator::OperatorConfig;
use crate::runtime::functions::map::{FilterFunction, ProjectionFunction, MapFunction};
use crate::runtime::functions::join::join_function::JoinFunction;
use crate::runtime::operators::source::source_operator::SourceConfig;


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
}

impl PlanningContext {
    pub fn new(df_session_context: SessionContext) -> Self {
        Self {
            df_session_context,
            connector_configs: HashMap::new(),
        }
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

        // dummy mem table
        let table = MemTable::try_new(schema, vec![]).unwrap();
        self.context.df_session_context.register_table(
            TableReference::Bare {
                table: table_name.as_str().into(),
            },
            Arc::new(table),
        ).unwrap();
    }

    pub fn logical_plan_to_graph(&mut self, logical_plan: &LogicalPlan) -> Result<LogicalGraph> {
        self.node_stack.clear();

        logical_plan.visit_with_subqueries(self)?;
        
        Ok(self.logical_graph.clone())
    }

    pub async fn sql_to_graph(&mut self, sql: &str) -> Result<LogicalGraph> {
        let logical_plan = self.context.df_session_context.state().create_logical_plan(sql).await?;
        // println!("logical_plan: {}", logical_plan.display_indent());

        // TODO: optimize logical plan
        
        self.logical_plan_to_graph(&logical_plan)
    }

    fn create_source_node(&mut self, table_scan: &TableScan) -> Result<NodeIndex> {
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
            1, // TODO: get from context
            None, // TODO set schemas
            None,
        );

        let node_index = self.logical_graph.add_node(node);

        Ok(node_index)
    }

    fn create_projection_node(&mut self, projection: &Projection) -> Result<NodeIndex> {
        let projection_function = ProjectionFunction::new(
            projection.input.schema().clone(), 
            projection.schema.clone(),
            projection.expr.clone(), 
            self.context.df_session_context.clone()
        );
        
        let node = LogicalNode::new(
            OperatorConfig::MapConfig(MapFunction::Projection(projection_function)),
            1, // TODO: get from context
            None, // TODO set schemas
            None,
        );

        let node_index = self.logical_graph.add_node(node);
        Ok(node_index)
    }

    fn create_filter_node(&mut self, filter: &Filter) -> Result<NodeIndex> {
        let filter_function = FilterFunction::new(
            filter.input.schema().clone(), 
            filter.predicate.clone(), 
            self.context.df_session_context.clone(), 
        );
        
        let node = LogicalNode::new(
            OperatorConfig::MapConfig(MapFunction::Filter(filter_function)),
            1, // TODO: get from context
            None,
            None,
        );

        let node_index = self.logical_graph.add_node(node);
        Ok(node_index)
    }

    fn create_join_node(&mut self, join: &Join) -> Result<NodeIndex> {
        let join_function = JoinFunction::new();
        
        let node = LogicalNode::new(
            OperatorConfig::JoinConfig(join_function),
            1, // TODO: get from context
            None,
            None,
        );
        
        let node_index = self.logical_graph.add_node(node);
        
        Ok(node_index)
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
                Some(self.create_source_node(table_scan)?)
            }
            
            LogicalPlan::Projection(projection) => {
                Some(self.create_projection_node(projection)?)
            }
            
            LogicalPlan::Filter(filter) => {
                Some(self.create_filter_node(filter)?)
            }
            
            LogicalPlan::Join(join) => {    
                Some(self.create_join_node(join)?)
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

    fn create_planner() -> Planner {
        let ctx = SessionContext::new();
        let mut planner = Planner::new(PlanningContext {
            df_session_context: ctx,
            connector_configs: HashMap::new(),
        });
        
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

    #[tokio::test]
    async fn test_simple_select() {
        let mut planner = create_planner();
        
        let sql = "SELECT id, name FROM test_table";
        let graph = planner.sql_to_graph(sql).await.unwrap();

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

    #[tokio::test]
    async fn test_select_with_filter() {
        let mut planner = create_planner();
        
        let sql = "SELECT id, name FROM test_table WHERE value > 3.0";
        let graph = planner.sql_to_graph(sql).await.unwrap();

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

    #[tokio::test]
    async fn test_forward_edges_connectivity() {
        let mut planner = create_planner();
        
        let sql = "SELECT id, name FROM test_table WHERE value > 3.0";
        let graph = planner.sql_to_graph(sql).await.unwrap();
        
        // Check edges
        let edges: Vec<_> = graph.get_edges().collect();
        assert_eq!(edges.len(), 2); // source->filter, filter->projection
        
        // Verify edge types are Forward
        for (_, _, edge) in edges {
            assert!(matches!(edge.edge_type, EdgeType::Forward));
        }
    }

    #[tokio::test]
    async fn test_join_tables() {
        let mut planner = create_planner();
        
        let sql = "SELECT t1.id, t1.name, t2.value FROM test_table t1 JOIN test_table2 t2 ON t1.id = t2.id";
        
        let graph = planner.sql_to_graph(sql).await.unwrap();
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
} 