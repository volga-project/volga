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

        logical_plan.visit(self)?;
        
        Ok(self.logical_graph.clone())
    }

    pub async fn sql_to_graph(&mut self, sql: &str) -> Result<LogicalGraph> {
        let logical_plan = self.context.df_session_context.state().create_logical_plan(sql).await?;
        
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

        // Add edge from previous node if it exists
        if let Some(prev_node_index) = self.node_stack.last() {
            self.logical_graph.add_edge(*prev_node_index, node_index, EdgeType::Forward);
        }

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

        // Add edge from previous node if it exists
        if let Some(prev_node_index) = self.node_stack.last() {
            self.logical_graph.add_edge(*prev_node_index, node_index, EdgeType::Forward);
        }

        Ok(node_index)
    }
}

impl<'a> TreeNodeVisitor<'a> for Planner {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: &'a Self::Node) -> Result<TreeNodeRecursion> {
        // Process node and create operator
        let node_index = match node {
            LogicalPlan::TableScan(table_scan) => {
                self.create_source_node(table_scan)?
            }
            
            LogicalPlan::Projection(projection) => {
                self.create_projection_node(projection)?
            }
            
            LogicalPlan::Filter(filter) => {
                self.create_filter_node(filter)?
            }
            
            LogicalPlan::Aggregate(aggregate) => {
                // Create a key-by followed by aggregate
                panic!("Unsupported");
            }
            
            _ => {
                panic!("Unsupported logical plan: {:?}", node);
            }
        };
        
        // Add node to stack
        self.node_stack.push(node_index);
        
        Ok(TreeNodeRecursion::Continue)
    }

    fn f_up(&mut self, _node: &'a Self::Node) -> Result<TreeNodeRecursion> {
        // Pop the current node from stack
        if !self.node_stack.is_empty() {
            self.node_stack.pop();
        }
        
        Ok(TreeNodeRecursion::Continue)
    }
}

#[cfg(test)]
mod tests {
    use crate::runtime::operators::source::source_operator::VectorSourceConfig;

    use super::*;
    use arrow::datatypes::{Schema, Field, DataType};
    use arrow::array::{Int32Array, StringArray, Float64Array};
    use arrow::record_batch::RecordBatch;
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
        
        // Create test data
        // let batch = RecordBatch::try_new(
        //     schema.clone(),
        //     vec![
        //         Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
        //         Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
        //         Arc::new(Float64Array::from(vec![1.1, 2.2, 3.3, 4.4, 5.5])),
        //     ],
        // ).unwrap();
        
        // Register table
        // ctx.register_batch("test_table", batch).unwrap();

        // Register source
        planner.register_source("test_table".to_string(), SourceConfig::VectorSourceConfig(VectorSourceConfig::new(vec![])), schema);
        
        planner
    }

    #[tokio::test]
    async fn test_simple_select() {
        let mut planner = create_planner();
        
        let sql = "SELECT id, name FROM test_table";
        let graph = planner.sql_to_graph(sql).await.unwrap();
        
        // Should have 2 nodes: source and projection
        let nodes: Vec<_> = graph.get_nodes().collect();
        assert_eq!(nodes.len(), 2);
        
        // First node should be source
        assert!(matches!(nodes[0].operator_config, OperatorConfig::SourceConfig(_)));
        
        // Second node should be projection
        assert!(matches!(nodes[1].operator_config, OperatorConfig::MapConfig(MapFunction::Projection(_))));
    }

    #[tokio::test]
    async fn test_select_with_filter() {
        let mut planner = create_planner();
        
        let sql = "SELECT id, name FROM test_table WHERE value > 3.0";
        let graph = planner.sql_to_graph(sql).await.unwrap();
        
        // Should have 3 nodes: source, filter, projection
        let nodes: Vec<_> = graph.get_nodes().collect();
        assert_eq!(nodes.len(), 3);
        
        // Check node types
        assert!(matches!(nodes[0].operator_config, OperatorConfig::SourceConfig(_)));
        assert!(matches!(nodes[1].operator_config, OperatorConfig::MapConfig(MapFunction::Filter(_))));
        assert!(matches!(nodes[2].operator_config, OperatorConfig::MapConfig(MapFunction::Projection(_))));
    }

    #[tokio::test]
    async fn test_filter_only() {
        let mut planner = create_planner();
        
        let sql = "SELECT * FROM test_table WHERE id > 2";
        let graph = planner.sql_to_graph(sql).await.unwrap();
        
        // Should have 2 nodes: source and filter
        let nodes: Vec<_> = graph.get_nodes().collect();
        assert_eq!(nodes.len(), 2);
        
        assert!(matches!(nodes[0].operator_config, OperatorConfig::SourceConfig(_)));
        assert!(matches!(nodes[1].operator_config, OperatorConfig::MapConfig(MapFunction::Filter(_))));
    }

    #[tokio::test]
    async fn test_select_all_columns() {
        let mut planner = create_planner();
        
        let sql = "SELECT * FROM test_table";
        let graph = planner.sql_to_graph(sql).await.unwrap();
        
        // Should have 1 node: source only (no projection needed for SELECT *)
        let nodes: Vec<_> = graph.get_nodes().collect();
        assert_eq!(nodes.len(), 1);
        
        assert!(matches!(nodes[0].operator_config, OperatorConfig::SourceConfig(_)));
    }

    #[tokio::test]
    async fn test_complex_filter() {
        let mut planner = create_planner();
        
        let sql = "SELECT id, name FROM test_table WHERE value > 2.0 AND id < 5";
        let graph = planner.sql_to_graph(sql).await.unwrap();
        
        // Should have 3 nodes: source, filter, projection
        let nodes: Vec<_> = graph.get_nodes().collect();
        assert_eq!(nodes.len(), 3);
        
        assert!(matches!(nodes[0].operator_config, OperatorConfig::SourceConfig(_)));
        assert!(matches!(nodes[1].operator_config, OperatorConfig::MapConfig(MapFunction::Filter(_))));
        assert!(matches!(nodes[2].operator_config, OperatorConfig::MapConfig(MapFunction::Projection(_))));
    }

    #[tokio::test]
    async fn test_edges_connectivity() {
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
} 