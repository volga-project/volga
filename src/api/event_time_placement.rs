//! Resolve where auto watermark assigners should attach by walking DataFusion
//! plan lineage from a window's ORDER BY event-time column to its defining site.

use datafusion::common::{Column, DataFusionError, Result};
use datafusion::logical_expr::{Expr, LogicalPlan, Projection, Window};
use petgraph::graph::NodeIndex;
use petgraph::Direction;

use super::logical_graph::LogicalGraph;
use crate::runtime::operators::operator::OperatorConfig;

/// Plan site where a watermark assigner should be attached.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignSite {
    /// Column name in the assign site's output schema.
    pub column_name: String,
    /// Identity of the defining [`LogicalPlan`] node (`ptr` as usize).
    pub plan_node_key: usize,
}

pub fn plan_node_key(plan: &LogicalPlan) -> usize {
    plan as *const LogicalPlan as usize
}

fn unwrap_alias(expr: &Expr) -> &Expr {
    match expr {
        Expr::Alias(alias) => unwrap_alias(alias.expr.as_ref()),
        other => other,
    }
}

/// Extract the event-time column from the first window ORDER BY expression.
pub fn event_time_column_from_window(window: &Window) -> Result<Column> {
    let first = window.window_expr.first().ok_or_else(|| {
        DataFusionError::Plan("window has no window expressions".to_string())
    })?;

    let Expr::WindowFunction(wf) = unwrap_alias(first) else {
        return Err(DataFusionError::Plan(format!(
            "expected WindowFunction for watermark lineage, got {first:?}"
        )));
    };

    let order = wf.params.order_by.first().ok_or_else(|| {
        DataFusionError::Plan("window ORDER BY is required for auto watermark placement".to_string())
    })?;

    match unwrap_alias(&order.expr) {
        Expr::Column(col) => Ok(col.clone()),
        other => Err(DataFusionError::Plan(format!(
            "window ORDER BY must be a column for auto watermark placement, got {other:?}"
        ))),
    }
}

fn find_projection_expr<'a>(proj: &'a Projection, col_name: &str) -> Result<&'a Expr> {
    for (idx, field) in proj.schema.fields().iter().enumerate() {
        if field.name() == col_name {
            return proj.expr.get(idx).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "projection schema/expr mismatch looking for column '{col_name}'"
                ))
            });
        }
    }
    for expr in &proj.expr {
        let (_, name) = expr.qualified_name();
        if name == col_name {
            return Ok(expr);
        }
    }
    Err(DataFusionError::Plan(format!(
        "projection does not define event-time column '{col_name}'"
    )))
}

/// Walk backward from `window.input` to the plan node that defines the event-time column.
pub fn resolve_event_time_assign_site(window: &Window) -> Result<AssignSite> {
    let mut col = event_time_column_from_window(window)?;
    let mut current = window.input.as_ref();

    loop {
        match current {
            LogicalPlan::Filter(filter) => {
                current = filter.input.as_ref();
            }
            LogicalPlan::SubqueryAlias(alias) => {
                current = alias.input.as_ref();
            }
            LogicalPlan::Sort(sort) => {
                current = sort.input.as_ref();
            }
            LogicalPlan::Limit(limit) => {
                current = limit.input.as_ref();
            }
            LogicalPlan::Repartition(repartition) => {
                current = repartition.input.as_ref();
            }
            LogicalPlan::Distinct(distinct) => {
                current = distinct.input().as_ref();
            }
            LogicalPlan::Projection(proj) => {
                let expr = find_projection_expr(proj, &col.name)?;
                match unwrap_alias(expr) {
                    Expr::Column(inner) => {
                        col = inner.clone();
                        current = proj.input.as_ref();
                    }
                    _ => {
                        // Computed (or otherwise non-column) expression — assign here.
                        return Ok(AssignSite {
                            column_name: col.name.clone(),
                            plan_node_key: plan_node_key(current),
                        });
                    }
                }
            }
            LogicalPlan::TableScan(_) => {
                return Ok(AssignSite {
                    column_name: col.name.clone(),
                    plan_node_key: plan_node_key(current),
                });
            }
            LogicalPlan::Join(_) | LogicalPlan::Union(_) => {
                return Err(DataFusionError::Plan(
                    "unsupported for auto watermark placement: multi-input operator on event-time lineage"
                        .to_string(),
                ));
            }
            other => {
                return Err(DataFusionError::Plan(format!(
                    "unsupported for auto watermark placement on event-time lineage: {}",
                    other.display()
                )));
            }
        }
    }
}

fn reaches(graph: &LogicalGraph, from: NodeIndex, to: NodeIndex) -> bool {
    if from == to {
        return true;
    }
    let mut visited = std::collections::HashSet::new();
    let mut stack = vec![from];
    while let Some(n) = stack.pop() {
        if !visited.insert(n) {
            continue;
        }
        for neigh in graph.get_neighbors(n, Direction::Outgoing) {
            if neigh == to {
                return true;
            }
            stack.push(neigh);
        }
    }
    false
}

/// Assign site must be upstream of the window's KeyBy (fragmenting edge), and must not
/// land on KeyBy/Window themselves.
pub fn validate_assign_before_window_keyby(
    graph: &LogicalGraph,
    assign_idx: NodeIndex,
    window_idx: NodeIndex,
) -> Result<()> {
    let assign_cfg = &graph.get_node_by_index(assign_idx).operator_config;
    if matches!(
        assign_cfg,
        OperatorConfig::KeyByConfig(_) | OperatorConfig::WindowConfig(_)
    ) {
        return Err(DataFusionError::Plan(
            "event-time watermark assign must not land on KeyBy or Window".to_string(),
        ));
    }

    let keyby_idx = graph
        .get_neighbors(window_idx, Direction::Incoming)
        .into_iter()
        .find(|&idx| {
            matches!(
                graph.get_node_by_index(idx).operator_config,
                OperatorConfig::KeyByConfig(_)
            )
        })
        .ok_or_else(|| {
            DataFusionError::Plan(
                "window has no KeyBy predecessor for watermark placement validation".to_string(),
            )
        })?;

    if !reaches(graph, assign_idx, keyby_idx) {
        return Err(DataFusionError::Plan(
            "event-time assign site must be upstream of the window's KeyBy".to_string(),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::catalog::MemTable;
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;

    fn events_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]))
    }

    async fn plan_sql(sql: &str) -> LogicalPlan {
        let ctx = SessionContext::new();
        let table = MemTable::try_new(events_schema(), vec![vec![]]).unwrap();
        ctx.register_table("events", Arc::new(table)).unwrap();
        let df = ctx.sql(sql).await.unwrap();
        df.into_optimized_plan().unwrap()
    }

    fn find_window(plan: &LogicalPlan) -> Window {
        let mut found = None;
        plan.apply(|node| {
            if let LogicalPlan::Window(w) = node {
                found = Some(w.clone());
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();
        found.expect("expected a Window in plan")
    }

    fn find_plan_by_key<'a>(plan: &'a LogicalPlan, key: usize) -> Option<&'a LogicalPlan> {
        let mut found = None;
        plan.apply(|node| {
            if plan_node_key(node) == key {
                found = Some(node);
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();
        found
    }

    #[tokio::test]
    async fn lineage_passthrough_column_stops_at_source() {
        let plan = plan_sql(
            "SELECT timestamp, key, value, \
             SUM(value) OVER (PARTITION BY key ORDER BY timestamp \
             RANGE BETWEEN INTERVAL '1000' MILLISECOND PRECEDING AND CURRENT ROW) as sum_value \
             FROM events",
        )
        .await;
        let window = find_window(&plan);
        let site = resolve_event_time_assign_site(&window).unwrap();
        assert_eq!(site.column_name, "timestamp");
        let site_plan = find_plan_by_key(&plan, site.plan_node_key).unwrap();
        assert!(matches!(site_plan, LogicalPlan::TableScan(_)));
    }

    #[tokio::test]
    async fn lineage_renamed_column_continues_to_source() {
        let plan = plan_sql(
            "SELECT event_time, key, value, \
             SUM(value) OVER (PARTITION BY key ORDER BY event_time \
             RANGE BETWEEN INTERVAL '1000' MILLISECOND PRECEDING AND CURRENT ROW) as sum_value \
             FROM (SELECT timestamp AS event_time, key, value FROM events)",
        )
        .await;
        let window = find_window(&plan);
        let site = resolve_event_time_assign_site(&window).unwrap();
        // After unwrapping the alias, assign on the source under the original column name.
        assert_eq!(site.column_name, "timestamp");
        let site_plan = find_plan_by_key(&plan, site.plan_node_key).unwrap();
        assert!(matches!(site_plan, LogicalPlan::TableScan(_)));
    }

    #[tokio::test]
    async fn lineage_computed_event_time_stops_on_projection() {
        let plan = plan_sql(
            "SELECT event_time, key, value, \
             SUM(value) OVER (PARTITION BY key ORDER BY event_time \
             RANGE BETWEEN INTERVAL '1000' MILLISECOND PRECEDING AND CURRENT ROW) as sum_value \
             FROM (SELECT timestamp + INTERVAL '0' MILLISECOND AS event_time, key, value FROM events)",
        )
        .await;
        let window = find_window(&plan);
        let site = resolve_event_time_assign_site(&window).unwrap();
        assert_eq!(site.column_name, "event_time");
        let site_plan = find_plan_by_key(&plan, site.plan_node_key).unwrap();
        assert!(matches!(site_plan, LogicalPlan::Projection(_)));
    }

    #[tokio::test]
    async fn lineage_join_on_path_errors() {
        let ctx = SessionContext::new();
        let table = MemTable::try_new(events_schema(), vec![vec![]]).unwrap();
        ctx.register_table("events", Arc::new(table)).unwrap();
        let table2 = MemTable::try_new(events_schema(), vec![vec![]]).unwrap();
        ctx.register_table("events2", Arc::new(table2)).unwrap();

        let sql = "SELECT e.timestamp, e.key, e.value, \
             SUM(e.value) OVER (PARTITION BY e.key ORDER BY e.timestamp \
             RANGE BETWEEN INTERVAL '1000' MILLISECOND PRECEDING AND CURRENT ROW) as sum_value \
             FROM events e JOIN events2 e2 ON e.key = e2.key";
        let df = ctx.sql(sql).await.unwrap();
        let plan = df.into_optimized_plan().unwrap();
        let window = find_window(&plan);
        let err = resolve_event_time_assign_site(&window).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("unsupported for auto watermark placement"),
            "unexpected error: {msg}"
        );
    }
}
