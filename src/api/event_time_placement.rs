//! Resolve where auto watermark assigners should attach by walking DataFusion
//! plan lineage from a window's ORDER BY event-time column to its defining site.

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DataFusionError, Result};
use datafusion::logical_expr::{Expr, LogicalPlan, Projection, Window};
use datafusion::physical_plan::expressions::Column as PhysicalColumn;
use petgraph::graph::NodeIndex;
use petgraph::Direction;

use super::logical_graph::LogicalGraph;
use crate::runtime::functions::map::MapFunction;
use crate::runtime::operators::operator::OperatorConfig;

/// Plan site where a watermark assigner should be attached.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AssignSite {
    Source {
        /// DF table name (lineage / diagnostics; graph match uses unique upstream Source).
        table_name: String,
        /// Column name in the source output schema.
        column_name: String,
    },
    /// Projection that defines a computed (non-column) event-time expression.
    Projection {
        column_name: String,
    },
}

impl AssignSite {
    pub fn column_name(&self) -> &str {
        match self {
            Self::Source { column_name, .. } | Self::Projection { column_name } => column_name,
        }
    }
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
                        return Ok(AssignSite::Projection {
                            column_name: col.name.clone(),
                        });
                    }
                }
            }
            LogicalPlan::TableScan(scan) => {
                return Ok(AssignSite::Source {
                    table_name: scan.table_name.table().to_string(),
                    column_name: col.name.clone(),
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

fn window_keyby(graph: &LogicalGraph, window_idx: NodeIndex) -> Result<NodeIndex> {
    graph
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
        })
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

    let keyby_idx = window_keyby(graph, window_idx)?;
    if !reaches(graph, assign_idx, keyby_idx) {
        return Err(DataFusionError::Plan(
            "event-time assign site must be upstream of the window's KeyBy".to_string(),
        ));
    }

    Ok(())
}

fn window_order_by_column_name(graph: &LogicalGraph, window_idx: NodeIndex) -> Option<String> {
    let OperatorConfig::WindowConfig(cfg) = &graph.get_node_by_index(window_idx).operator_config
    else {
        return None;
    };
    let order = cfg.window_exec.window_expr().first()?.order_by().first()?;
    order
        .expr
        .as_any()
        .downcast_ref::<PhysicalColumn>()
        .map(|c| c.name().to_string())
}

fn projection_defines_computed_column(map: &MapFunction, column_name: &str) -> bool {
    let MapFunction::Projection(proj) = map else {
        return false;
    };
    for (idx, field) in proj.out_schema().fields().iter().enumerate() {
        if field.name() != column_name {
            continue;
        }
        let Some(expr) = proj.exprs().get(idx) else {
            return false;
        };
        return !matches!(unwrap_alias(expr), Expr::Column(_));
    }
    false
}

fn find_assign_node(
    graph: &LogicalGraph,
    site: &AssignSite,
    window_idxs: &[NodeIndex],
) -> Result<NodeIndex> {
    let keybys = window_idxs
        .iter()
        .map(|&w| window_keyby(graph, w))
        .collect::<Result<Vec<_>>>()?;

    let mut candidates = Vec::new();
    for idx in graph.get_all_node_indices() {
        if !keybys.iter().all(|&kb| reaches(graph, idx, kb)) {
            continue;
        }
        match (site, &graph.get_node_by_index(idx).operator_config) {
            (AssignSite::Source { .. }, OperatorConfig::SourceConfig(_)) => {
                candidates.push(idx);
            }
            (AssignSite::Projection { column_name }, OperatorConfig::MapConfig(map))
                if projection_defines_computed_column(map, column_name) =>
            {
                candidates.push(idx);
            }
            _ => {}
        }
    }

    match candidates.as_slice() {
        [only] => Ok(*only),
        [] => Err(DataFusionError::Plan(format!(
            "no logical node found for watermark assign site {site:?}"
        ))),
        _ => Err(DataFusionError::Plan(format!(
            "ambiguous logical nodes for watermark assign site {site:?}: {} candidates",
            candidates.len()
        ))),
    }
}

fn attach_assign(graph: &mut LogicalGraph, assign_idx: NodeIndex, column_name: String) -> Result<()> {
    let cfg = graph.watermark_assign_config_for_column(column_name);
    let node = graph
        .get_node_by_index_mut(assign_idx)
        .expect("assign node index must exist");
    match &node.watermark_assign {
        Some(existing) => {
            if existing.time_hint != cfg.time_hint {
                return Err(DataFusionError::Plan(format!(
                    "conflicting watermark assign time hints on {}: {:?} vs {:?}",
                    node.operator_id, existing.time_hint, cfg.time_hint
                )));
            }
        }
        None => {
            node.watermark_assign = Some(cfg);
        }
    }
    Ok(())
}

/// Attach auto watermark assign configs on the defining Source/Projection nodes for each
/// window in `plan`. Call after the logical graph has been built from that plan.
pub fn apply_auto_watermark_assigns(plan: &LogicalPlan, graph: &mut LogicalGraph) -> Result<()> {
    if !graph.watermarks_enabled() {
        return Ok(());
    }

    let mut jobs: Vec<(AssignSite, String)> = Vec::new();
    plan.apply(|node| {
        if let LogicalPlan::Window(window) = node {
            let site = resolve_event_time_assign_site(window)?;
            let et = event_time_column_from_window(window)?;
            jobs.push((site, et.name));
        }
        Ok(TreeNodeRecursion::Continue)
    })?;

    for (site, window_et_name) in jobs {
        let window_idxs: Vec<NodeIndex> = graph
            .get_all_node_indices()
            .into_iter()
            .filter(|&idx| {
                window_order_by_column_name(graph, idx).as_deref() == Some(window_et_name.as_str())
            })
            .collect();
        if window_idxs.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "no window operator with ORDER BY column '{window_et_name}' for watermark placement"
            )));
        }

        let assign_idx = find_assign_node(graph, &site, &window_idxs)?;
        for &window_idx in &window_idxs {
            validate_assign_before_window_keyby(graph, assign_idx, window_idx)?;
        }
        attach_assign(graph, assign_idx, site.column_name().to_string())?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::logical_graph::LogicalNode;
    use crate::api::planner::{Planner, PlanningContext};
    use crate::runtime::operators::operator::OperatorConfig;
    use crate::runtime::operators::source::source_operator::{SourceConfig, VectorSourceConfig};
    use crate::runtime::watermark::TimeHint;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::catalog::MemTable;
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

    fn planner_with_events() -> Planner {
        let mut planner = Planner::new(PlanningContext::new(SessionContext::new()));
        planner.register_source(
            "events".to_string(),
            SourceConfig::VectorSourceConfig(VectorSourceConfig::new(vec![])),
            events_schema(),
        );
        planner
    }

    fn assign_nodes(graph: &LogicalGraph) -> Vec<&LogicalNode> {
        graph
            .get_nodes()
            .filter(|n| n.watermark_assign.is_some())
            .collect()
    }

    fn assert_time_hint_column(node: &LogicalNode, expected: &str) {
        let hint = &node
            .watermark_assign
            .as_ref()
            .expect("expected watermark_assign")
            .time_hint;
        assert!(
            matches!(hint, TimeHint::ColumnName { name } if name == expected),
            "expected ColumnName({expected}), got {hint:?} on {}",
            node.operator_id
        );
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
        assert_eq!(
            site,
            AssignSite::Source {
                table_name: "events".to_string(),
                column_name: "timestamp".to_string(),
            }
        );
    }

    #[tokio::test]
    async fn lineage_filter_on_path_still_stops_at_source() {
        let plan = plan_sql(
            "SELECT timestamp, key, value, \
             SUM(value) OVER (PARTITION BY key ORDER BY timestamp \
             RANGE BETWEEN INTERVAL '1000' MILLISECOND PRECEDING AND CURRENT ROW) as sum_value \
             FROM events WHERE value > 0",
        )
        .await;
        let window = find_window(&plan);
        let site = resolve_event_time_assign_site(&window).unwrap();
        assert_eq!(
            site,
            AssignSite::Source {
                table_name: "events".to_string(),
                column_name: "timestamp".to_string(),
            }
        );
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
        assert_eq!(
            site,
            AssignSite::Source {
                table_name: "events".to_string(),
                column_name: "timestamp".to_string(),
            }
        );
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
        assert_eq!(
            site,
            AssignSite::Projection {
                column_name: "event_time".to_string(),
            }
        );
    }

    #[tokio::test]
    async fn lineage_rename_above_computed_stops_on_defining_projection() {
        // Use the unoptimized plan so nested projs are not folded away: peel
        // `event_time` → `et`, then stop on the computed Projection output `et`.
        let ctx = SessionContext::new();
        let table = MemTable::try_new(events_schema(), vec![vec![]]).unwrap();
        ctx.register_table("events", Arc::new(table)).unwrap();
        let sql = "SELECT event_time, key, value, \
             SUM(value) OVER (PARTITION BY key ORDER BY event_time \
             RANGE BETWEEN INTERVAL '1000' MILLISECOND PRECEDING AND CURRENT ROW) as sum_value \
             FROM ( \
               SELECT et AS event_time, key, value FROM ( \
                 SELECT timestamp + INTERVAL '0' MILLISECOND AS et, key, value FROM events \
               ) \
             )";
        let df = ctx.sql(sql).await.unwrap();
        let plan = df.logical_plan().clone();
        let window = find_window(&plan);
        let site = resolve_event_time_assign_site(&window).unwrap();
        assert_eq!(
            site,
            AssignSite::Projection {
                column_name: "et".to_string(),
            }
        );
    }

    #[tokio::test]
    async fn lineage_order_by_non_column_errors() {
        let plan = plan_sql(
            "SELECT timestamp, key, value, \
             SUM(value) OVER (PARTITION BY key ORDER BY timestamp + INTERVAL '0' MILLISECOND \
             RANGE BETWEEN INTERVAL '1000' MILLISECOND PRECEDING AND CURRENT ROW) as sum_value \
             FROM events",
        )
        .await;
        let window = find_window(&plan);
        let err = resolve_event_time_assign_site(&window).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("window ORDER BY must be a column"),
            "unexpected error: {msg}"
        );
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

    #[tokio::test]
    async fn apply_passthrough_attaches_on_source_not_keyby_or_window() {
        let mut planner = planner_with_events();
        let graph = planner
            .sql_to_graph(
                "SELECT timestamp, key, value, \
                 SUM(value) OVER (PARTITION BY key ORDER BY timestamp \
                 RANGE BETWEEN INTERVAL '1000' MILLISECOND PRECEDING AND CURRENT ROW) as sum_value \
                 FROM events",
            )
            .unwrap();

        let assigns = assign_nodes(&graph);
        assert_eq!(assigns.len(), 1, "expected exactly one assign site");
        assert!(
            matches!(assigns[0].operator_config, OperatorConfig::SourceConfig(_)),
            "expected Source assign, got {:?}",
            assigns[0].operator_config
        );
        assert_time_hint_column(assigns[0], "timestamp");
        assert_eq!(
            assigns[0]
                .watermark_assign
                .as_ref()
                .unwrap()
                .emit_interval,
            crate::runtime::watermark::WatermarkAssignConfig::DEFAULT_EMIT_INTERVAL,
            "default emit interval"
        );

        for node in graph.get_nodes() {
            if matches!(
                node.operator_config,
                OperatorConfig::KeyByConfig(_) | OperatorConfig::WindowConfig(_)
            ) {
                assert!(
                    node.watermark_assign.is_none(),
                    "KeyBy/Window must not have watermark_assign: {}",
                    node.operator_id
                );
            }
        }
    }

    #[tokio::test]
    async fn apply_computed_attaches_on_projection_map_not_source() {
        let mut planner = planner_with_events();
        let graph = planner
            .sql_to_graph(
                "SELECT event_time, key, value, \
                 SUM(value) OVER (PARTITION BY key ORDER BY event_time \
                 RANGE BETWEEN INTERVAL '1000' MILLISECOND PRECEDING AND CURRENT ROW) as sum_value \
                 FROM (SELECT timestamp + INTERVAL '0' MILLISECOND AS event_time, key, value FROM events)",
            )
            .unwrap();

        let assigns = assign_nodes(&graph);
        assert_eq!(assigns.len(), 1, "expected exactly one assign site");
        assert!(
            matches!(
                assigns[0].operator_config,
                OperatorConfig::MapConfig(MapFunction::Projection(_))
            ),
            "expected Projection Map assign, got {:?}",
            assigns[0].operator_config
        );
        assert_time_hint_column(assigns[0], "event_time");

        for node in graph.get_nodes() {
            if matches!(node.operator_config, OperatorConfig::SourceConfig(_)) {
                assert!(
                    node.watermark_assign.is_none(),
                    "Source must not have assign when event-time is computed downstream"
                );
            }
        }
    }

    #[tokio::test]
    async fn apply_two_windows_same_event_time_share_one_assign() {
        let mut planner = planner_with_events();
        let graph = planner
            .sql_to_graph(
                "SELECT timestamp, key, value, \
                 SUM(value) OVER (PARTITION BY key ORDER BY timestamp \
                 RANGE BETWEEN INTERVAL '1000' MILLISECOND PRECEDING AND CURRENT ROW) as sum_value, \
                 AVG(value) OVER (PARTITION BY key ORDER BY timestamp \
                 RANGE BETWEEN INTERVAL '1000' MILLISECOND PRECEDING AND CURRENT ROW) as avg_value \
                 FROM events",
            )
            .unwrap();

        let assigns = assign_nodes(&graph);
        assert_eq!(
            assigns.len(),
            1,
            "same ORDER BY column should attach once, got {} assign nodes",
            assigns.len()
        );
        assert!(matches!(
            assigns[0].operator_config,
            OperatorConfig::SourceConfig(_)
        ));
        assert_time_hint_column(assigns[0], "timestamp");
    }

    #[tokio::test]
    async fn apply_noop_when_watermarks_disabled() {
        let plan = plan_sql(
            "SELECT timestamp, key, value, \
             SUM(value) OVER (PARTITION BY key ORDER BY timestamp \
             RANGE BETWEEN INTERVAL '1000' MILLISECOND PRECEDING AND CURRENT ROW) as sum_value \
             FROM events",
        )
        .await;
        let mut graph = LogicalGraph::new();
        graph.set_watermarks_enabled(false);
        apply_auto_watermark_assigns(&plan, &mut graph).unwrap();
        assert!(
            assign_nodes(&graph).is_empty(),
            "disabled watermarks must not attach assigns"
        );
    }
}
