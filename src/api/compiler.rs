use std::sync::Arc;

use arrow_integration_test::schema_from_json;
use crate::api::{LogicalGraph, Planner, PlanningContext};
use crate::api::spec::connectors::{SourceSpec, SourceSpecKind};
use crate::api::spec::event_time::EventTimeSpec;
use crate::api::spec::operators::{OperatorOverrides, OperatorTuningSpec};
use crate::api::spec::pipeline::{ConnectorConfigs, ExecutionMode, PipelineSpec};
use crate::runtime::functions::source::datagen_source::DatagenSourceConfig;
use crate::runtime::functions::source::kafka::KafkaSourceConfig;
use crate::runtime::functions::source::parquet::ParquetSourceConfig;
use crate::runtime::functions::source::request_source::RequestSourceConfig;
use crate::runtime::operators::operator::OperatorConfig;
use crate::runtime::operators::source::source_operator::SourceConfig;

fn resolve_source_schema(src: &SourceSpec) -> arrow::datatypes::Schema {
    schema_from_json(&src.schema_json)
        .expect("failed to parse source.schema_json as Arrow integration schema")
}

fn connector_configs_from_spec(spec: &PipelineSpec) -> ConnectorConfigs {
    let mut connector_configs = ConnectorConfigs::default();
    for src in &spec.sources {
        let schema = Arc::new(resolve_source_schema(src));
        let source_config = match &src.source {
            SourceSpecKind::Datagen(cfg) => {
                SourceConfig::DatagenSourceConfig(DatagenSourceConfig::new(schema.clone(), cfg.clone()))
            }
            SourceSpecKind::Kafka(cfg) => {
                SourceConfig::KafkaSourceConfig(KafkaSourceConfig::new(schema.clone(), cfg.clone()))
            }
            SourceSpecKind::Parquet(cfg) => {
                SourceConfig::ParquetSourceConfig(ParquetSourceConfig::new(schema.clone(), cfg.clone()))
            }
        };
        connector_configs
            .sources
            .insert(src.table_name.clone(), (source_config, schema));
    }
    if let Some(req) = &spec.request_source_sink {
        let schema_json = req
            .schema_json
            .as_ref()
            .expect("request_source_sink.schema_json must be set");
        let schema = Arc::new(
            schema_from_json(schema_json)
                .expect("failed to parse request_source_sink.schema_json as Arrow integration schema"),
        );
        let request_source = RequestSourceConfig::new(req.clone()).set_schema(schema);
        connector_configs.request_source = Some(SourceConfig::HttpRequestSourceConfig(request_source));
        connector_configs.request_sink = req.sink.as_ref().map(|s| s.to_sink_config());
    }
    if let Some(sink) = &spec.sink {
        connector_configs.sink = Some(sink.to_sink_config());
    }
    connector_configs
}

fn merge_connector_configs(
    mut base: ConnectorConfigs,
    overrides: &ConnectorConfigs,
) -> ConnectorConfigs {
    for (table_name, source_cfg) in &overrides.sources {
        base.sources.insert(table_name.clone(), source_cfg.clone());
    }
    if let Some(request_source) = &overrides.request_source {
        base.request_source = Some(request_source.clone());
    }
    if let Some(request_sink) = &overrides.request_sink {
        base.request_sink = Some(request_sink.clone());
    }
    if let Some(sink) = &overrides.sink {
        base.sink = Some(sink.clone());
    }
    base
}

fn compile_logical_graph_from_parts(
    sql: &str,
    parallelism: usize,
    execution_mode: ExecutionMode,
    operator_overrides: &OperatorOverrides,
    event_time: &EventTimeSpec,
    connector_configs: &ConnectorConfigs,
) -> LogicalGraph {
    let df_ctx = datafusion::prelude::SessionContext::new();
    let mut planner = Planner::new(
        PlanningContext::new(df_ctx)
            .with_parallelism(parallelism)
            .with_execution_mode(execution_mode),
    );

    for (table_name, (source_config, schema)) in &connector_configs.sources {
        planner.register_source(table_name.clone(), source_config.clone(), schema.clone());
    }

    if let Some(req_src) = &connector_configs.request_source {
        planner.register_request_source_sink(req_src.clone(), connector_configs.request_sink.clone());
    }

    if let Some(sink) = &connector_configs.sink {
        planner.register_sink(sink.clone());
    }

    let mut graph = planner
        .sql_to_graph(sql)
        .expect("failed to create logical graph from PipelineSpec");

    // Apply operator overrides after operator_ids are assigned.
    let node_indices = graph.get_all_node_indices();
    for idx in node_indices {
        let operator_id = graph.get_node_by_index(idx).operator_id.clone();
        let override_spec = operator_overrides.per_operator.get(&operator_id);

        if let Some(node) = graph.get_node_by_index_mut(idx) {
            if let OperatorConfig::WindowConfig(ref mut cfg) = node.operator_config {
                if let Some(OperatorTuningSpec::Window(win)) = &operator_overrides.defaults.tuning {
                    cfg.set_spec(win.clone());
                }
                if let Some(o) = override_spec {
                    if let Some(OperatorTuningSpec::Window(win)) = &o.tuning {
                        cfg.set_spec(win.clone());
                    }
                }
                // Pipeline event_time is the only compile-time lateness source
                // (override WindowOperatorSpec.lateness is ignored).
                cfg.spec.lateness = event_time.window.allowed_lateness_ms;
            }
        }
    }

    graph.set_event_time(event_time.clone());
    graph
}

pub fn compile_logical_graph(spec: &PipelineSpec, connector_overrides: Option<&ConnectorConfigs>) -> LogicalGraph {
    let sql = spec
        .sql
        .as_ref()
        .expect("PipelineSpec has no sql");
    let spec_connector_configs = connector_configs_from_spec(spec);
    let connector_configs_owned = if let Some(overrides) = connector_overrides {
        merge_connector_configs(spec_connector_configs, overrides)
    } else {
        assert!(
            !spec_connector_configs.sources.is_empty()
                || spec_connector_configs.request_source.is_some(),
            "PipelineSpec must contain source/request-source connectors when compile_logical_graph is called without overrides"
        );
        spec_connector_configs
    };
    compile_logical_graph_from_parts(
        sql,
        spec.parallelism,
        spec.execution_mode,
        &spec.operator_overrides,
        &spec.event_time,
        &connector_configs_owned,
    )
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};

    use crate::orchestrator::orchestrator::WorkerNode;
    use crate::orchestrator::task_assignment::TaskWorkerMapping;
    use super::compile_logical_graph;
    use crate::api::spec::pipeline::{ConnectorConfigs, PipelineSpecBuilder, ExecutionProfile};
    use crate::runtime::operators::source::source_operator::{SourceConfig, VectorSourceConfig};
    use crate::transport::channel::Channel;

    fn build_mock_vertex_mapping(
        graph: &crate::runtime::execution_graph::ExecutionGraph,
    ) -> TaskWorkerMapping {
        let node_a = WorkerNode::new("node-a".to_string(), "127.0.0.1".to_string(), 10001, 11001);
        let node_b = WorkerNode::new("node-b".to_string(), "127.0.0.1".to_string(), 10002, 11002);
        let mut mapping: TaskWorkerMapping = HashMap::new();

        for vertex in graph.get_vertices().values() {
            let node = if vertex.task_index % 2 == 0 {
                node_a.clone()
            } else {
                node_b.clone()
            };
            mapping.insert(vertex.vertex_id.as_ref().to_string(), node);
        }

        mapping
    }

    // make sure compiling spec is deterministic
    #[test]
    fn test_compiled_graph_signature_consistency() {
        let events_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let users_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let mut connector_configs = ConnectorConfigs::default();
        connector_configs.sources.insert(
            "events".to_string(),
            (SourceConfig::VectorSourceConfig(VectorSourceConfig::new(vec![])), events_schema),
        );
        connector_configs.sources.insert(
            "users".to_string(),
            (SourceConfig::VectorSourceConfig(VectorSourceConfig::new(vec![])), users_schema),
        );
        let spec = PipelineSpecBuilder::new()
            .with_execution_profile(ExecutionProfile::SingleWorker {
                num_threads_per_task: 4,
            })
            .with_parallelism(2)
            .sql(
                "SELECT \
                    e.id, \
                    e.ts, \
                    SUM(e.value) OVER ( \
                        PARTITION BY e.id \
                        ORDER BY e.ts \
                        RANGE BETWEEN 2 PRECEDING AND CURRENT ROW \
                    ) AS rolling_sum \
                 FROM events e \
                 WHERE e.value > 0.0",
            )
            .build();

        let mut graph1 = compile_logical_graph(&spec, Some(&connector_configs)).to_execution_graph();
        let mut graph2 = compile_logical_graph(&spec, Some(&connector_configs)).to_execution_graph();

        let mapping1 = build_mock_vertex_mapping(&graph1);
        let mapping2 = build_mock_vertex_mapping(&graph2);

        graph1.configure_channels(Some(&mapping1), None);
        graph2.configure_channels(Some(&mapping2), None);

        let has_local = graph1
            .get_edges()
            .values()
            .any(|e| matches!(e.channel, Some(Channel::Local { .. })));
        let has_remote = graph1
            .get_edges()
            .values()
            .any(|e| matches!(e.channel, Some(Channel::Remote { .. })));
        assert!(has_local, "expected at least one local channel");
        assert!(has_remote, "expected at least one remote channel");

        let signature1 = graph1.signature();
        let signature2 = graph2.signature();

        assert!(!signature1.is_empty(), "signature should not be empty");
        assert_eq!(
            signature1, signature2,
            "execution graph signatures should match for the same PipelineSpec"
        );
    }
}

