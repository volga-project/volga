use std::sync::Arc;

use crate::api::{LogicalGraph, Planner, PlanningContext};
use crate::api::spec::connectors::{SourceSpec, schema_from_ipc};
use crate::api::spec::pipeline::PipelineSpec;
use crate::api::spec::operators::OperatorTuningSpec;
use crate::runtime::functions::source::datagen_source::DatagenSourceConfig;
use crate::runtime::functions::source::kafka::KafkaSourceConfig;
use crate::runtime::functions::source::parquet::ParquetSourceConfig;
use crate::runtime::functions::source::request_source::RequestSourceConfig;
use crate::runtime::operators::operator::OperatorConfig;
use crate::runtime::operators::source::source_operator::SourceConfig;

pub fn compile_logical_graph(spec: &PipelineSpec) -> LogicalGraph {
    if let Some(graph) = &spec.logical_graph {
        return graph.clone();
    }

    let sql = spec
        .sql
        .as_ref()
        .expect("PipelineSpec has no sql or logical_graph");

    let df_ctx = datafusion::prelude::SessionContext::new();
    let mut planner = Planner::new(
        PlanningContext::new(df_ctx)
            .with_parallelism(spec.parallelism)
            .with_execution_mode(spec.execution_mode),
    );

    if !spec.inline_sources.is_empty() {
        for (table_name, (source_config, schema)) in &spec.inline_sources {
            planner.register_source(table_name.clone(), source_config.clone(), schema.clone());
        }
    } else {
        for src in &spec.sources {
            let schema = Arc::new(schema_from_ipc(&src.schema_ipc));
            let source_config = match &src.source {
                SourceSpec::Datagen(cfg) => {
                    SourceConfig::DatagenSourceConfig(DatagenSourceConfig::new(schema.clone(), cfg.clone()))
                }
                SourceSpec::Kafka(cfg) => {
                    SourceConfig::KafkaSourceConfig(KafkaSourceConfig::new(schema.clone(), cfg.clone()))
                }
                SourceSpec::Parquet(cfg) => {
                    SourceConfig::ParquetSourceConfig(ParquetSourceConfig::new(schema.clone(), cfg.clone()))
                }
            };

            planner.register_source(src.table_name.clone(), source_config, schema);
        }
    }

    if let Some(req_src) = &spec.inline_request_source {
        planner.register_request_source_sink(req_src.clone(), spec.inline_request_sink.clone());
    } else if let Some(req) = &spec.request_source_sink {
        let schema = Arc::new(schema_from_ipc(&req.schema_ipc));
        let request_source = RequestSourceConfig::new(req.clone()).set_schema(schema);

        let sink_cfg = req.sink.as_ref().map(|s| s.to_sink_config());
        planner.register_request_source_sink(SourceConfig::HttpRequestSourceConfig(request_source), sink_cfg);
    }

    if let Some(sink) = &spec.inline_sink {
        planner.register_sink(sink.clone());
    } else if let Some(sink) = &spec.sink {
        planner.register_sink(sink.to_sink_config());
    }

    let mut graph = planner
        .sql_to_graph(sql)
        .expect("failed to create logical graph from PipelineSpec");

    // Apply operator overrides after operator_ids are assigned.
    let node_indices = graph.get_all_node_indices();
    for idx in node_indices {
        let operator_id = graph.get_node_by_index(idx).operator_id.clone();
        let override_spec = spec.operator_overrides.per_operator.get(&operator_id);

        if let Some(node) = graph.get_node_by_index_mut(idx) {
            if let OperatorConfig::WindowConfig(ref mut cfg) = node.operator_config {
                if let Some(OperatorTuningSpec::Window(win)) = &spec.operator_overrides.defaults.tuning {
                    cfg.set_spec(win.clone());
                }
                if let Some(o) = override_spec {
                    if let Some(OperatorTuningSpec::Window(win)) = &o.tuning {
                        cfg.set_spec(win.clone());
                    }
                }
            }
        }
    }

    graph
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};

    use crate::cluster::cluster_provider::ClusterNode;
    use crate::cluster::node_assignment::ExecutionVertexNodeMapping;
    use super::compile_logical_graph;
    use crate::api::spec::pipeline::{PipelineSpecBuilder, ExecutionProfile};
    use crate::runtime::operators::source::source_operator::{SourceConfig, VectorSourceConfig};
    use crate::transport::channel::Channel;

    fn build_mock_vertex_mapping(
        graph: &crate::runtime::execution_graph::ExecutionGraph,
    ) -> ExecutionVertexNodeMapping {
        let node_a = ClusterNode::new("node-a".to_string(), "127.0.0.1".to_string(), 10001);
        let node_b = ClusterNode::new("node-b".to_string(), "127.0.0.1".to_string(), 10002);
        let mut mapping: ExecutionVertexNodeMapping = HashMap::new();

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

        let spec = PipelineSpecBuilder::new()
            .with_execution_profile(ExecutionProfile::SingleWorker {
                num_threads_per_task: 4,
            })
            .with_parallelism(2)
            .with_source(
                "events".to_string(),
                SourceConfig::VectorSourceConfig(VectorSourceConfig::new(vec![])),
                events_schema,
            )
            .with_source(
                "users".to_string(),
                SourceConfig::VectorSourceConfig(VectorSourceConfig::new(vec![])),
                users_schema,
            )
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

        let mut graph1 = compile_logical_graph(&spec).to_execution_graph();
        let mut graph2 = compile_logical_graph(&spec).to_execution_graph();

        let mapping1 = build_mock_vertex_mapping(&graph1);
        let mapping2 = build_mock_vertex_mapping(&graph2);

        graph1.update_channels_with_node_mapping(Some(&mapping1));
        graph2.update_channels_with_node_mapping(Some(&mapping2));

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

