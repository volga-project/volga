use std::sync::Arc;

use crate::api::{LogicalGraph, Planner, PlanningContext};
use crate::api::spec::connectors::{SourceSpec, schema_from_ipc};
use crate::api::spec::pipeline::PipelineSpec;
use crate::api::spec::operators::OperatorTuningSpec;
use crate::runtime::functions::source::datagen_source::DatagenSourceConfig;
use crate::runtime::functions::source::kafka::KafkaSourceConfig;
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

