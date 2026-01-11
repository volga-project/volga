use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

use arrow::datatypes::Schema;
use serde::{Deserialize, Serialize};

use crate::api::{LogicalGraph, Planner, PlanningContext};
use crate::runtime::functions::source::datagen_source::{DatagenSourceConfig, FieldGenerator};
use crate::runtime::functions::source::request_source::RequestSourceConfig;
use crate::runtime::operators::sink::sink_operator::SinkConfig;
use crate::runtime::operators::source::source_operator::SourceConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecutionMode {
    Request,
    Streaming,
    Batch,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ExecutionProfile {
    SingleWorkerNoMaster { num_threads_per_task: usize },
    LocalOrchestrated,
    Orchestrated { num_workers_per_operator: usize },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PipelineSpec {
    pub execution_profile: ExecutionProfile,
    pub execution_mode: ExecutionMode,
    pub parallelism: usize,
    pub sources: Vec<TableSourceSpec>,
    pub request_source_sink: Option<RequestSourceSinkSpec>,
    pub sink: Option<SinkSpec>,
    pub sql: Option<String>,
    #[serde(skip)]
    pub logical_graph: Option<LogicalGraph>,
    #[serde(skip)]
    pub inline_sources: HashMap<String, (SourceConfig, Arc<Schema>)>,
    #[serde(skip)]
    pub inline_request_source: Option<SourceConfig>,
    #[serde(skip)]
    pub inline_request_sink: Option<SinkConfig>,
    #[serde(skip)]
    pub inline_sink: Option<SinkConfig>,
}

#[derive(Clone, Debug)]
pub struct PipelineSpecBuilder {
    spec: PipelineSpec,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TableSourceSpec {
    pub table_name: String,
    #[serde(with = "b64_bytes")]
    pub schema_ipc: Vec<u8>,
    pub source: SourceSpec,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SourceSpec {
    Datagen(DatagenSpec),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DatagenSpec {
    pub rate: Option<f32>,
    pub limit: Option<usize>,
    pub run_for_s: Option<f64>,
    pub batch_size: usize,
    pub fields: HashMap<String, FieldGenerator>,
    pub replayable: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RequestSourceSinkSpec {
    pub bind_address: String,
    pub max_pending_requests: usize,
    pub request_timeout_ms: u64,
    #[serde(with = "b64_bytes")]
    pub schema_ipc: Vec<u8>,
    pub sink: Option<SinkSpec>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SinkSpec {
    InMemoryStorageGrpc { server_addr: String },
    Request,
}

impl PipelineSpecBuilder {
    pub fn new() -> Self {
        Self {
            spec: PipelineSpec {
                execution_profile: ExecutionProfile::SingleWorkerNoMaster {
                    num_threads_per_task: 4,
                },
                execution_mode: ExecutionMode::Streaming,
                parallelism: 1,
                sources: Vec::new(),
                request_source_sink: None,
                sink: None,
                sql: None,
                logical_graph: None,
                inline_sources: HashMap::new(),
                inline_request_source: None,
                inline_request_sink: None,
                inline_sink: None,
            },
        }
    }

    pub fn with_parallelism(mut self, parallelism: usize) -> Self {
        self.spec.parallelism = parallelism;
        self
    }

    pub fn with_execution_mode(mut self, execution_mode: ExecutionMode) -> Self {
        self.spec.execution_mode = execution_mode;
        self
    }

    pub fn with_execution_profile(mut self, profile: ExecutionProfile) -> Self {
        self.spec.execution_profile = profile;
        self
    }

    pub fn sql(mut self, sql: &str) -> Self {
        self.spec.sql = Some(sql.to_string());
        self.spec.logical_graph = None;
        self
    }

    pub fn with_logical_graph(mut self, logical_graph: LogicalGraph) -> Self {
        self.spec.logical_graph = Some(logical_graph);
        self.spec.sql = None;
        self
    }

    pub fn add_table_source(mut self, src: TableSourceSpec) -> Self {
        self.spec.sources.push(src);
        self
    }

    pub fn with_source(mut self, table_name: String, source_config: SourceConfig, schema: Arc<Schema>) -> Self {
        self.spec.inline_sources.insert(table_name, (source_config, schema));
        self
    }

    pub fn with_request_source_sink_inline(mut self, source_config: SourceConfig, sink_config: Option<SinkConfig>) -> Self {
        self.spec.inline_request_source = Some(source_config);
        self.spec.inline_request_sink = sink_config;
        self
    }

    pub fn with_request_source_sink(mut self, cfg: RequestSourceSinkSpec) -> Self {
        self.spec.request_source_sink = Some(cfg);
        self
    }

    pub fn with_sink(mut self, sink: SinkSpec) -> Self {
        self.spec.sink = Some(sink);
        self
    }

    pub fn with_sink_inline(mut self, sink: SinkConfig) -> Self {
        self.spec.inline_sink = Some(sink);
        self
    }

    pub fn build(self) -> PipelineSpec {
        self.spec
    }
}

impl PipelineSpec {
    pub fn to_logical_graph(&self) -> LogicalGraph {
        if let Some(graph) = &self.logical_graph {
            return graph.clone();
        }

        let sql = self
            .sql
            .as_ref()
            .expect("PipelineSpec has no sql or logical_graph");

        let df_ctx = datafusion::prelude::SessionContext::new();
        let mut planner = Planner::new(
            PlanningContext::new(df_ctx)
                .with_parallelism(self.parallelism)
                .with_execution_mode(self.execution_mode),
        );

        if !self.inline_sources.is_empty() {
            for (table_name, (source_config, schema)) in &self.inline_sources {
                planner.register_source(table_name.clone(), source_config.clone(), schema.clone());
            }
        } else {
            for src in &self.sources {
                let schema = Arc::new(schema_from_ipc(&src.schema_ipc));
                let source_config = match &src.source {
                    SourceSpec::Datagen(cfg) => {
                        let mut c = DatagenSourceConfig::new(
                            schema.clone(),
                            cfg.rate,
                            cfg.limit,
                            cfg.run_for_s,
                            cfg.batch_size,
                            cfg.fields.clone(),
                        );
                        c.set_replayable(cfg.replayable);
                        SourceConfig::DatagenSourceConfig(c)
                    }
                };

                planner.register_source(src.table_name.clone(), source_config, schema);
            }
        }

        if let Some(req_src) = &self.inline_request_source {
            planner.register_request_source_sink(req_src.clone(), self.inline_request_sink.clone());
        } else if let Some(req) = &self.request_source_sink {
            let schema = Arc::new(schema_from_ipc(&req.schema_ipc));
            let request_source = RequestSourceConfig::new(
                req.bind_address.clone(),
                req.max_pending_requests,
                req.request_timeout_ms,
            )
            .set_schema(schema);

            let sink_cfg = req.sink.as_ref().map(|s| s.to_sink_config());
            planner.register_request_source_sink(
                SourceConfig::HttpRequestSourceConfig(request_source),
                sink_cfg,
            );
        }

        if let Some(sink) = &self.inline_sink {
            planner.register_sink(sink.clone());
        } else if let Some(sink) = &self.sink {
            planner.register_sink(sink.to_sink_config());
        }

        planner
            .sql_to_graph(sql)
            .expect("failed to create logical graph from PipelineSpec")
    }
}

impl SinkSpec {
    pub fn to_sink_config(&self) -> SinkConfig {
        match self {
            SinkSpec::InMemoryStorageGrpc { server_addr } => {
                SinkConfig::InMemoryStorageGrpcSinkConfig(server_addr.clone())
            }
            SinkSpec::Request => SinkConfig::RequestSinkConfig,
        }
    }
}

pub fn schema_to_ipc(schema: &Schema) -> Vec<u8> {
    let mut buf = Vec::new();
    let cursor = Cursor::new(&mut buf);
    let mut writer = arrow::ipc::writer::StreamWriter::try_new(cursor, schema).unwrap();
    writer.finish().unwrap();
    buf
}

pub fn schema_from_ipc(bytes: &[u8]) -> Schema {
    let cursor = Cursor::new(bytes);
    let reader = arrow::ipc::reader::StreamReader::try_new(cursor, None).unwrap();
    reader.schema().as_ref().clone()
}

mod b64_bytes {
    use base64::{engine::general_purpose, Engine as _};
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(bytes: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = general_purpose::STANDARD.encode(bytes);
        serializer.serialize_str(&s)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        general_purpose::STANDARD
            .decode(s)
            .map_err(serde::de::Error::custom)
    }
}

