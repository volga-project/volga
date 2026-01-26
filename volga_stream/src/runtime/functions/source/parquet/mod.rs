use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use arrow::record_batch::RecordBatch;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectMeta};
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::arrow::async_reader::ParquetObjectReader;
use regex::Regex;

use crate::common::message::Message;
use crate::runtime::functions::function_trait::FunctionTrait;
use crate::runtime::functions::parquet_utils::build_object_store;
use crate::runtime::functions::source::source_function::SourceFunctionTrait;
use crate::runtime::runtime_context::RuntimeContext;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ParquetSourceSpec {
    pub path: String,
    #[serde(default)]
    pub storage_options: HashMap<String, String>,
    pub regex_pattern: Option<String>,
    pub batch_size: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct ParquetSourceConfig {
    pub schema: SchemaRef,
    pub spec: ParquetSourceSpec,
    pub projection: Option<Vec<usize>>,
    pub projected_schema: Option<SchemaRef>,
}

impl ParquetSourceConfig {
    pub fn new(schema: SchemaRef, spec: ParquetSourceSpec) -> Self {
        Self {
            schema,
            spec,
            projection: None,
            projected_schema: None,
        }
    }

    pub fn get_projection(&self) -> (Option<Vec<usize>>, Option<SchemaRef>) {
        (self.projection.clone(), self.projected_schema.clone())
    }

    pub fn set_projection(&mut self, projection: Vec<usize>, schema: SchemaRef) {
        self.projection = Some(projection);
        self.projected_schema = Some(schema);
    }
}

pub struct ParquetSourceFunction {
    config: ParquetSourceConfig,
    file_list: Vec<ObjectMeta>,
    current_stream: Mutex<Option<Box<dyn Stream<Item = Result<RecordBatch, parquet::errors::ParquetError>> + Unpin + Send>>>,
}

impl std::fmt::Debug for ParquetSourceFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParquetSourceFunction")
            .field("config", &self.config)
            .field("file_list_len", &self.file_list.len())
            .finish()
    }
}

#[async_trait]
impl SourceFunctionTrait for ParquetSourceFunction {
    async fn fetch(&mut self) -> Option<Message> {
        loop {
            if self.current_stream.lock().unwrap().is_none() {
                let Some(next_file) = self.file_list.pop() else {
                    return None;
                };
                if let Ok(stream) = self.build_stream(&next_file).await {
                    *self.current_stream.lock().unwrap() = Some(stream);
                } else {
                    continue;
                }
            }

            let stream = self.current_stream.lock().unwrap().take();
            if let Some(mut stream) = stream {
                let next = stream.next().await;
                *self.current_stream.lock().unwrap() = Some(stream);
                match next {
                    Some(Ok(batch)) => return Some(Message::new(None, batch, None, None)),
                    Some(Err(_)) => {
                        *self.current_stream.lock().unwrap() = None;
                        continue;
                    }
                    None => {
                        *self.current_stream.lock().unwrap() = None;
                        continue;
                    }
                }
            }
        }
    }
}

#[async_trait]
impl FunctionTrait for ParquetSourceFunction {
    async fn open(&mut self, ctx: &RuntimeContext) -> Result<()> {
        let (store, prefix) = build_object_store(
            &self.config.spec.path,
            &self.config.spec.storage_options,
            true,
        )?;
        let regex = self
            .config
            .spec
            .regex_pattern
            .as_ref()
            .map(|r| Regex::new(r))
            .transpose()?;

        let mut files = list_objects(store.clone(), prefix, regex).await?;
        files.sort_by_key(|m| m.location.clone());
        let parallelism = ctx.parallelism() as usize;
        let task_index = ctx.task_index() as usize;
        files.retain(|m| (hash_path(&m.location) % parallelism) == task_index);
        self.file_list = files;
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl ParquetSourceFunction {
    pub fn new(config: ParquetSourceConfig) -> Self {
        Self {
            config,
            file_list: Vec::new(),
            current_stream: Mutex::new(None),
        }
    }

    async fn build_stream(&self, meta: &ObjectMeta) -> Result<Box<dyn Stream<Item = Result<RecordBatch, parquet::errors::ParquetError>> + Unpin + Send>> {
        let (store, _) = build_object_store(
            &self.config.spec.path,
            &self.config.spec.storage_options,
            true,
        )?;
        let reader = ParquetObjectReader::new(store, meta.location.clone())
            .with_file_size(meta.size);
        let mut builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
        if let Some(batch_size) = self.config.spec.batch_size {
            builder = builder.with_batch_size(batch_size);
        }
        if let Some(projection) = &self.config.projection {
            let file_metadata = builder.metadata().file_metadata();
            let mask = ProjectionMask::roots(file_metadata.schema_descr(), projection.clone());
            builder = builder.with_projection(mask);
        }
        Ok(Box::new(builder.build()?))
    }
}

async fn list_objects(
    store: Arc<dyn ObjectStore>,
    prefix: ObjectPath,
    regex: Option<Regex>,
) -> Result<Vec<ObjectMeta>> {
    let mut out = Vec::new();
    let mut stream = store.list(Some(&prefix));
    while let Some(res) = stream.next().await {
        let meta = res?;
        if let Some(regex) = &regex {
            let path = meta.location.to_string();
            if !regex.is_match(&path) {
                continue;
            }
        }
        out.push(meta);
    }
    Ok(out)
}

fn hash_path(path: &ObjectPath) -> usize {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    path.to_string().hash(&mut hasher);
    hasher.finish() as usize
}

#[cfg(test)]
mod unit_tests;
#[cfg(test)]
mod integration_tests;
