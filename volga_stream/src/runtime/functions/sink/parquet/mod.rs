use std::collections::HashMap;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use arrow::array::Array;
use arrow::compute::{partition as partition_array, take};
use arrow::record_batch::RecordBatch;
use arrow::datatypes::SchemaRef;
use bytes::Bytes;
use datafusion::common::ScalarValue;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression as ParquetCompression, GzipLevel, ZstdLevel};
use parquet::file::properties::WriterProperties;
use uuid::Uuid;

use crate::common::Message;
use crate::runtime::functions::function_trait::FunctionTrait;
use crate::runtime::runtime_context::RuntimeContext;
use crate::runtime::operators::sink::sink_operator::SinkConfig;
use crate::runtime::functions::sink::sink_function::SinkFunctionTrait;

#[cfg(test)]
mod unit_tests;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ParquetSinkSpec {
    pub path: String,
    #[serde(default)]
    pub storage_options: HashMap<String, String>,
    pub compression: Option<String>,
    pub row_group_size_bytes: Option<usize>,
    pub target_file_size: Option<usize>,
    pub max_buffer_bytes: Option<usize>,
    pub partition_fields: Option<Vec<String>>,
}

#[derive(Debug, Clone)]
pub struct ParquetSinkConfig {
    pub spec: ParquetSinkSpec,
}

impl ParquetSinkSpec {
    pub fn to_config(&self) -> ParquetSinkConfig {
        ParquetSinkConfig { spec: self.clone() }
    }
}

#[derive(Debug)]
pub struct ParquetSinkFunction {
    config: ParquetSinkConfig,
    store: Option<Arc<dyn ObjectStore>>,
    base_prefix: ObjectPath,
    task_index: Option<i32>,
    writers: HashMap<String, ParquetWriterState>,
}

#[derive(Debug)]
struct ParquetWriterState {
    writer: ArrowWriter<Cursor<Vec<u8>>>,
    part: u64,
    schema: SchemaRef,
}

impl ParquetSinkFunction {
    pub fn new(config: ParquetSinkConfig) -> Self {
        Self {
            config,
            store: None,
            base_prefix: ObjectPath::from(""),
            task_index: None,
            writers: HashMap::new(),
        }
    }

    fn writer_properties(&self) -> WriterProperties {
        let mut builder = WriterProperties::builder();
        if let Some(compression) = &self.config.spec.compression {
            let compression = match compression.as_str() {
                "snappy" => ParquetCompression::SNAPPY,
                "gzip" => ParquetCompression::GZIP(GzipLevel::default()),
                "zstd" => ParquetCompression::ZSTD(ZstdLevel::default()),
                "lz4" => ParquetCompression::LZ4,
                "none" => ParquetCompression::UNCOMPRESSED,
                other => panic!("Unsupported parquet compression '{}'", other),
            };
            builder = builder.set_compression(compression);
        }
        if let Some(row_group_size) = self.config.spec.row_group_size_bytes {
            builder = builder.set_max_row_group_size(row_group_size);
        }
        builder.build()
    }

    fn next_part_name(&self, partition_path: &str, part: u64) -> ObjectPath {
        let task_index = self.task_index.unwrap_or_default();
        let file = format!("part-{}-{}-{}.parquet", task_index, Uuid::new_v4(), part);
        let full = if partition_path.is_empty() {
            self.base_prefix.child(file)
        } else {
            self.base_prefix.child(format!("{}/{}", partition_path, file))
        };
        full
    }

    async fn write_batch_to_partition(
        &mut self,
        partition_path: &str,
        batch: RecordBatch,
    ) -> Result<()> {
        let writer_state = self.writers.entry(partition_path.to_string()).or_insert_with(|| {
            let cursor = Cursor::new(Vec::new());
            let writer = ArrowWriter::try_new(
                cursor,
                batch.schema(),
                Some(self.writer_properties()),
            )
            .unwrap();
            ParquetWriterState { writer, part: 0, schema: batch.schema() }
        });

        writer_state.writer.write(&batch)?;

        if let Some(target_size) = self.config.spec.target_file_size {
            let current_size = writer_state.writer.inner().get_ref().len();
            if current_size >= target_size {
                self.flush_writer(partition_path).await?;
            }
        }
        self.enforce_buffer_limit().await?;
        Ok(())
    }

    async fn flush_writer(&mut self, partition_path: &str) -> Result<()> {
        let Some(store) = &self.store else {
            return Err(anyhow!("sink not initialized"));
        };
        let state = self
            .writers
            .get_mut(partition_path)
            .ok_or_else(|| anyhow!("missing writer state"))?;
        state.writer.finish()?;
        let cursor = std::mem::replace(&mut state.writer, ArrowWriter::try_new(
            Cursor::new(Vec::new()),
            state.schema.clone(),
            Some(self.writer_properties()),
        )?);
        let data = cursor.into_inner()?.into_inner();
        let obj_path = self.next_part_name(partition_path, state.part);
        state.part += 1;
        store.put(&obj_path, object_store::PutPayload::from_bytes(Bytes::from(data))).await?;
        Ok(())
    }

    async fn flush_all(&mut self) -> Result<()> {
        let keys: Vec<String> = self.writers.keys().cloned().collect();
        for key in keys {
            self.flush_writer(&key).await?;
        }
        Ok(())
    }

    async fn enforce_buffer_limit(&mut self) -> Result<()> {
        let Some(max_buffer_bytes) = self.config.spec.max_buffer_bytes else {
            return Ok(());
        };
        while self.total_buffered_bytes() > max_buffer_bytes {
            let Some(partition) = self.largest_buffer_partition() else {
                break;
            };
            self.flush_writer(&partition).await?;
        }
        Ok(())
    }

    fn total_buffered_bytes(&self) -> usize {
        self.writers
            .values()
            .map(|writer| writer.writer.inner().get_ref().len())
            .sum()
    }

    fn largest_buffer_partition(&self) -> Option<String> {
        self.writers
            .iter()
            .max_by_key(|(_, writer)| writer.writer.inner().get_ref().len())
            .map(|(key, _)| key.clone())
    }

    fn partition_batches(&self, batch: RecordBatch) -> Result<Vec<(RecordBatch, String)>> {
        let Some(fields) = &self.config.spec.partition_fields else {
            return Ok(vec![(batch, "".to_string())]);
        };
        let mut builder = arrow::array::StringBuilder::with_capacity(batch.num_rows(), batch.num_rows() * 32);
        for row in 0..batch.num_rows() {
            let mut parts = Vec::with_capacity(fields.len());
            for field in fields {
                let idx = batch
                    .schema()
                    .index_of(field)
                    .map_err(|_| anyhow!("partition field '{}' not found in schema", field))?;
                let array = batch.column(idx);
                let value = ScalarValue::try_from_array(array, row)?;
                let value_str = value.to_string();
                parts.push(format!("{}={}", field, value_str));
            }
            builder.append_value(parts.join("/"));
        }
        let partition_values = Arc::new(builder.finish()) as Arc<dyn Array>;
        let sorted_indices = arrow::compute::sort_to_indices(&partition_values, None, None)?;
        let sorted_partition = take(&partition_values, &sorted_indices, None).unwrap();
        let sorted_batch = RecordBatch::try_new(
            batch.schema(),
            batch
                .columns()
                .iter()
                .map(|col| take(col, &sorted_indices, None).unwrap())
                .collect(),
        )?;
        let ranges = partition_array(&[sorted_partition.clone()])?;
        let typed_partition = sorted_partition
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .ok_or_else(|| anyhow!("partition array not string"))?;

        let mut out = Vec::with_capacity(ranges.len());
        for range in ranges.ranges() {
            let partition_string = typed_partition.value(range.start);
            let slice = sorted_batch.slice(range.start, range.end - range.start);
            out.push((slice, partition_string.to_string()));
        }
        Ok(out)
    }
}

#[async_trait]
impl SinkFunctionTrait for ParquetSinkFunction {
    async fn sink(&mut self, message: Message) -> Result<()> {
        let batch = message.record_batch().clone();
        for (partition_batch, partition_path) in self.partition_batches(batch)? {
            self.write_batch_to_partition(&partition_path, partition_batch).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl FunctionTrait for ParquetSinkFunction {
    async fn open(&mut self, ctx: &RuntimeContext) -> Result<()> {
        let (store, prefix) = build_object_store(&self.config.spec.path, &self.config.spec.storage_options)?;
        self.store = Some(store);
        self.base_prefix = prefix;
        self.task_index = Some(ctx.task_index());
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        self.flush_all().await
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

#[cfg(test)]
impl ParquetSinkFunction {
    fn buffered_bytes_for_test(&self) -> usize {
        self.total_buffered_bytes()
    }
}

fn build_object_store(
    path: &str,
    storage_options: &HashMap<String, String>,
) -> Result<(Arc<dyn ObjectStore>, ObjectPath)> {
    if path.starts_with("s3://") {
        let (bucket, prefix) = split_s3_path(path)?;
        let mut builder = object_store::aws::AmazonS3Builder::new()
            .with_bucket_name(bucket);
        if let Some(region) = storage_options.get("region") {
            builder = builder.with_region(region);
        }
        if let Some(endpoint) = storage_options.get("endpoint_url") {
            builder = builder.with_endpoint(endpoint);
            if endpoint.starts_with("http://") {
                builder = builder.with_allow_http(true);
            }
        }
        if let (Some(key), Some(secret)) = (
            storage_options.get("access_key_id"),
            storage_options.get("secret_access_key"),
        ) {
            builder = builder.with_access_key_id(key).with_secret_access_key(secret);
        }
        let store = builder.build()?;
        let store = Arc::new(store) as Arc<dyn ObjectStore>;
        Ok((store, ObjectPath::from(prefix)))
    } else {
        let local_path = if let Some(stripped) = path.strip_prefix("file://") {
            stripped
        } else {
            path
        };
        let local_path = PathBuf::from(local_path);
        let store = object_store::local::LocalFileSystem::new_with_prefix(&local_path)?;
        let store = Arc::new(store) as Arc<dyn ObjectStore>;
        Ok((store, ObjectPath::from("")))
    }
}

fn split_s3_path(path: &str) -> Result<(String, String)> {
    let stripped = path.trim_start_matches("s3://");
    let mut parts = stripped.splitn(2, '/');
    let bucket = parts.next().ok_or_else(|| anyhow!("missing s3 bucket"))?.to_string();
    let prefix = parts.next().unwrap_or("").to_string();
    Ok((bucket, prefix))
}

impl ParquetSinkConfig {
    pub fn new(spec: ParquetSinkSpec) -> Self {
        Self { spec }
    }
}
