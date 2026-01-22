use std::collections::HashMap;
use std::io::Cursor;
use std::time::Duration;

use anyhow::Result;
use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message as KafkaMessage, Offset, TopicPartitionList};
use serde::{Deserialize, Serialize};
use tokio::time::{sleep, timeout};

use crate::common::message::Message;
use crate::runtime::functions::function_trait::FunctionTrait;
use crate::runtime::functions::source::source_function::SourceFunctionTrait;
use crate::runtime::runtime_context::RuntimeContext;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KafkaSourceSpec {
    pub bootstrap_servers: String,
    pub topic: String,
    pub group_id: Option<String>,
    pub group_id_prefix: Option<String>,
    pub offset: KafkaOffsetSpec,
    #[serde(default)]
    pub client_configs: HashMap<String, String>,
    pub poll_timeout_ms: u64,
    pub max_batch_records: usize,
    pub max_batch_bytes: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum KafkaOffsetSpec {
    Earliest,
    Latest,
    Group,
}

#[derive(Debug, Clone)]
pub struct KafkaSourceConfig {
    pub schema: SchemaRef,
    pub spec: KafkaSourceSpec,
    pub projection: Option<Vec<usize>>,
    pub projected_schema: Option<SchemaRef>,
}

impl KafkaSourceConfig {
    pub fn new(schema: SchemaRef, spec: KafkaSourceSpec) -> Self {
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

pub struct KafkaSourceFunction {
    config: KafkaSourceConfig,
    consumer: Option<StreamConsumer>,
    offsets: HashMap<i32, i64>,
    assigned_partitions: Vec<i32>,
    pending_restore: Option<HashMap<i32, i64>>,
}

impl std::fmt::Debug for KafkaSourceFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaSourceFunction")
            .field("config", &self.config)
            .field("offsets", &self.offsets)
            .field("assigned_partitions", &self.assigned_partitions)
            .field("pending_restore", &self.pending_restore)
            .finish()
    }
}

impl KafkaSourceFunction {
    pub fn new(config: KafkaSourceConfig) -> Self {
        Self {
            config,
            consumer: None,
            offsets: HashMap::new(),
            assigned_partitions: Vec::new(),
            pending_restore: None,
        }
    }

    fn group_id(&self, ctx: &RuntimeContext) -> String {
        if let Some(group_id) = &self.config.spec.group_id {
            return group_id.clone();
        }
        if let Some(prefix) = &self.config.spec.group_id_prefix {
            return format!("{}-volga-{}-{}", prefix, ctx.vertex_id(), ctx.task_index());
        }
        format!("volga-{}-{}", ctx.vertex_id(), ctx.task_index())
    }

    fn offset_from_spec(&self) -> Offset {
        match self.config.spec.offset {
            KafkaOffsetSpec::Earliest => Offset::Beginning,
            KafkaOffsetSpec::Latest => Offset::End,
            KafkaOffsetSpec::Group => Offset::Stored,
        }
    }

    fn auto_offset_reset(&self) -> &'static str {
        match self.config.spec.offset {
            KafkaOffsetSpec::Earliest => "earliest",
            KafkaOffsetSpec::Latest => "latest",
            KafkaOffsetSpec::Group => "latest",
        }
    }

    fn build_consumer(&self, ctx: &RuntimeContext) -> Result<StreamConsumer> {
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", &self.config.spec.bootstrap_servers)
            .set("enable.partition.eof", "false")
            .set("enable.auto.commit", "false")
            .set("group.id", self.group_id(ctx))
            .set("auto.offset.reset", self.auto_offset_reset());

        for (k, v) in &self.config.spec.client_configs {
            config.set(k, v);
        }

        Ok(config.create()?)
    }

    fn decode_record_batch(&self, payload: &[u8]) -> Result<RecordBatch> {
        let mut reader = StreamReader::try_new(Cursor::new(payload), None)?;
        let mut batches = Vec::new();
        while let Some(batch) = reader.next() {
            batches.push(batch?);
        }
        if batches.is_empty() {
            return Ok(RecordBatch::new_empty(self.config.schema.clone()));
        }
        if reader.schema() != self.config.schema {
            panic!(
                "Kafka source schema mismatch: payload schema {:?} != expected {:?}",
                reader.schema(),
                self.config.schema
            );
        }
        if batches.len() == 1 {
            Ok(batches.remove(0))
        } else {
            Ok(concat_batches(&self.config.schema, &batches)?)
        }
    }

    fn assign_partitions(&mut self, ctx: &RuntimeContext) -> Result<()> {
        let consumer = self.consumer.as_ref().expect("consumer not initialized");
        let metadata = consumer.fetch_metadata(Some(&self.config.spec.topic), Duration::from_secs(30))?;
        let partitions = metadata.topics()[0].partitions();

        let mut map = HashMap::new();
        let assigned = Self::compute_assigned_partitions(
            partitions.len(),
            ctx.task_index(),
            ctx.parallelism(),
        );
        for p in partitions.iter() {
            if !assigned.contains(&p.id()) {
                continue;
            }
            map.insert((self.config.spec.topic.clone(), p.id()), self.offset_from_spec());
        }
        self.assigned_partitions = assigned;

        let tpl = TopicPartitionList::from_topic_map(&map)?;
        consumer.assign(&tpl)?;

        Ok(())
    }

    fn compute_assigned_partitions(
        total_partitions: usize,
        task_index: i32,
        parallelism: i32,
    ) -> Vec<i32> {
        (0..total_partitions)
            .filter(|i| i % parallelism as usize == task_index as usize)
            .map(|i| i as i32)
            .collect()
    }

    fn apply_restore_offsets(&mut self, offsets: HashMap<i32, i64>) -> Result<()> {
        let consumer = self.consumer.as_ref().expect("consumer not initialized");
        for (partition, offset) in &offsets {
            consumer.seek(
                &self.config.spec.topic,
                *partition,
                Offset::Offset(*offset),
                Duration::from_secs(1),
            )?;
        }
        self.offsets = offsets;
        Ok(())
    }
}

#[async_trait::async_trait]
impl SourceFunctionTrait for KafkaSourceFunction {
    async fn fetch(&mut self) -> Option<Message> {
        let consumer = self.consumer.as_ref().expect("consumer not initialized");
        let max_records = self.config.spec.max_batch_records.max(1);
        let max_bytes = self.config.spec.max_batch_bytes.max(1);
        let poll_timeout = Duration::from_millis(self.config.spec.poll_timeout_ms.max(1));

        loop {
            if self.assigned_partitions.is_empty() {
                sleep(poll_timeout).await;
                continue;
            }

            let mut batches: Vec<RecordBatch> = Vec::new();
            let mut total_records: usize = 0;
            let mut total_bytes: usize = 0;

            loop {
                if total_records >= max_records || total_bytes >= max_bytes {
                    break;
                }

                let recv = timeout(poll_timeout, consumer.recv()).await;
                let msg = match recv {
                    Ok(Ok(m)) => m,
                    Ok(Err(_)) => continue,
                    Err(_) => {
                        break;
                    }
                };

                if let Some(payload) = msg.payload() {
                    total_bytes += payload.len();
                    match self.decode_record_batch(payload) {
                        Ok(batch) => {
                            if batch.num_rows() == 0 {
                                continue;
                            }
                            total_records += batch.num_rows();
                            batches.push(batch);
                            self.offsets
                                .insert(msg.partition(), msg.offset().saturating_add(1));
                        }
                        Err(_) => continue,
                    }
                }
            }

            if !batches.is_empty() {
                let batch = if batches.len() == 1 {
                    batches.remove(0)
                } else {
                    concat_batches(&self.config.schema, &batches).expect("concat batches")
                };
                return Some(Message::new(None, batch, None, None));
            }
        }
    }

    async fn snapshot_position(&self) -> Result<Vec<u8>> {
        Ok(bincode::serialize(&self.offsets)?)
    }

    async fn restore_position(&mut self, bytes: &[u8]) -> Result<()> {
        if bytes.is_empty() {
            return Ok(());
        }
        let offsets: HashMap<i32, i64> = bincode::deserialize(bytes)?;
        if self.consumer.is_some() {
            self.apply_restore_offsets(offsets)?;
        } else {
            self.pending_restore = Some(offsets);
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl FunctionTrait for KafkaSourceFunction {
    async fn open(&mut self, ctx: &RuntimeContext) -> Result<()> {
        let consumer = self.build_consumer(ctx)?;
        self.consumer = Some(consumer);
        self.assign_partitions(ctx)?;

        if let Some(offsets) = self.pending_restore.take() {
            self.apply_restore_offsets(offsets)?;
        }
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

#[cfg(test)]
mod unit_tests;

#[cfg(test)]
mod integration_tests;
