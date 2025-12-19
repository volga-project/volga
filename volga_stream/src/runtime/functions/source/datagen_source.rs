use async_trait::async_trait;
use anyhow::Result;
use crate::common::message::{Message, MAX_WATERMARK_VALUE};
use crate::runtime::runtime_context::RuntimeContext;
use crate::runtime::functions::function_trait::FunctionTrait;
use std::any::Any;
use std::time::{SystemTime, Duration, UNIX_EPOCH};
use super::source_function::SourceFunctionTrait;
use arrow::array::ArrayRef;
use arrow::array::builder::{Float64Builder, Int64Builder, StringBuilder, TimestampMillisecondBuilder};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use std::collections::HashMap;
use rand::{SeedableRng, Rng};
use rand::rngs::StdRng;
use rand::distributions::Alphanumeric;
use datafusion::common::ScalarValue;
use serde::{Serialize, Deserialize};
use crate::runtime::utils;
use serde_with::serde_as;

/// Configuration for datagen source
#[derive(Debug, Clone)]
pub struct DatagenSourceConfig {
    pub schema: Arc<Schema>, // Arrow schema for the generated data
    pub rate: Option<f32>, // Events per second (global rate across all tasks), None = as fast as possible
    pub limit: Option<usize>, // Total number of records to generate (across all tasks)
    pub run_for_s: Option<f64>, // Run for exactly this duration in seconds, limit must be None if set
    pub batch_size: usize, // Number of records per batch
    pub fields: HashMap<String, FieldGenerator>,
    pub projection: Option<Vec<usize>>,
    pub projected_schema: Option<SchemaRef>,
    pub replayable: bool,
}

impl DatagenSourceConfig {
    pub fn new(schema: Arc<Schema>, rate: Option<f32>, limit: Option<usize>, run_for_s: Option<f64>, batch_size: usize, fields: HashMap<String, FieldGenerator>) -> Self {
        Self { schema, rate, limit, run_for_s, batch_size, fields, projection: None, projected_schema: None, replayable: false }
    }

    pub fn get_projection(&self) -> (Option<Vec<usize>>, Option<SchemaRef>) {
        (self.projection.clone(), self.projected_schema.clone())
    }

    pub fn set_projection(&mut self, projection: Vec<usize>, schema: SchemaRef) {
        self.projection = Some(projection);
        self.projected_schema = Some(schema);
    }

    pub fn set_replayable(&mut self, replayable: bool) {
        self.replayable = replayable;
    }
}

/// Data generation strategies
#[derive(Debug, Clone)]
pub enum FieldGenerator {
    IncrementalTimestamp { start_ms: i64, step_ms: i64 },
    ProcessingTimestamp, // Uses current SystemTime
    String { length: usize }, // Simple random string
    Key { num_unique: usize }, // Key field with unique values
    Increment { start: ScalarValue, step: ScalarValue },
    Uniform { min: ScalarValue, max: ScalarValue },
    Values { values: Vec<ScalarValue> }, // Round-robin through provided values
}

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DatagenSourcePosition {
    records_generated: usize,
    max_watermark_sent: bool,
    current_key_index: usize,
    task_records_limit: Option<usize>,
    key_values: Vec<String>,
    per_key_timestamps: HashMap<String, i64>,
    #[serde_as(as = "HashMap<_, HashMap<_, utils::ScalarValueAsBytes>>")]
    per_key_increments: HashMap<String, HashMap<usize, ScalarValue>>,
    per_key_values_indices: HashMap<String, HashMap<usize, usize>>,
}

/// Datagen source function with deterministic rate coordination
#[derive(Debug)]
pub struct DatagenSourceFunction {
    config: DatagenSourceConfig,
    
    // Runtime state
    task_index: Option<i32>,
    parallelism: Option<i32>,
    vertex_id: Option<String>,
    max_watermark_sent: bool,
    
    // Rate coordination state
    next_gen_time: Option<SystemTime>,
    gen_interval: Option<Duration>,
    records_generated: usize,
    task_records_limit: Option<usize>,
    start_time: Option<SystemTime>, // When datagen started (for run_for_s)
    
    // Generation state
    rng: Option<StdRng>,
    schema: Arc<Schema>,
    
    // Key-based state tracking
    key_values: Vec<String>, // All possible key values for this task
    current_key_index: usize, // Round-robin index for key selection
    per_key_timestamps: HashMap<String, i64>, // Timestamp state per key
    per_key_increments: HashMap<String, HashMap<usize, ScalarValue>>, // Increment state per key per field
    per_key_values_indices: HashMap<String, HashMap<usize, usize>>, // Values indices per key per field
}

impl DatagenSourceFunction {
    pub fn new(config: DatagenSourceConfig) -> Self {
        let schema = config.schema.clone();
        Self {
            config,
            task_index: None,
            parallelism: None,
            vertex_id: None,
            max_watermark_sent: false,
            next_gen_time: None,
            gen_interval: None,
            records_generated: 0,
            task_records_limit: None,
            start_time: None,
            rng: None,
            schema,
            key_values: Vec::new(),
            current_key_index: 0,
            per_key_timestamps: HashMap::new(),
            per_key_increments: HashMap::new(),
            per_key_values_indices: HashMap::new(),
        }
    }

    fn validate_replayable_config(&self) {
        if !self.config.replayable {
            return;
        }

        for (field_name, gen) in &self.config.fields {
            let ok = matches!(
                gen,
                FieldGenerator::IncrementalTimestamp { .. }
                    | FieldGenerator::Key { .. }
                    | FieldGenerator::Increment { .. }
                    | FieldGenerator::Values { .. }
            );
            if !ok {
                panic!(
                    "Datagen replayable mode requires deterministic generators only; field '{}' has {:?}",
                    field_name, gen
                );
            }
        }
    }

    /// Calculate deterministic timing for this task
    fn calculate_timing(&self) -> Option<(Duration, SystemTime)> {
        let rate = match self.config.rate {
            Some(rate) => rate,
            None => return None, // No rate limiting
        };
        
        let task_index = self.task_index.expect("Task index not set");
        let parallelism = self.parallelism.expect("Parallelism not set");
        
        // Each task gets: global_rate / parallelism
        let task_rate = rate / parallelism as f32;
        let interval = Duration::from_secs_f32(1.0 / task_rate);
        
        // Stagger task start times to spread load
        let start_offset = Duration::from_secs_f32(task_index as f32 / rate);
        let start_time = SystemTime::now() + start_offset;
        
        Some((interval, start_time))
    }

    fn init_keys(&mut self) {
        let task_index = self.task_index.expect("Task index not set");
        let parallelism = self.parallelism.expect("Parallelism not set");
        
        // Find key field
        let key_field_index = self.schema.fields().iter().position(|field| {
            if let Some(generator) = self.config.fields.get(field.name()) {
                matches!(generator, FieldGenerator::Key { .. })
            } else {
                false
            }
        });

        // TODO assert we have a single key field
        
        // Initialize key values
        if let Some(key_idx) = key_field_index {
            let key_field_name = self.schema.fields()[key_idx].name();
            if let Some(FieldGenerator::Key { num_unique }) = self.config.fields.get(key_field_name) {
                // Distribute unique keys across all tasks
                self.key_values = Self::gen_key_values_for_task(parallelism as usize, task_index as usize, *num_unique);
                
                // Initialize per-key state for distributed keys
                for key in self.key_values.iter() {
                    self.per_key_timestamps.insert(key.clone(), 0);
                    self.per_key_increments.insert(key.clone(), HashMap::new());
                    self.per_key_values_indices.insert(key.clone(), HashMap::new());
                }
            }
        } else {
            // No key field, create a single global key per task
            let global_key = format!("key-{}", task_index);
            self.key_values.push(global_key.clone());
            self.per_key_timestamps.insert(global_key.clone(), 0);
            self.per_key_increments.insert(global_key.clone(), HashMap::new());
            self.per_key_values_indices.insert(global_key, HashMap::new());
        }
    }

    pub fn gen_key_values_for_task(parallelism: usize, task_index: usize, num_unique: usize) -> Vec<String> {
        let mut key_values = Vec::new();
        
        let keys_per_task = num_unique / parallelism as usize;
        let remainder = num_unique % parallelism as usize;
        
        // If there's a remainder, give it to the last task
        let num_keys_for_task = if remainder != 0 && task_index == parallelism - 1 {
            keys_per_task + remainder
        } else {
            keys_per_task
        };
        
        for key_id in 0..num_keys_for_task {
            let key = format!("key-{}-{}", task_index, key_id);
            key_values.push(key.clone());
        }
        key_values
    }

    /// Generate a single record batch
    fn generate_batch(&mut self, batch_size: usize) -> Result<RecordBatch> {
        let schema = self.schema.clone();
        let mut columns: Vec<ArrayRef> = Vec::new();
        
        // Generate records with round-robin key selection
        let mut batch_keys = Vec::with_capacity(batch_size);
        for _ in 0..batch_size {
            let key = self.key_values[self.current_key_index].clone();
            self.current_key_index = (self.current_key_index + 1) % self.key_values.len();
            batch_keys.push(key);
        }
        
        // Iterate over schema fields and get generators by name
        for (field_idx, field) in self.schema.fields().iter().enumerate() {
            let field_name = field.name();
            let generator = self.config.fields.get(field_name)
                .expect(&format!("No generator found for field: {}", field_name));
            
            let column = match generator {
                FieldGenerator::IncrementalTimestamp { start_ms, step_ms } => {
                    Self::generate_incremental_timestamp_column(&mut self.per_key_timestamps, *start_ms, *step_ms, &batch_keys)?
                },
                
                FieldGenerator::ProcessingTimestamp => {
                    Self::generate_processing_timestamp_column_static(batch_size)?
                },
                
                FieldGenerator::String { length } => {
                    let rng = self.rng.as_mut().expect("RNG not initialized");
                    Self::generate_random_string_column_static(*length, batch_size, rng)?
                },
                
                FieldGenerator::Key { .. } => {
                    Self::generate_key_column_static(&batch_keys)?
                },
                
                FieldGenerator::Increment { start, step } => {
                    Self::generate_increment_column_per_key(&mut self.per_key_increments, field_idx, start, step, &batch_keys)?
                },
                
                FieldGenerator::Uniform { min, max } => {
                    let rng = self.rng.as_mut().expect("RNG not initialized");
                    Self::generate_uniform_column_static(min, max, batch_size, rng)?
                },
                
                FieldGenerator::Values { values } => {
                    Self::generate_values_column_per_key(&mut self.per_key_values_indices, field_idx, values, &batch_keys)?
                },
            };
            columns.push(column);
        }
        
        self.records_generated += batch_size;
        RecordBatch::try_new(schema.clone(), columns).map_err(Into::into)
    }

    /// Generate incremental timestamp column with per-key increment
    fn generate_incremental_timestamp_column(per_key_timestamps: &mut HashMap<String, i64>, start_ms: i64, step_ms: i64, batch_keys: &[String]) -> Result<ArrayRef> {
        let mut builder = TimestampMillisecondBuilder::with_capacity(batch_keys.len());
        
        for key in batch_keys {
            let current_ts = per_key_timestamps.get_mut(key).unwrap();
            if *current_ts == 0 {
                *current_ts = start_ms;
            }
            builder.append_value(*current_ts);
            *current_ts += step_ms;
        }
        
        Ok(Arc::new(builder.finish()) as ArrayRef)
    }

    /// Generate processing timestamp column using current system time
    fn generate_processing_timestamp_column_static(batch_size: usize) -> Result<ArrayRef> {
        let mut builder = TimestampMillisecondBuilder::with_capacity(batch_size);
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        
        for _ in 0..batch_size {
            builder.append_value(now_ms);
        }
        
        Ok(Arc::new(builder.finish()) as ArrayRef)
    }

    /// Generate random string column
    fn generate_random_string_column_static(length: usize, batch_size: usize, rng: &mut StdRng) -> Result<ArrayRef> {
        let mut builder = StringBuilder::with_capacity(batch_size, length * batch_size);
        
        for _ in 0..batch_size {
            let s: String = (0..length)
                .map(|_| rng.sample(Alphanumeric) as char)
                .collect();
            builder.append_value(&s);
        }
        
        Ok(Arc::new(builder.finish()) as ArrayRef)
    }

    /// Generate key column
    fn generate_key_column_static(batch_keys: &[String]) -> Result<ArrayRef> {
        let mut builder = StringBuilder::with_capacity(batch_keys.len(), batch_keys.len() * 20);
        
        for key in batch_keys {
            builder.append_value(key);
        }
        
        Ok(Arc::new(builder.finish()) as ArrayRef)
    }

    /// Generate increment column with per-key state
    fn generate_increment_column_per_key(per_key_increments: &mut HashMap<String, HashMap<usize, ScalarValue>>, field_idx: usize, start: &ScalarValue, step: &ScalarValue, batch_keys: &[String]) -> Result<ArrayRef> {
        match (start, step) {
            (ScalarValue::Int64(Some(start_val)), ScalarValue::Int64(Some(step_val))) => {
                let mut builder = Int64Builder::with_capacity(batch_keys.len());
                
                for key in batch_keys {
                    let key_increments = per_key_increments.get_mut(key).unwrap();
                    let current_val = key_increments.entry(field_idx)
                        .or_insert_with(|| ScalarValue::Int64(Some(*start_val)));
                    
                    if let ScalarValue::Int64(Some(val)) = current_val {
                        builder.append_value(*val);
                        *current_val = ScalarValue::Int64(Some(*val + *step_val));
                    }
                }
                
                Ok(Arc::new(builder.finish()) as ArrayRef)
            },
            (ScalarValue::Float64(Some(start_val)), ScalarValue::Float64(Some(step_val))) => {
                let mut builder = Float64Builder::with_capacity(batch_keys.len());
                
                for key in batch_keys {
                    let key_increments = per_key_increments.get_mut(key).unwrap();
                    let current_val = key_increments.entry(field_idx)
                        .or_insert_with(|| ScalarValue::Float64(Some(*start_val)));
                    
                    if let ScalarValue::Float64(Some(val)) = current_val {
                        builder.append_value(*val);
                        *current_val = ScalarValue::Float64(Some(*val + *step_val));
                    }
                }
                
                Ok(Arc::new(builder.finish()) as ArrayRef)
            },
            _ => Err(anyhow::anyhow!("Unsupported increment types: {:?} and {:?}", start, step))
        }
    }

    /// Generate uniform column using ScalarValue
    fn generate_uniform_column_static(min: &ScalarValue, max: &ScalarValue, batch_size: usize, rng: &mut StdRng) -> Result<ArrayRef> {
        match (min, max) {
            (ScalarValue::Int64(Some(min_val)), ScalarValue::Int64(Some(max_val))) => {
                let mut builder = Int64Builder::with_capacity(batch_size);
                for _ in 0..batch_size {
                    let value = rng.gen_range(*min_val..=*max_val);
                    builder.append_value(value);
                }
                Ok(Arc::new(builder.finish()) as ArrayRef)
            },
            (ScalarValue::Float64(Some(min_val)), ScalarValue::Float64(Some(max_val))) => {
                let mut builder = Float64Builder::with_capacity(batch_size);
                for _ in 0..batch_size {
                    let value = rng.gen_range(*min_val..=*max_val);
                    builder.append_value(value);
                }
                Ok(Arc::new(builder.finish()) as ArrayRef)
            },
            _ => Err(anyhow::anyhow!("Unsupported uniform types: {:?} and {:?}", min, max))
        }
    }

    /// Generate values column using round-robin selection from ScalarValue vector per key
    fn generate_values_column_per_key(per_key_values_indices: &mut HashMap<String, HashMap<usize, usize>>, field_idx: usize, values: &[ScalarValue], batch_keys: &[String]) -> Result<ArrayRef> {
        if values.is_empty() {
            return Err(anyhow::anyhow!("Values list cannot be empty"));
        }
        
        // Determine the data type from the first value
        match &values[0] {
            ScalarValue::Int64(_) => {
                let mut builder = Int64Builder::with_capacity(batch_keys.len());
                for key in batch_keys {
                    let key_values_indices = per_key_values_indices.get_mut(key).unwrap();
                    let current_index = key_values_indices.entry(field_idx).or_insert(0);
                    
                    if let ScalarValue::Int64(Some(val)) = &values[*current_index] {
                        builder.append_value(*val);
                    }
                    *current_index = (*current_index + 1) % values.len();
                }
                Ok(Arc::new(builder.finish()) as ArrayRef)
            },
            ScalarValue::Float64(_) => {
                let mut builder = Float64Builder::with_capacity(batch_keys.len());
                for key in batch_keys {
                    let key_values_indices = per_key_values_indices.get_mut(key).unwrap();
                    let current_index = key_values_indices.entry(field_idx).or_insert(0);
                    
                    if let ScalarValue::Float64(Some(val)) = &values[*current_index] {
                        builder.append_value(*val);
                    }
                    *current_index = (*current_index + 1) % values.len();
                }
                Ok(Arc::new(builder.finish()) as ArrayRef)
            },
            ScalarValue::Utf8(_) => {
                let mut builder = StringBuilder::with_capacity(batch_keys.len(), batch_keys.len() * 20);
                for key in batch_keys {
                    let key_values_indices = per_key_values_indices.get_mut(key).unwrap();
                    let current_index = key_values_indices.entry(field_idx).or_insert(0);
                    
                    if let ScalarValue::Utf8(Some(val)) = &values[*current_index] {
                        builder.append_value(val);
                    }
                    *current_index = (*current_index + 1) % values.len();
                }
                Ok(Arc::new(builder.finish()) as ArrayRef)
            },
            _ => Err(anyhow::anyhow!("Unsupported values type: {:?}", values[0]))
        }
    }

    /// Check if we should generate more records
    fn should_continue(&self) -> bool {
        // Check record limit
        if let Some(limit) = self.task_records_limit {
            if self.records_generated >= limit {
                return false;
            }
        }
        
        // Check time limit
        if let Some(run_for_s) = self.config.run_for_s {
            if let Some(start_time) = self.start_time {
                let elapsed = start_time.elapsed().unwrap_or_default();
                if elapsed.as_secs_f64() >= run_for_s {
                    return false;
                }
            }
        }
        
        true
    }

    /// Send max watermark if needed
    fn send_max_watermark_if_needed(&mut self) -> Option<Message> {
        if !self.max_watermark_sent {
            self.max_watermark_sent = true;
            Some(Message::Watermark(crate::common::message::WatermarkMessage::new(
                self.vertex_id.clone().unwrap(),
                MAX_WATERMARK_VALUE,
                None
            )))
        } else {
            None
        }
    }

}

#[async_trait]
impl SourceFunctionTrait for DatagenSourceFunction {
    async fn fetch(&mut self) -> Option<Message> {
        // If this task has no keys assigned, don't produce anything
        if self.key_values.is_empty() {
            return self.send_max_watermark_if_needed();
        }

        // Check if we should continue generating
        if !self.should_continue() {
            return self.send_max_watermark_if_needed();
        }

        // Generate batch - adjust size if we're near the limit
        let batch_size = if let Some(limit) = self.task_records_limit {
            std::cmp::min(self.config.batch_size, limit - self.records_generated)
        } else {
            self.config.batch_size
        };

        if batch_size == 0 {
            return self.send_max_watermark_if_needed();
        }

        // Wait for next gen time if rate limiting is enabled
        if let Some(next_time) = self.next_gen_time {
            if let Ok(sleep_duration) = next_time.duration_since(SystemTime::now()) {
                tokio::time::sleep(sleep_duration).await;
            }
            
            // Calculate next event time based on the number of events in this batch
            if let Some(interval) = self.gen_interval {
                self.next_gen_time = Some(next_time + interval * batch_size as u32);
            }
        }

        match self.generate_batch(batch_size) {
            Ok(batch) => Some(Message::new(None, batch, None, None)),
            Err(e) => {
                panic!("Error generating batch: {}", e);
            }
        }
    }

    async fn snapshot_position(&self) -> Result<Vec<u8>> {
        if !self.config.replayable {
            return Ok(vec![]);
        }

        let pos = DatagenSourcePosition {
            records_generated: self.records_generated,
            max_watermark_sent: self.max_watermark_sent,
            current_key_index: self.current_key_index,
            task_records_limit: self.task_records_limit,
            key_values: self.key_values.clone(),
            per_key_timestamps: self.per_key_timestamps.clone(),
            per_key_increments: self.per_key_increments.clone(),
            per_key_values_indices: self.per_key_values_indices.clone(),
        };
        Ok(bincode::serialize(&pos)?)
    }

    async fn restore_position(&mut self, bytes: &[u8]) -> Result<()> {
        if !self.config.replayable {
            return Ok(());
        }
        if bytes.is_empty() {
            return Ok(());
        }
        let pos: DatagenSourcePosition = bincode::deserialize(bytes)?;
        self.records_generated = pos.records_generated;
        self.max_watermark_sent = pos.max_watermark_sent;
        self.current_key_index = pos.current_key_index;
        self.task_records_limit = pos.task_records_limit;
        self.key_values = pos.key_values;
        self.per_key_timestamps = pos.per_key_timestamps;
        self.per_key_increments = pos.per_key_increments;
        self.per_key_values_indices = pos.per_key_values_indices;
        Ok(())
    }
}

#[async_trait]
impl FunctionTrait for DatagenSourceFunction {
    async fn open(&mut self, ctx: &RuntimeContext) -> Result<()> {
        self.validate_replayable_config();
        // Set runtime context
        self.task_index = Some(ctx.task_index());
        self.parallelism = Some(ctx.parallelism());
        self.vertex_id = Some(ctx.vertex_id().to_string());
        // Calculate this task's share of records
        if let Some(global_limit) = self.config.limit {
            let records_per_task = global_limit / ctx.parallelism() as usize;
            let remainder = global_limit % ctx.parallelism() as usize;
            
            // If there's a remainder, give it to the last task
            self.task_records_limit = Some(
                if remainder != 0 && ctx.task_index() == ctx.parallelism() - 1 {
                    records_per_task + remainder
                } else {
                    records_per_task
                }
            );
        }
        
        // Initialize RNG with deterministic seed
        let base_seed = 42;
        let task_seed = base_seed + ctx.task_index() as u64;
        self.rng = Some(StdRng::seed_from_u64(task_seed));
        
        // Initialize keys
        self.init_keys();
        
        // Set start time if run_for_s is configured
        if self.config.run_for_s.is_some() {
            self.start_time = Some(SystemTime::now());
        }
        
        // Set up timing if rate limiting is enabled
        if let Some((interval, start_time)) = self.calculate_timing() {
            self.gen_interval = Some(interval);
            self.next_gen_time = Some(start_time);
        }
        
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::runtime_context::RuntimeContext;
    use std::collections::HashSet;
    use arrow::array::{Array, StringArray, Int64Array, Float64Array, TimestampMillisecondArray};
    use arrow::datatypes::{DataType, Field, TimeUnit};

    fn create_test_config() -> DatagenSourceConfig {
        let schema = Arc::new(Schema::new(vec![
            Field::new("event_time", DataType::Timestamp(TimeUnit::Millisecond, None), false),
            Field::new("key", DataType::Utf8, false),
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
            Field::new("category", DataType::Utf8, false),
        ]));

        let fields = HashMap::from([
            ("event_time".to_string(), FieldGenerator::IncrementalTimestamp { 
                start_ms: 1000000, 
                step_ms: 100 
            }),
            ("key".to_string(), FieldGenerator::Key { 
                num_unique: 6  // 6 keys total across all tasks
            }),
            ("id".to_string(), FieldGenerator::Increment { 
                start: ScalarValue::Int64(Some(1)), 
                step: ScalarValue::Int64(Some(1))
            }),
            ("value".to_string(), FieldGenerator::Uniform { 
                min: ScalarValue::Float64(Some(0.0)), 
                max: ScalarValue::Float64(Some(100.0))
            }),
            ("category".to_string(), FieldGenerator::Values { 
                values: vec![
                    ScalarValue::Utf8(Some("A".to_string())),
                    ScalarValue::Utf8(Some("B".to_string())),
                    ScalarValue::Utf8(Some("C".to_string())),
                ]
            }),
        ]);

        DatagenSourceConfig::new(schema, Some(1000.0), Some(121), None, 10, fields)
    }

    #[tokio::test]
    async fn test_datagen() {
        let config = create_test_config();
        let parallelism = 3;
        let mut all_records = Vec::new();
        let mut all_keys = HashSet::new();
        let mut total_records = 0;

        // Run multiple tasks in parallel
        for task_index in 0..parallelism {
            let mut source = DatagenSourceFunction::new(config.clone());
            
            let ctx = RuntimeContext::new(
                "test".to_string(),
                task_index,
                parallelism,
                None,
                None,
                None
            );
            
            source.open(&ctx).await.unwrap();
            
            let mut task_records = Vec::new();
            let mut batch_count = 0;
            
            loop {
                match source.fetch().await {
                    Some(Message::Regular(base_msg)) => {
                        let batch = &base_msg.record_batch;
                        
                        // Extract data from batch
                        let event_times = batch.column(0).as_any().downcast_ref::<TimestampMillisecondArray>().unwrap();
                        let keys = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
                        let ids = batch.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
                        let values = batch.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
                        let categories = batch.column(4).as_any().downcast_ref::<StringArray>().unwrap();
                        
                        for row in 0..batch.num_rows() {
                            let record = (
                                event_times.value(row),
                                keys.value(row).to_string(),
                                ids.value(row),
                                values.value(row),
                                categories.value(row).to_string(),
                            );
                            task_records.push(record.clone());
                            all_keys.insert(record.1.clone());
                        }
                        
                        total_records += batch.num_rows();
                        batch_count += 1;
                    },
                    Some(Message::Keyed(_)) => {
                        panic!("Unexpected keyed message");
                    },
                    Some(Message::Watermark(_)) => break,
                    Some(Message::CheckpointBarrier(_)) => break,
                    None => break,
                }
                
                if batch_count > 50 {
                    break;
                }
            }
            
            all_records.extend(task_records);
        }
        
        // Verify total record count
        assert_eq!(total_records, 121, "Should generate exactly 120 records");
        
        // Verify unique keys (should be 6 total, distributed across tasks)
        assert_eq!(all_keys.len(), 6, "Should have exactly 6 unique keys");
        
        // Verify key format (should be key-{task_id}-{key_id})
        for key in &all_keys {
            assert!(key.starts_with("key-"), "Key should start with 'key-': {}", key);
        }
        
        // Group records by key and verify per-key properties
        let mut records_by_key: HashMap<String, Vec<_>> = HashMap::new();
        for record in &all_records {
            records_by_key.entry(record.1.clone()).or_insert_with(Vec::new).push(record);
        }
        
        for (key, key_records) in records_by_key {
            // Verify incremental timestamps per key
            let mut prev_timestamp = None;
            for record in &key_records {
                if let Some(prev) = prev_timestamp {
                    assert!(record.0 > prev, "Timestamps should be incremental for key {}", key);
                }
                prev_timestamp = Some(record.0);
            }
            
            // Verify incremental IDs per key
            let mut prev_id = None;
            for record in &key_records {
                if let Some(prev) = prev_id {
                    assert_eq!(record.2, prev + 1, "IDs should increment by 1 for key {}", key);
                }
                prev_id = Some(record.2);
            }
            
            // Verify uniform values are in range
            for record in &key_records {
                assert!(record.3 >= 0.0 && record.3 <= 100.0, "Value should be in range [0, 100] for key {}", key);
            }
            
            // Verify categories are from the expected set
            for record in &key_records {
                assert!(["A", "B", "C"].contains(&record.4.as_str()), "Category should be A, B, or C for key {}", key);
            }
        }
    }

    #[tokio::test]
    async fn test_datagen_rate_timing() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("processing_time", DataType::Timestamp(TimeUnit::Millisecond, None), false),
            Field::new("key", DataType::Utf8, false),
            Field::new("id", DataType::Int64, false),
        ]));

        let fields = HashMap::from([
            ("processing_time".to_string(), FieldGenerator::ProcessingTimestamp),
            ("key".to_string(), FieldGenerator::Key { num_unique: 4 }),
            ("id".to_string(), FieldGenerator::Increment { 
                start: ScalarValue::Int64(Some(1)), 
                step: ScalarValue::Int64(Some(1))
            }),
        ]);

        let config = DatagenSourceConfig::new(schema, Some(10.0), Some(40), None, 1, fields);

        let parallelism = 4; // 4 tasks, so each should generate at ~10 events/sec
        // Run tasks concurrently using tokio::spawn
        let mut handles = Vec::new();
        
        for task_index in 0..parallelism {
            let config_clone = config.clone();
            let handle = tokio::spawn(async move {
                let mut source = DatagenSourceFunction::new(config_clone);
                
                let ctx = RuntimeContext::new(
                    "test".to_string(),
                    task_index,
                    parallelism,
                    None,
                    None,
                    None
                );
                
                source.open(&ctx).await.unwrap();
                
                let mut task_timestamps = Vec::new();
                let mut batch_count = 0;
                
                loop {
                    match source.fetch().await {
                        Some(Message::Regular(base_msg)) => {
                            let batch = &base_msg.record_batch;
                            let processing_times = batch.column(0).as_any().downcast_ref::<TimestampMillisecondArray>().unwrap();
                            
                            for row in 0..batch.num_rows() {
                                task_timestamps.push(processing_times.value(row));
                            }
                            
                            batch_count += 1;
                        },
                        Some(Message::Watermark(_)) => break,
                        None => break,
                        _ => {}
                    }
                    
                    if batch_count > 50 {
                        panic!("Batch count exceeded 50");
                    }
                }
                
                task_timestamps
            });
            handles.push(handle);
        }
        
        let mut all_timestamps = Vec::new();

        // Collect results from all tasks
        for handle in handles {
            let task_timestamps = handle.await.unwrap();
            all_timestamps.extend(task_timestamps);
        }
        
        // Sort timestamps to analyze timing
        all_timestamps.sort();
        
        // Verify we got the expected number of records
        assert_eq!(all_timestamps.len(), 40, "Should generate exactly 40 records");
        
        let rate = config.rate.expect("Rate should be set for this test");
        let expected_interval_ms = 1000.0 / rate; // Global interval: 100ms for 10 events/sec
        let tolerance = expected_interval_ms * 0.5; // ±50% tolerance for timing variations
        
         for i in 1..all_timestamps.len() {
            let interval = all_timestamps[i] - all_timestamps[i-1];
            assert!(
                interval as f32 >= expected_interval_ms - tolerance &&
                interval as f32 <= expected_interval_ms + tolerance,
                "Interval {:.1}ms should be within ±{:.1}ms of expected {:.1}ms",
                interval, tolerance, expected_interval_ms
            );
         }
    }
}
