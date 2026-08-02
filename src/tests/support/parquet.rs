use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow::datatypes::SchemaRef;

use crate::common::message::Message;
use crate::runtime::functions::function_trait::FunctionTrait;
use crate::runtime::functions::sink::parquet::{ParquetSinkConfig, ParquetSinkFunction, ParquetSinkSpec};
use crate::runtime::functions::sink::SinkFunctionTrait;
use crate::runtime::functions::source::parquet::{
    ParquetSourceConfig, ParquetSourceFunction, ParquetSourceSpec,
};
use crate::runtime::functions::source::source_function::SourceFunctionTrait;
use crate::runtime::runtime_context::RuntimeContext;

pub fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Utf8, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

pub async fn parquet_roundtrip_via_sink_and_source(
    schema: SchemaRef,
    sink_path: String,
    sink_options: HashMap<String, String>,
    source_path: String,
    source_options: HashMap<String, String>,
) {
    let sink_spec = ParquetSinkSpec {
        path: sink_path,
        storage_options: sink_options,
        compression: None,
        row_group_size_bytes: None,
        target_file_size: Some(1),
        max_buffer_bytes: None,
        max_concurrent_puts: None,
        partition_fields: None,
    };
    let sink_config = ParquetSinkConfig::new(sink_spec);
    let mut sink = ParquetSinkFunction::new(sink_config);
    let sink_ctx = RuntimeContext::new("sink".to_string().into(), 0, 1, None, None, None);
    sink.open(&sink_ctx).await.unwrap();

    let mut input_batches = Vec::new();
    for batch_idx in 0..10 {
        let mut keys = Vec::new();
        let mut values = Vec::new();
        for row in 0..100 {
            keys.push(format!("key_{}_{}", batch_idx, row));
            values.push((batch_idx * 100 + row) as i64);
        }
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(keys)) as _,
                Arc::new(Int64Array::from(values)) as _,
            ],
        )
        .unwrap();
        input_batches.push(batch);
    }
    for batch in &input_batches {
        sink.sink(Message::new(None, batch.clone(), None, None))
            .await
            .unwrap();
    }
    sink.close().await.unwrap();

    let source_spec = ParquetSourceSpec {
        path: source_path,
        storage_options: source_options,
        regex_pattern: None,
        batch_size: Some(1024),
    };
    let source_config = ParquetSourceConfig::new(schema.clone(), source_spec);
    let mut source = ParquetSourceFunction::new(source_config);
    let source_ctx = RuntimeContext::new("src".to_string().into(), 0, 1, None, None, None);
    source.open(&source_ctx).await.unwrap();

    let mut output_rows = Vec::new();
    while let Some(msg) = source.fetch(None).await.into_message() {
        let batch = msg.record_batch();
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            output_rows.push((keys.value(row).to_string(), values.value(row)));
        }
    }

    let mut input_rows = Vec::new();
    for batch in &input_batches {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            input_rows.push((keys.value(row).to_string(), values.value(row)));
        }
    }
    input_rows.sort();
    output_rows.sort();
    assert_eq!(output_rows, input_rows);
}
