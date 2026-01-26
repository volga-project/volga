use super::*;
use crate::runtime::runtime_context::RuntimeContext;
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use std::fs;
use uuid::Uuid;

#[tokio::test]
async fn sink_enforces_max_buffer_bytes() {
    let tmp_dir = std::env::temp_dir().join(format!("volga_parquet_sink_{}", Uuid::new_v4()));
    fs::create_dir_all(&tmp_dir).unwrap();

    let spec = ParquetSinkSpec {
        path: format!("file://{}", tmp_dir.to_string_lossy()),
        storage_options: HashMap::new(),
        compression: None,
        row_group_size_bytes: None,
        target_file_size: None,
        max_buffer_bytes: Some(1),
        max_concurrent_puts: None,
        partition_fields: Some(vec!["k".to_string()]),
    };
    let mut sink = ParquetSinkFunction::new(ParquetSinkConfig::new(spec));
    let ctx = RuntimeContext::new("sink".to_string().into(), 0, 1, None, None, None);
    sink.open(&ctx).await.unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Utf8, false),
        Field::new("v", DataType::Int64, false),
    ]));
    for i in 0..5 {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![format!("k{}", i)])) as _,
                Arc::new(Int64Array::from(vec![i as i64])) as _,
            ],
        )
        .unwrap();
        sink.sink(Message::new(None, batch, None, None)).await.unwrap();
    }

    sink.close().await.unwrap();
    let entries: Vec<_> = fs::read_dir(&tmp_dir).unwrap().collect();
    assert!(entries.len() > 1);
}

#[test]
fn partition_batches_respects_partition_fields() {
    let spec = ParquetSinkSpec {
        path: "file:///tmp".to_string(),
        storage_options: HashMap::new(),
        compression: None,
        row_group_size_bytes: None,
        target_file_size: None,
        max_buffer_bytes: None,
        max_concurrent_puts: None,
        partition_fields: Some(vec!["k".to_string()]),
    };
    let sink = ParquetSinkFunction::new(ParquetSinkConfig::new(spec));

    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Utf8, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["A", "B"])) as _,
            Arc::new(Int64Array::from(vec![1_i64, 2_i64])) as _,
        ],
    )
    .unwrap();

    let partitions = sink.partition_batches(batch).unwrap();
    let mut partition_paths: Vec<String> = partitions
        .into_iter()
        .map(|(_, path)| path)
        .collect();
    partition_paths.sort();
    assert_eq!(partition_paths, vec!["k=A".to_string(), "k=B".to_string()]);
}
