use super::*;
use std::collections::HashMap;
use std::fs::{self, File};
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use futures::StreamExt;
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::throttle::{ThrottleConfig, ThrottledStore};
use parquet::arrow::ArrowWriter;
use uuid::Uuid;

use crate::runtime::functions::sink::parquet::{ParquetSinkConfig, ParquetSinkFunction, ParquetSinkSpec};
use crate::runtime::functions::sink::SinkFunctionTrait;
use crate::tests::support::parquet::{parquet_roundtrip_via_sink_and_source, test_schema};

fn write_parquet_file(path: &Path, schema: SchemaRef, batch: RecordBatch) {
    let file = File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

fn make_batch(schema: SchemaRef, k: &str, v: i64) -> RecordBatch {
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![k])) as _,
            Arc::new(Int64Array::from(vec![v])) as _,
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn parquet_source_applies_projection_mask() {
    let tmp_dir = std::env::temp_dir().join(format!("volga_parquet_test_{}", Uuid::new_v4()));
    fs::create_dir_all(&tmp_dir).unwrap();
    let file_path = tmp_dir.join("input.parquet");

    let schema = test_schema();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["A"])) as _,
            Arc::new(Int64Array::from(vec![42_i64])) as _,
        ],
    )
    .unwrap();
    write_parquet_file(&file_path, schema.clone(), batch);

    let spec = ParquetSourceSpec {
        path: format!("file://{}", tmp_dir.to_string_lossy()),
        storage_options: HashMap::new(),
        regex_pattern: None,
        batch_size: Some(1024),
    };
    let mut config = ParquetSourceConfig::new(schema.clone(), spec);
    let projection = vec![0];
    let projected_schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Utf8, false),
    ]));
    config.set_projection(projection, projected_schema.clone());

    let mut source = ParquetSourceFunction::new(config);
    let ctx = RuntimeContext::new(
        "parquet_source".to_string().into(),
        0,
        1,
        None,
        None,
        None,
    );
    source.open(&ctx).await.unwrap();

    let msg = source.fetch(None).await.expect_data("expected batch");
    let batch = msg.record_batch();
    assert_eq!(batch.schema(), projected_schema);
    assert_eq!(batch.num_columns(), 1);
}

#[tokio::test]
async fn parquet_localfs_roundtrip() {
    let tmp_dir = std::env::temp_dir().join(format!("volga_parquet_local_{}", Uuid::new_v4()));
    let input_dir = tmp_dir.join("input");
    let output_dir = tmp_dir.join("output");
    fs::create_dir_all(&input_dir).unwrap();
    fs::create_dir_all(&output_dir).unwrap();

    let schema = test_schema();
    parquet_roundtrip_via_sink_and_source(
        schema,
        format!("file://{}", output_dir.to_string_lossy()),
        HashMap::new(),
        format!("file://{}", output_dir.to_string_lossy()),
        HashMap::new(),
    )
    .await;
}

#[tokio::test]
async fn parquet_parallel_tasks_consume_all_files() {
    let tmp_dir = std::env::temp_dir().join(format!("volga_parquet_parallel_{}", Uuid::new_v4()));
    fs::create_dir_all(&tmp_dir).unwrap();

    let schema = test_schema();
    for i in 0..4 {
        write_parquet_file(
            &tmp_dir.join(format!("input_{}.parquet", i)),
            schema.clone(),
            make_batch(schema.clone(), "A", i as i64),
        );
    }

    let spec = ParquetSourceSpec {
        path: format!("file://{}", tmp_dir.to_string_lossy()),
        storage_options: HashMap::new(),
        regex_pattern: None,
        batch_size: Some(1024),
    };
    let config = ParquetSourceConfig::new(schema.clone(), spec);

    let mut rows = Vec::new();
    for task_index in 0..2 {
        let mut source = ParquetSourceFunction::new(config.clone());
        let ctx = RuntimeContext::new("src".to_string().into(), task_index, 2, None, None, None);
        source.open(&ctx).await.unwrap();
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
                rows.push((keys.value(row).to_string(), values.value(row)));
            }
        }
    }
    rows.sort();
    let mut expected = vec![
        ("A".to_string(), 0),
        ("A".to_string(), 1),
        ("A".to_string(), 2),
        ("A".to_string(), 3),
    ];
    expected.sort();
    assert_eq!(rows, expected);
}

#[tokio::test]
async fn parquet_projection_pushdown_roundtrip() {
    let tmp_dir = std::env::temp_dir().join(format!("volga_parquet_projection_{}", Uuid::new_v4()));
    let input_dir = tmp_dir.join("input");
    fs::create_dir_all(&input_dir).unwrap();

    let schema = test_schema();
    write_parquet_file(
        &input_dir.join("input.parquet"),
        schema.clone(),
        make_batch(schema.clone(), "A", 1),
    );

    let spec = ParquetSourceSpec {
        path: format!("file://{}", input_dir.to_string_lossy()),
        storage_options: HashMap::new(),
        regex_pattern: None,
        batch_size: Some(1024),
    };
    let mut config = ParquetSourceConfig::new(schema.clone(), spec);
    let projection = vec![0];
    let projected_schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Utf8, false),
    ]));
    config.set_projection(projection, projected_schema.clone());

    let mut source = ParquetSourceFunction::new(config);
    let ctx = RuntimeContext::new("src".to_string().into(), 0, 1, None, None, None);
    source.open(&ctx).await.unwrap();

    let msg = source.fetch(None).await.expect_data("expected batch");
    let batch = msg.record_batch();
    assert_eq!(batch.schema(), projected_schema);
    assert_eq!(batch.num_columns(), 1);
}

#[tokio::test]
async fn parquet_partitioned_sink_writes_directories() {
    let tmp_dir = std::env::temp_dir().join(format!("volga_parquet_partitions_{}", Uuid::new_v4()));
    let output_dir = tmp_dir.join("output");
    fs::create_dir_all(&output_dir).unwrap();

    let schema = test_schema();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["A", "B"])) as _,
            Arc::new(Int64Array::from(vec![1_i64, 2_i64])) as _,
        ],
    )
    .unwrap();

    let sink_spec = ParquetSinkSpec {
        path: format!("file://{}", output_dir.to_string_lossy()),
        storage_options: HashMap::new(),
        compression: None,
        row_group_size_bytes: None,
        target_file_size: None,
        max_buffer_bytes: None,
        max_concurrent_puts: None,
        partition_fields: Some(vec!["k".to_string()]),
    };
    let sink_config = ParquetSinkConfig::new(sink_spec);
    let mut sink = ParquetSinkFunction::new(sink_config);
    let sink_ctx = RuntimeContext::new("sink".to_string().into(), 0, 1, None, None, None);
    sink.open(&sink_ctx).await.unwrap();
    sink.sink(Message::new(None, batch, None, None)).await.unwrap();
    sink.close().await.unwrap();

    let (store, prefix) = crate::runtime::functions::parquet_utils::build_object_store(
        &format!("file://{}", output_dir.to_string_lossy()),
        &HashMap::new(),
        false,
    )
    .unwrap();
    let mut listed = store.list(Some(&prefix));
    let mut seen = Vec::new();
    while let Some(item) = listed.next().await {
        let meta = item.unwrap();
        seen.push(meta.location.to_string());
    }
    assert!(seen.iter().any(|p| p.contains("k=A")));
    assert!(seen.iter().any(|p| p.contains("k=B")));
}

#[tokio::test]
async fn parquet_sink_bounded_concurrency_backpressure() {
    let delay_ms = 40;
    let max_concurrent = 2_usize;
    let batches = 12;

    let throttle = ThrottledStore::new(InMemory::new(), ThrottleConfig::default());
    throttle.config_mut(|cfg: &mut ThrottleConfig| {
        cfg.wait_put_per_call = std::time::Duration::from_millis(delay_ms);
    });
    let store = Arc::new(throttle) as Arc<dyn object_store::ObjectStore>;

    let sink_spec = ParquetSinkSpec {
        path: "mem://test".to_string(),
        storage_options: HashMap::new(),
        compression: None,
        row_group_size_bytes: None,
        target_file_size: Some(1),
        max_buffer_bytes: None,
        max_concurrent_puts: Some(max_concurrent),
        partition_fields: Some(vec!["k".to_string()]),
    };
    let sink_config = ParquetSinkConfig::new(sink_spec);
    let mut sink = ParquetSinkFunction::new_with_store(sink_config, store.clone(), ObjectPath::from(""));
    let sink_ctx = RuntimeContext::new("sink".to_string().into(), 0, 1, None, None, None);
    sink.open(&sink_ctx).await.unwrap();

    let schema = test_schema();
    let start = std::time::Instant::now();
    for i in 0..batches {
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
    let mut total_bytes = 0_u64;
    let mut listed = store.list(None);
    while let Some(item) = listed.next().await {
        let meta = item.unwrap();
        total_bytes += meta.size;
    }
    let elapsed = start.elapsed();
    let expected_min = std::time::Duration::from_millis(
        (batches as u64 * delay_ms as u64) / max_concurrent as u64,
    );
    assert!(elapsed >= expected_min);
    let throughput_mb_s = (total_bytes as f64 / (1024.0 * 1024.0)) / elapsed.as_secs_f64();
    println!("parquet_sink_throughput_mb_s={:.2}", throughput_mb_s);
}
