use super::*;
use crate::runtime::functions::sink::parquet::{ParquetSinkConfig, ParquetSinkFunction, ParquetSinkSpec};
use crate::runtime::functions::sink::SinkFunctionTrait;
use crate::runtime::runtime_context::RuntimeContext;
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use std::collections::HashMap;
use std::fs::{self, File};
use std::path::Path;
use std::sync::Arc;
use object_store::path::Path as ObjectPath;
use object_store::memory::InMemory;
use object_store::throttle::{ThrottleConfig, ThrottledStore};
use testcontainers::{clients::Cli, GenericImage, RunnableImage};
use testcontainers::core::WaitFor;
use uuid::Uuid;

fn write_parquet_file(path: &Path, schema: SchemaRef, batch: RecordBatch) {
    let file = File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Utf8, false),
        Field::new("v", DataType::Int64, false),
    ]))
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

async fn parquet_roundtrip_via_sink_and_source(
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
        sink.sink(Message::new(None, batch.clone(), None, None)).await.unwrap();
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
    while let Some(msg) = source.fetch().await {
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
        while let Some(msg) = source.fetch().await {
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
    write_parquet_file(&input_dir.join("input.parquet"), schema.clone(), make_batch(schema.clone(), "A", 1));

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

    let msg = source.fetch().await.expect("expected batch");
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

    let (store, prefix) = super::build_object_store(
        &format!("file://{}", output_dir.to_string_lossy()),
        &HashMap::new(),
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

#[tokio::test]
#[ignore]
async fn parquet_s3_roundtrip_localstack() {
    let docker = Cli::default();
    let image = GenericImage::new("localstack/localstack", "3.0")
        .with_env_var("SERVICES", "s3")
        .with_env_var("DEFAULT_REGION", "us-east-1")
        .with_exposed_port(4566)
        .with_wait_for(WaitFor::seconds(3));
    let runnable = RunnableImage::from(image).with_mapped_port((4566, 4566));
    let container = docker.run(runnable);
    let endpoint = format!("http://127.0.0.1:{}", container.get_host_port_ipv4(4566));
    wait_for_localstack_ready("127.0.0.1:4566").await;
    create_localstack_bucket(&endpoint, "volga-test").await;

    let schema = test_schema();
    let opts = HashMap::from([
        ("endpoint_url".to_string(), endpoint),
        ("region".to_string(), "us-east-1".to_string()),
        ("access_key_id".to_string(), "test".to_string()),
        ("secret_access_key".to_string(), "test".to_string()),
    ]);
    parquet_roundtrip_via_sink_and_source(
        schema,
        "s3://volga-test/output".to_string(),
        opts.clone(),
        "s3://volga-test/output".to_string(),
        opts,
    )
    .await;
}

async fn wait_for_localstack_ready(addr: &str) {
    for _ in 0..40 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    panic!("LocalStack did not become ready at {}", addr);
}

async fn create_localstack_bucket(endpoint: &str, bucket: &str) {
    let url = format!("{}/{}", endpoint, bucket);
    let client = reqwest::Client::new();
    for _ in 0..10 {
        match client.put(&url).send().await {
            Ok(resp) if resp.status().is_success() || resp.status().as_u16() == 409 => {
                return;
            }
            _ => {}
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    panic!("Failed to create LocalStack bucket at {}", url);
}
