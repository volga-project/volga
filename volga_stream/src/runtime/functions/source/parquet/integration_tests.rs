use super::*;
use crate::runtime::functions::sink::parquet::{ParquetSinkConfig, ParquetSinkFunction, ParquetSinkSpec};
use crate::runtime::functions::sink::SinkFunctionTrait;
use crate::runtime::runtime_context::RuntimeContext;
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;
use bytes::Bytes;
use object_store::path::Path as ObjectPath;
use testcontainers::{clients::Cli, GenericImage, RunnableImage, WaitFor};

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

#[tokio::test]
async fn parquet_localfs_roundtrip() {
    let tmp_dir = std::env::temp_dir().join(format!("volga_parquet_local_{}", Uuid::new_v4()));
    let input_dir = tmp_dir.join("input");
    let output_dir = tmp_dir.join("output");
    fs::create_dir_all(&input_dir).unwrap();
    fs::create_dir_all(&output_dir).unwrap();

    let schema = test_schema();
    write_parquet_file(&input_dir.join("a.parquet"), schema.clone(), make_batch(schema.clone(), "A", 1));
    write_parquet_file(&input_dir.join("b.parquet"), schema.clone(), make_batch(schema.clone(), "B", 2));

    let source_spec = ParquetSourceSpec {
        path: format!("file://{}", input_dir.to_string_lossy()),
        storage_options: HashMap::new(),
        regex_pattern: None,
        batch_size: Some(1024),
    };
    let source_config = ParquetSourceConfig::new(schema.clone(), source_spec);
    let mut source = ParquetSourceFunction::new(source_config);
    let source_ctx = RuntimeContext::new("src".to_string().into(), 0, 1, None, None, None);
    source.open(&source_ctx).await.unwrap();

    let sink_spec = ParquetSinkSpec {
        path: format!("file://{}", output_dir.to_string_lossy()),
        storage_options: HashMap::new(),
        compression: None,
        row_group_size_bytes: None,
        target_file_size: None,
        max_buffer_bytes: None,
        partition_fields: None,
    };
    let sink_config = ParquetSinkConfig::new(sink_spec);
    let mut sink = ParquetSinkFunction::new(sink_config);
    let sink_ctx = RuntimeContext::new("sink".to_string().into(), 0, 1, None, None, None);
    sink.open(&sink_ctx).await.unwrap();

    while let Some(msg) = source.fetch().await {
        sink.sink(msg).await.unwrap();
    }
    sink.close().await.unwrap();

    let (store, prefix) = super::build_object_store(
        &format!("file://{}", output_dir.to_string_lossy()),
        &HashMap::new(),
    )
    .unwrap();
    let mut listed = store.list(Some(&prefix));
    let mut count = 0;
    while let Some(item) = listed.next().await {
        item.unwrap();
        count += 1;
    }
    assert!(count > 0);
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

    let mut total = 0;
    for task_index in 0..2 {
        let mut source = ParquetSourceFunction::new(config.clone());
        let ctx = RuntimeContext::new("src".to_string().into(), task_index, 2, None, None, None);
        source.open(&ctx).await.unwrap();
        while let Some(_msg) = source.fetch().await {
            total += 1;
        }
    }
    assert_eq!(total, 4);
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
#[ignore]
async fn parquet_s3_roundtrip_localstack() {
    let docker = Cli::default();
    let image = GenericImage::new("localstack/localstack", "latest")
        .with_env_var("SERVICES", "s3")
        .with_env_var("DEFAULT_REGION", "us-east-1")
        .with_exposed_port(4566)
        .with_wait_for(WaitFor::seconds(5));
    let runnable = RunnableImage::from(image);
    let container = docker.run(runnable);
    let endpoint = format!("http://127.0.0.1:{}", container.get_host_port_ipv4(4566));

    std::env::set_var("AWS_ACCESS_KEY_ID", "test");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
    std::env::set_var("AWS_REGION", "us-east-1");
    let config = aws_config::from_env()
        .endpoint_url(endpoint.clone())
        .load()
        .await;
    let client = aws_sdk_s3::Client::new(&config);
    client
        .create_bucket()
        .bucket("volga-test")
        .send()
        .await
        .unwrap();

    let schema = test_schema();
    let batch = make_batch(schema.clone(), "A", 1);
    let buffer = Cursor::new(Vec::new());
    let mut writer = ArrowWriter::try_new(buffer, schema.clone(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
    let buffer = writer.into_inner().unwrap();

    let store = object_store::aws::AmazonS3Builder::new()
        .with_bucket_name("volga-test")
        .with_region("us-east-1")
        .with_endpoint(endpoint.clone())
        .with_allow_http(true)
        .with_access_key_id("test")
        .with_secret_access_key("test")
        .build()
        .unwrap();
    store
        .put(&ObjectPath::from("input/a.parquet"), Bytes::from(buffer.into_inner()))
        .await
        .unwrap();

    let source_spec = ParquetSourceSpec {
        path: "s3://volga-test/input".to_string(),
        storage_options: HashMap::from([
            ("endpoint_url".to_string(), endpoint.clone()),
            ("region".to_string(), "us-east-1".to_string()),
            ("access_key_id".to_string(), "test".to_string()),
            ("secret_access_key".to_string(), "test".to_string()),
        ]),
        regex_pattern: None,
        batch_size: Some(1024),
    };
    let source_config = ParquetSourceConfig::new(schema.clone(), source_spec);
    let mut source = ParquetSourceFunction::new(source_config);
    let source_ctx = RuntimeContext::new("src".to_string().into(), 0, 1, None, None, None);
    source.open(&source_ctx).await.unwrap();

    let sink_spec = ParquetSinkSpec {
        path: "s3://volga-test/output".to_string(),
        storage_options: HashMap::from([
            ("endpoint_url".to_string(), endpoint.clone()),
            ("region".to_string(), "us-east-1".to_string()),
            ("access_key_id".to_string(), "test".to_string()),
            ("secret_access_key".to_string(), "test".to_string()),
        ]),
        compression: None,
        row_group_size_bytes: None,
        target_file_size: None,
        max_buffer_bytes: None,
        partition_fields: None,
    };
    let sink_config = ParquetSinkConfig::new(sink_spec);
    let mut sink = ParquetSinkFunction::new(sink_config);
    let sink_ctx = RuntimeContext::new("sink".to_string().into(), 0, 1, None, None, None);
    sink.open(&sink_ctx).await.unwrap();

    while let Some(msg) = source.fetch().await {
        sink.sink(msg).await.unwrap();
    }
    sink.close().await.unwrap();

    let store = object_store::aws::AmazonS3Builder::new()
        .with_bucket_name("volga-test")
        .with_region("us-east-1")
        .with_endpoint(endpoint)
        .with_allow_http(true)
        .with_access_key_id("test")
        .with_secret_access_key("test")
        .build()
        .unwrap();
    let mut listed = store.list(Some(&ObjectPath::from("output")));
    let mut count = 0;
    while let Some(item) = listed.next().await {
        item.unwrap();
        count += 1;
    }
    assert!(count > 0);
}
