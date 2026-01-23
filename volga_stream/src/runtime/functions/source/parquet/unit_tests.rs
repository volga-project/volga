use super::*;
use std::collections::HashMap;
use std::fs::{self, File};
use std::path::Path;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use uuid::Uuid;

fn write_parquet(path: &Path, schema: SchemaRef, batch: RecordBatch) {
    let file = File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

#[tokio::test]
async fn parquet_source_applies_projection_mask() {
    let tmp_dir = std::env::temp_dir().join(format!("volga_parquet_test_{}", Uuid::new_v4()));
    fs::create_dir_all(&tmp_dir).unwrap();
    let file_path = tmp_dir.join("input.parquet");

    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Utf8, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["A"])) as _,
            Arc::new(Int64Array::from(vec![42_i64])) as _,
        ],
    )
    .unwrap();
    write_parquet(&file_path, schema.clone(), batch);

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

    let msg = source.fetch().await.expect("expected batch");
    let batch = msg.record_batch();
    assert_eq!(batch.schema(), projected_schema);
    assert_eq!(batch.num_columns(), 1);
}
