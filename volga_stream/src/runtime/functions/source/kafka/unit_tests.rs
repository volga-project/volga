use super::*;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;

fn test_config() -> KafkaSourceConfig {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Utf8, false),
        Field::new("ts_ms", DataType::Int64, false),
    ]));
    let spec = KafkaSourceSpec {
        bootstrap_servers: "localhost:9092".to_string(),
        topic: "t".to_string(),
        group_id: None,
        group_id_prefix: None,
        offset: KafkaOffsetSpec::Latest,
        client_configs: HashMap::new(),
        poll_timeout_ms: 10,
        max_batch_records: Some(100),
        max_batch_bytes: Some(1024 * 1024),
    };
    KafkaSourceConfig::new(schema, spec)
}

#[test]
fn assigned_partitions_respect_task_index() {
    let parts = KafkaSourceFunction::compute_assigned_partitions(6, 1, 3);
    assert_eq!(parts, vec![1, 4]);
}

#[tokio::test]
async fn snapshot_restore_roundtrip() {
    let mut src = KafkaSourceFunction::new(test_config());
    src.offsets.insert(0, 10);
    src.offsets.insert(1, 20);

    let snapshot = src.snapshot_position().await.unwrap();

    let mut restored = KafkaSourceFunction::new(test_config());
    restored.restore_position(&snapshot).await.unwrap();
    assert_eq!(restored.pending_restore.unwrap(), src.offsets);
}

#[test]
fn decode_record_batch_roundtrip() {
    let config = test_config();
    let source = KafkaSourceFunction::new(config.clone());

    let batch = RecordBatch::try_new(
        config.schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["A"])) as _,
            Arc::new(Int64Array::from(vec![100_i64])) as _,
        ],
    )
    .unwrap();

    let mut out = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut out, &config.schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }

    let decoded = source.decode_record_batch(&out).unwrap();
    assert_eq!(decoded.num_rows(), 1);
    assert_eq!(decoded.schema(), config.schema);
}
