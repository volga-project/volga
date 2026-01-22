use super::*;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use rdkafka::consumer::BaseConsumer;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use testcontainers::clients;
use testcontainers::core::WaitFor;
use testcontainers::{GenericImage, RunnableImage};

use crate::common::message::Message;
use crate::runtime::functions::function_trait::FunctionTrait;
use crate::runtime::functions::source::source_function::SourceFunctionTrait;
use crate::runtime::runtime_context::RuntimeContext;

fn redpanda_image() -> RunnableImage<GenericImage> {
    let image = GenericImage::new("redpandadata/redpanda", "latest")
        .with_exposed_port(9092)
        .with_wait_for(WaitFor::seconds(3));
    let args = vec![
        "start".to_string(),
        "--overprovisioned".to_string(),
        "--smp".to_string(),
        "1".to_string(),
        "--memory".to_string(),
        "1G".to_string(),
        "--reserve-memory".to_string(),
        "0M".to_string(),
        "--node-id".to_string(),
        "0".to_string(),
        "--check=false".to_string(),
        "--kafka-addr".to_string(),
        "PLAINTEXT://0.0.0.0:9092".to_string(),
        "--advertise-kafka-addr".to_string(),
        "PLAINTEXT://127.0.0.1:9092".to_string(),
    ];
    RunnableImage::from((image, args)).with_mapped_port((9092, 9092))
}

fn test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Utf8, false),
        Field::new("ts_ms", DataType::Int64, false),
    ]))
}

fn make_payload(schema: &Schema, key: &str, ts: i64) -> Vec<u8> {
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(StringArray::from(vec![key])) as _,
            Arc::new(Int64Array::from(vec![ts])) as _,
        ],
    )
    .unwrap();

    let mut payload = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut payload, schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }
    payload
}

fn build_source(bootstrap_servers: String, schema: Arc<Schema>, offset: KafkaOffsetSpec) -> KafkaSourceFunction {
    let spec = KafkaSourceSpec {
        bootstrap_servers,
        topic: "kafka_source_test".to_string(),
        group_id: Some("kafka_source_test_group".to_string()),
        group_id_prefix: None,
        offset,
        client_configs: HashMap::new(),
        poll_timeout_ms: 50,
        max_batch_records: 1,
        max_batch_bytes: 1024 * 1024,
    };
    let config = KafkaSourceConfig::new(schema, spec);
    KafkaSourceFunction::new(config)
}

async fn read_n(source: &mut KafkaSourceFunction, n: usize) -> Vec<(String, i64)> {
    let mut out = Vec::new();
    while out.len() < n {
        let msg = source.fetch().await.expect("expected message");
        let batch = msg.record_batch();
        let keys = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let ts = batch.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..batch.num_rows() {
            out.push((keys.value(i).to_string(), ts.value(i)));
        }
    }
    out
}

async fn wait_for_broker(bootstrap_servers: &str) {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .create()
        .unwrap();

    for _ in 0..40 {
        if consumer
            .fetch_metadata(None, Duration::from_millis(250))
            .is_ok()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("Kafka broker did not become ready at {}", bootstrap_servers);
}

#[tokio::test]
#[ignore]
async fn kafka_source_reads_arrow_ipc() {
    let docker = clients::Cli::default();
    let node = docker.run(redpanda_image());
    let broker = node.get_host_port_ipv4(9092);
    let bootstrap_servers = format!("127.0.0.1:{broker}");
    wait_for_broker(&bootstrap_servers).await;

    let schema = test_schema();
    let payload = make_payload(&schema, "A", 100);

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .create()
        .unwrap();
    let _ = producer
        .send(
            FutureRecord::<(), _>::to("kafka_source_test").payload(&payload),
            Duration::from_secs(0),
        )
        .await
        .unwrap();

    let mut source = build_source(bootstrap_servers, schema, KafkaOffsetSpec::Earliest);

    let ctx = RuntimeContext::new(
        "kafka_source".to_string().into(),
        0,
        1,
        None,
        None,
        None,
    );

    source.open(&ctx).await.unwrap();
    let msg = source.fetch().await.expect("expected message");

    let batch = msg.record_batch();
    assert_eq!(batch.num_rows(), 1);
    assert!(matches!(msg, Message::Regular(_)));
}

#[tokio::test]
#[ignore]
async fn kafka_source_checkpoint_restore_replays_from_saved_offsets() {
    let docker = clients::Cli::default();
    let node = docker.run(redpanda_image());
    let broker = node.get_host_port_ipv4(9092);
    let bootstrap_servers = format!("127.0.0.1:{broker}");
    wait_for_broker(&bootstrap_servers).await;

    let schema = test_schema();
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .create()
        .unwrap();

    for ts in 1..=5_i64 {
        let payload = make_payload(&schema, "A", ts);
        let _ = producer
            .send(
                FutureRecord::<(), _>::to("kafka_source_test").payload(&payload),
                Duration::from_secs(0),
            )
            .await
            .unwrap();
    }

    let mut source = build_source(bootstrap_servers.clone(), schema.clone(), KafkaOffsetSpec::Earliest);
    let ctx = RuntimeContext::new(
        "kafka_source".to_string().into(),
        0,
        1,
        None,
        None,
        None,
    );
    source.open(&ctx).await.unwrap();
    let _ = read_n(&mut source, 3).await;
    let snapshot = source.snapshot_position().await.unwrap();

    for ts in 6..=8_i64 {
        let payload = make_payload(&schema, "A", ts);
        let _ = producer
            .send(
                FutureRecord::<(), _>::to("kafka_source_test").payload(&payload),
                Duration::from_secs(0),
            )
            .await
            .unwrap();
    }

    let mut recovered = build_source(bootstrap_servers, schema, KafkaOffsetSpec::Earliest);
    recovered.restore_position(&snapshot).await.unwrap();
    recovered.open(&ctx).await.unwrap();
    let rows = read_n(&mut recovered, 5).await;

    let expected: Vec<i64> = (4..=8).collect();
    let got: Vec<i64> = rows.into_iter().map(|(_, ts)| ts).collect();
    assert_eq!(got, expected);
}
