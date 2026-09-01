use std::sync::Arc;
use std::time::Duration;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use futures::{stream, StreamExt};

use crate::common::message::{Message, WatermarkMessage};
use crate::runtime::operators::operator::{
    NextInputs, OperatorBase, OperatorConfig, OperatorTrait,
};
use crate::runtime::operators::source::source_operator::{SourceConfig, VectorSourceConfig};

fn int_batch(rows: Vec<i64>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(rows))]).unwrap()
}

fn data(rows: Vec<i64>) -> Message {
    Message::new(None, int_batch(rows), None, None)
}

fn wm(v: u64) -> Message {
    Message::Watermark(WatermarkMessage::new("src".to_string(), v, None))
}

fn data_len(out: &NextInputs) -> usize {
    match out {
        NextInputs::Data(msgs) => msgs.len(),
        other => panic!("expected Data, got {other:?}"),
    }
}

fn base_with_input(
    stream: impl futures::Stream<Item = Message> + Send + Sync + 'static,
) -> OperatorBase {
    let mut base = OperatorBase::new(OperatorConfig::SourceConfig(
        SourceConfig::VectorSourceConfig(VectorSourceConfig::new(vec![])),
    ));
    base.set_input(Some(Box::pin(stream)));
    base
}

#[tokio::test]
async fn does_not_block_waiting_to_fill_budget() {
    let mut base = base_with_input(stream::iter(vec![data(vec![1])]).chain(stream::pending()));
    let out = tokio::time::timeout(Duration::from_millis(200), base.next_inputs(8))
        .await
        .expect("must not wait on pending input");
    assert_eq!(data_len(&out), 1);
}

#[tokio::test]
async fn stops_before_watermark() {
    let mut base = base_with_input(stream::iter(vec![data(vec![1]), data(vec![2]), wm(9)]));
    let out = base.next_inputs(8).await;
    assert_eq!(data_len(&out), 2);
    match base.next_inputs(8).await {
        NextInputs::Control(Message::Watermark(w)) => assert_eq!(w.watermark_value, 9),
        other => panic!("next fetch should see the watermark, got {other:?}"),
    }
}

#[tokio::test]
async fn record_limit_leaves_over_budget_message() {
    let mut base = base_with_input(stream::iter(vec![data(vec![1, 2, 3]), data(vec![4, 5, 6])]));
    let out = base.next_inputs(5).await;
    assert_eq!(data_len(&out), 1);
    match base.next_inputs(5).await {
        NextInputs::Data(msgs) => assert_eq!(msgs[0].record_batch().num_rows(), 3),
        other => panic!("expected leftover data, got {other:?}"),
    }
}
