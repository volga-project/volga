use std::sync::Arc;
use std::time::Duration;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use futures::{stream, StreamExt};

use crate::common::message::{Message, WatermarkMessage};
use crate::runtime::operators::operator::drain_ready_after;

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

async fn drain_first<S>(input: &mut futures::stream::Peekable<S>, max: usize) -> Vec<Message>
where
    S: futures::Stream<Item = Message> + Unpin,
{
    let first = input.next().await.expect("expected a data message");
    assert!(!first.is_control());
    drain_ready_after(first, input, max).await
}

#[tokio::test]
async fn does_not_block_waiting_to_fill_budget() {
    let mut input = stream::iter(vec![data(vec![1])])
        .chain(stream::pending())
        .peekable();
    let out = tokio::time::timeout(Duration::from_millis(200), drain_first(&mut input, 8))
        .await
        .expect("must not wait on pending input");
    assert_eq!(out.len(), 1);
}

#[tokio::test]
async fn stops_before_watermark() {
    let mut input = stream::iter(vec![data(vec![1]), data(vec![2]), wm(9)]).peekable();
    let out = drain_first(&mut input, 8).await;
    assert_eq!(out.len(), 2);
    match input.next().await {
        Some(Message::Watermark(w)) => assert_eq!(w.watermark_value, 9),
        other => panic!("next fetch should see the watermark, got {other:?}"),
    }
}

#[tokio::test]
async fn record_limit_leaves_over_budget_message() {
    let mut input = stream::iter(vec![data(vec![1, 2, 3]), data(vec![4, 5, 6])]).peekable();
    let out = drain_first(&mut input, 5).await;
    assert_eq!(out.len(), 1);
    let leftover = drain_first(&mut input, 5).await;
    assert_eq!(leftover[0].record_batch().num_rows(), 3);
}
