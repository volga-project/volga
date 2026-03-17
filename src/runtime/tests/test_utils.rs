use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Float64Array, StringArray, TimestampMillisecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

use crate::runtime::observability::snapshot_types::StreamTaskStatus;
use crate::runtime::worker::Worker;

pub fn create_window_input_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Millisecond, None), false),
        Field::new("value", DataType::Float64, false),
        Field::new("partition_key", DataType::Utf8, false),
    ]))
}

pub async fn wait_for_status(worker: &Worker, status: StreamTaskStatus, timeout: Duration) {
    let start = std::time::Instant::now();
    loop {
        let st = worker.get_state().await;
        if !st.task_statuses.is_empty() && st.all_tasks_have_status(status) {
            return;
        }
        if start.elapsed() > timeout {
            panic!("Timeout waiting for {:?}, state = {:?}", status, st.task_statuses);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[derive(Debug, Clone)]
pub struct WindowOutputRow {
    pub trace: Option<String>,
    pub upstream_vertex_id: Option<String>,
    pub upstream_task_index: Option<i32>,
    pub timestamp_ms: i64,
    pub partition_key: String,
    pub value: f64,
    pub sum: f64,
    pub cnt: i64,
    pub avg: f64,
}

pub fn parse_task_index_from_vertex_id(vertex_id: &str) -> Option<i32> {
    vertex_id.rsplit('_').next()?.parse::<i32>().ok()
}

pub fn window_rows_from_messages(messages: Vec<crate::common::message::Message>) -> Vec<WindowOutputRow> {
    let mut out = Vec::new();
    for msg in messages {
        let extras = msg.get_extras().unwrap_or_default();
        let trace = extras.get("trace").cloned();

        let upstream_vertex_id = msg.upstream_vertex_id();
        let upstream_task_index = upstream_vertex_id
            .as_deref()
            .and_then(parse_task_index_from_vertex_id);

        let batch = msg.record_batch();
        let ts = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("timestamp col");
        let val = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("value col");
        let key = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("key col");
        let sum = batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("sum col");
        let cnt = batch
            .column(4)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("cnt col");
        let avg = batch
            .column(5)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("avg col");

        for i in 0..batch.num_rows() {
            out.push(WindowOutputRow {
                trace: trace.clone(),
                upstream_vertex_id: upstream_vertex_id.clone(),
                upstream_task_index,
                timestamp_ms: ts.value(i),
                partition_key: key.value(i).to_string(),
                value: val.value(i),
                sum: sum.value(i),
                cnt: cnt.value(i),
                avg: avg.value(i),
            });
        }
    }
    out
}

