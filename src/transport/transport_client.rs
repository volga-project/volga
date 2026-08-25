use anyhow::Result;
use metrics::gauge;
use std::collections::HashMap;
use crate::{common::message::Message, runtime::{health::{WorkerFatalReason, WorkerHealth}, metrics::{MetricsLabels, LABEL_PIPELINE_ID, LABEL_SOURCE_TASK_ID, LABEL_TARGET_TASK_ID, LABEL_TASK_ID, LABEL_WORKER_ID, METRIC_STREAM_TASK_BACKPRESSURE_RATIO, METRIC_STREAM_TASK_RX_QUEUED_RECORDS, METRIC_STREAM_TASK_TX_QUEUE_REM, METRIC_STREAM_TASK_TX_QUEUE_SIZE}, operators::operator::MessageStream}, transport::{batch_channel::{BatchReceiver, BatchSender, BackpressureTracker}, channel::Channel}};
use std::time::Duration;
use tokio::{sync::mpsc::error::SendError, task::JoinHandle, time};
use tokio::sync::Notify;
use futures::stream;
use std::sync::{Arc, atomic::{AtomicBool, AtomicU32, Ordering}};
use crate::runtime::VertexId;

// pub type MessageStream = Pin<Box<dyn Stream<Item = Message> + Send>>;

#[derive(Debug)]
pub struct TransportClientConfig {
    pub vertex_id: VertexId,
    pub reader_receivers: Option<HashMap<String, BatchReceiver>>,
    pub writer_senders: Option<HashMap<String, BatchSender>>,
    pub metrics_labels: Option<MetricsLabels>,
}

impl TransportClientConfig {
    pub fn new(vertex_id: VertexId) -> Self {
        Self {
            vertex_id,
            reader_receivers: None,
            writer_senders: None,
            metrics_labels: None,
        }
    }

    pub fn add_reader_receiver(&mut self, channel_id: String, receiver: BatchReceiver) {
        if self.reader_receivers.is_none() {
            self.reader_receivers = Some(HashMap::new());
        }
        self.reader_receivers.as_mut().unwrap().insert(channel_id, receiver);
    }

    pub fn add_writer_sender(&mut self, channel_id: String, sender: BatchSender) {
        if self.writer_senders.is_none() {
            self.writer_senders = Some(HashMap::new());
        }
        self.writer_senders.as_mut().unwrap().insert(channel_id, sender);
    }

    pub fn set_metrics_labels(&mut self, labels: MetricsLabels) {
        self.metrics_labels = Some(labels);
    }
}

/// Aborts the dest-side queue ticker when the stream task ends (close / panic).
pub(crate) struct RxQueueTicker(JoinHandle<()>);

impl Drop for RxQueueTicker {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[derive(Debug)]
pub struct DataReader {
    vertex_id: VertexId,
    receivers: HashMap<String, BatchReceiver>,
}

#[derive(Debug)]
struct UpstreamGate {
    enabled: AtomicBool,
    notify: Notify,
}

impl UpstreamGate {
    fn new() -> Self {
        Self {
            enabled: AtomicBool::new(true),
            notify: Notify::new(),
        }
    }

    fn block(&self) {
        self.enabled.store(false, Ordering::Release);
    }

    fn unblock(&self) {
        self.enabled.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }
}

#[derive(Debug, Clone)]
pub struct DataReaderControl {
    // upstream_vertex_id -> gate shared by all channels from that upstream
    gates: HashMap<String, Arc<UpstreamGate>>,
}

impl DataReaderControl {
    pub fn block_upstream(&self, upstream_vertex_id: &str) {
        if let Some(gate) = self.gates.get(upstream_vertex_id) {
            gate.block();
        }
    }

    pub fn unblock_upstream(&self, upstream_vertex_id: &str) {
        if let Some(gate) = self.gates.get(upstream_vertex_id) {
            gate.unblock();
        }
    }

    pub fn unblock_all(&self) {
        for gate in self.gates.values() {
            gate.unblock();
        }
    }

    #[cfg(test)]
    pub fn empty_for_test() -> Self {
        Self { gates: HashMap::new() }
    }
}

impl DataReader {
    pub fn new(vertex_id: VertexId, receivers: HashMap<String, BatchReceiver>) -> Self {
        Self {
            vertex_id,
            receivers,
        }
    }

    pub fn queued_by_source(&self) -> Vec<(String, Arc<AtomicU32>)> {
        self.receivers
            .iter()
            .map(|(channel_id, receiver)| {
                let source_task_id = channel_id
                    .split("_to_")
                    .next()
                    .unwrap_or(channel_id)
                    .to_string();
                (source_task_id, receiver.queued_records_handle())
            })
            .collect()
    }

    /// 1s dest-side sample so depths stay live while the operator is in `process` / blocked on send.
    pub(crate) fn spawn_rx_queue_ticker(&self, labels: Option<MetricsLabels>) -> RxQueueTicker {
        let channels = self.queued_by_source();
        let vertex_id = self.vertex_id.clone();
        RxQueueTicker(tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(1));
            interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                for (source_task_id, queued) in &channels {
                    let value = queued.load(Ordering::Relaxed) as f64;
                    if let Some(labels) = &labels {
                        gauge!(
                            METRIC_STREAM_TASK_RX_QUEUED_RECORDS,
                            LABEL_TASK_ID => vertex_id.clone(),
                            LABEL_SOURCE_TASK_ID => source_task_id.clone(),
                            LABEL_PIPELINE_ID => labels.pipeline_id.clone(),
                            LABEL_WORKER_ID => labels.worker_id.clone(),
                        )
                        .set(value);
                    } else {
                        gauge!(
                            METRIC_STREAM_TASK_RX_QUEUED_RECORDS,
                            LABEL_TASK_ID => vertex_id.clone(),
                            LABEL_SOURCE_TASK_ID => source_task_id.clone(),
                        )
                        .set(value);
                    }
                }
            }
        }))
    }

    pub fn message_stream(self) -> MessageStream {
        // Convert each BatchReceiver into a boxed Stream using unfold
        let receiver_streams: Vec<MessageStream> = self.receivers
            .into_iter()
            .map(|(_channel_id, receiver)| {
                // Convert BatchReceiver to Stream using unfold and box it for Unpin
                Box::pin(stream::unfold(receiver, |mut rx| async move {
                    match rx.recv().await {
                        Some(message) => Some((message, rx)),
                        None => None, // Channel closed
                    }
                })) as MessageStream
            })
            .collect();
        
        Box::pin(stream::select_all(receiver_streams))
    }

    pub fn message_stream_with_control(self) -> (MessageStream, DataReaderControl) {
        let mut gates: HashMap<String, Arc<UpstreamGate>> = HashMap::new();

        // Convert each BatchReceiver into a gated stream and then select_all.
        // No background tasks: gating happens inside the stream.
        let receiver_streams: Vec<MessageStream> = self.receivers
            .into_iter()
            .map(|(channel_id, receiver)| {
                // channel id is "{source}_to_{target}"
                let upstream_vertex_id = channel_id
                    .split("_to_")
                    .next()
                    .unwrap_or("")
                    .to_string();

                let gate = gates
                    .entry(upstream_vertex_id)
                    .or_insert_with(|| Arc::new(UpstreamGate::new()))
                    .clone();

                Box::pin(stream::unfold((receiver, gate), |(mut rx, gate)| async move {
                    loop {
                        while !gate.enabled.load(Ordering::Acquire) {
                            gate.notify.notified().await;
                        }

                        match rx.recv().await {
                            Some(message) => return Some((message, (rx, gate))),
                            None => return None, // Channel closed
                        }
                    }
                })) as MessageStream
            })
            .collect();

        (Box::pin(stream::select_all(receiver_streams)), DataReaderControl { gates })
    }
}

#[derive(Debug, Clone)]
pub struct DataWriter {
    pub vertex_id: VertexId,
    pub senders: HashMap<String, BatchSender>,
    metrics_labels: Option<MetricsLabels>,
    worker_health: Arc<WorkerHealth>,
    /// Task-scoped queue-wait clock (ratio gauges + task time BP share this write path).
    backpressure_tracker: Option<Arc<BackpressureTracker>>,
}

impl DataWriter {
    pub fn new(
        vertex_id: VertexId,
        senders: HashMap<String, BatchSender>,
        metrics_labels: Option<MetricsLabels>,
        worker_health: Arc<WorkerHealth>,
    ) -> Self {
        Self {
            vertex_id,
            senders,
            metrics_labels,
            worker_health,
            backpressure_tracker: None,
        }
    }

    pub fn set_backpressure_tracker(&mut self, bp: Arc<BackpressureTracker>) {
        self.backpressure_tracker = Some(bp);
    }

    pub async fn start(&mut self) {}

    pub async fn flush_and_close(&mut self) -> Result<(), SendError<Message>> {
        Ok(())
    }

    /// Publish queue size / remaining / fill-ratio for one outbound channel.
    fn publish_queue_metrics(&self, sender: &BatchSender, target_vertex_id: &str) {
        let queue_size = sender.size();
        let queue_remaining = sender.capacity();
        let backpressure = sender.backpressure_ratio();
        if let Some(labels) = &self.metrics_labels {
            gauge!(
                METRIC_STREAM_TASK_TX_QUEUE_SIZE,
                LABEL_TASK_ID => self.vertex_id.clone(),
                LABEL_TARGET_TASK_ID => target_vertex_id.to_string(),
                LABEL_PIPELINE_ID => labels.pipeline_id.clone(),
                LABEL_WORKER_ID => labels.worker_id.clone()
            )
            .set(queue_size);
            gauge!(
                METRIC_STREAM_TASK_TX_QUEUE_REM,
                LABEL_TASK_ID => self.vertex_id.clone(),
                LABEL_TARGET_TASK_ID => target_vertex_id.to_string(),
                LABEL_PIPELINE_ID => labels.pipeline_id.clone(),
                LABEL_WORKER_ID => labels.worker_id.clone()
            )
            .set(queue_remaining);
            gauge!(
                METRIC_STREAM_TASK_BACKPRESSURE_RATIO,
                LABEL_TASK_ID => self.vertex_id.clone(),
                LABEL_TARGET_TASK_ID => target_vertex_id.to_string(),
                LABEL_PIPELINE_ID => labels.pipeline_id.clone(),
                LABEL_WORKER_ID => labels.worker_id.clone()
            )
            .set(backpressure);
        } else {
            gauge!(
                METRIC_STREAM_TASK_TX_QUEUE_SIZE,
                LABEL_TASK_ID => self.vertex_id.clone(),
                LABEL_TARGET_TASK_ID => target_vertex_id.to_string()
            )
            .set(queue_size);
            gauge!(
                METRIC_STREAM_TASK_TX_QUEUE_REM,
                LABEL_TASK_ID => self.vertex_id.clone(),
                LABEL_TARGET_TASK_ID => target_vertex_id.to_string()
            )
            .set(queue_remaining);
            gauge!(
                METRIC_STREAM_TASK_BACKPRESSURE_RATIO,
                LABEL_TASK_ID => self.vertex_id.clone(),
                LABEL_TARGET_TASK_ID => target_vertex_id.to_string()
            )
            .set(backpressure);
        }
    }

    /// Queue the message, waiting on the bound. `true` = queued; `false` = closed + fatal.
    pub async fn write_message(&mut self, channel: &Channel, message: &Message) -> bool {
        if self.senders.is_empty() {
            panic!("DataWriter {:?} no channels registered", self.vertex_id);
        }
        let channel_id = channel.get_channel_id();
        let Some(sender) = self.senders.get(&channel_id) else {
            panic!("DataWriter {:?} channel {} not found", self.vertex_id, channel_id);
        };
        let target_vertex_id = channel.get_target_vertex_id();
        let bp = self.backpressure_tracker.as_deref();
        // Same path as task time BP: sample queue, then send (waits record on `bp`).
        self.publish_queue_metrics(sender, &target_vertex_id);
        match sender.send(message.clone(), bp).await {
            Ok(()) => true,
            Err(_) => {
                self.worker_health.report_fatal(
                    WorkerFatalReason::TransportDisconnect,
                    format!(
                        "DataWriter {:?} channel {} closed",
                        self.vertex_id, channel_id
                    ),
                );
                false
            }
        }
    }

    pub fn get_queue_size_and_capacity(&self, channel_id: &str) -> Option<(u32, u32)> {
        self.senders.get(channel_id).map(|sender| (sender.size(), sender.capacity()))
    }
}

#[derive(Debug)]
pub struct TransportClient {
    vertex_id: VertexId,
    pub reader: Option<DataReader>,
    pub writer: Option<DataWriter>,
}

impl TransportClient {
    pub fn new(
        vertex_id: VertexId,
        config: TransportClientConfig,
        worker_health: Arc<WorkerHealth>,
    ) -> Self {
        let mut reader: Option<DataReader> = None;
        let mut writer: Option<DataWriter> = None;

        let TransportClientConfig { reader_receivers, writer_senders, metrics_labels, .. } = config;

        if let Some(receivers) = reader_receivers {
            reader = Some(DataReader::new(vertex_id.clone(), receivers));
        }
        if let Some(senders) = writer_senders {
            writer = Some(DataWriter::new(
                vertex_id.clone(),
                senders,
                metrics_labels,
                worker_health,
            ));
        }

        Self {
            vertex_id: vertex_id.clone(),
            reader,
            writer,
        }
    }

    pub fn vertex_id(&self) -> &str {
        self.vertex_id.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::message::WatermarkMessage;
    use crate::transport::batch_channel::batch_bounded_channel;
    use std::time::Duration;
    use tokio::time::sleep;

    fn wm(i: u64) -> Message {
        Message::Watermark(WatermarkMessage::new("t".to_string(), i, Some(0)))
    }

    fn writer_with_queue(queue: u32) -> (Channel, DataWriter, BatchReceiver, Arc<WorkerHealth>) {
        let channel = Channel::new_local_with_queue("w".to_string(), "r".to_string(), queue);
        let (tx, rx) = batch_bounded_channel(queue);
        let health = Arc::new(WorkerHealth::new());
        let writer = DataWriter::new(
            Arc::from("w"),
            HashMap::from([(channel.get_channel_id(), tx)]),
            None,
            health.clone(),
        );
        (channel, writer, rx, health)
    }

    #[tokio::test]
    async fn write_message_waits_until_queued() {
        let (channel, mut writer, mut rx, health) = writer_with_queue(1);
        assert!(writer.write_message(&channel, &wm(1)).await);

        let mut blocked_writer = writer.clone();
        let blocked_channel = channel.clone();
        let blocked = tokio::spawn(async move {
            blocked_writer.write_message(&blocked_channel, &wm(2)).await
        });

        sleep(Duration::from_millis(30)).await;
        assert!(
            !blocked.is_finished(),
            "send should wait while the queue is full"
        );

        assert!(rx.recv().await.is_some());
        assert!(
            blocked.await.expect("blocked send task panicked"),
            "send should complete after one recv"
        );
        assert!(health.last_fatal().is_none());
    }

    #[tokio::test]
    async fn write_message_false_and_fatal_on_closed() {
        let (channel, mut writer, rx, health) = writer_with_queue(2);
        drop(rx);
        assert!(!writer.write_message(&channel, &wm(1)).await);
        assert!(matches!(
            health.last_fatal().expect("expected fatal").reason,
            WorkerFatalReason::TransportDisconnect
        ));
    }

    #[tokio::test]
    async fn write_message_false_and_fatal_when_closed_while_waiting() {
        let (channel, mut writer, rx, health) = writer_with_queue(1);
        assert!(writer.write_message(&channel, &wm(1)).await);

        let mut blocked_writer = writer.clone();
        let blocked_channel = channel.clone();
        let blocked = tokio::spawn(async move {
            blocked_writer.write_message(&blocked_channel, &wm(2)).await
        });

        sleep(Duration::from_millis(30)).await;
        assert!(!blocked.is_finished());
        drop(rx);
        assert!(
            !blocked.await.expect("blocked send task panicked"),
            "closed channel must return false"
        );
        assert!(matches!(
            health.last_fatal().expect("expected fatal").reason,
            WorkerFatalReason::TransportDisconnect
        ));
    }
}