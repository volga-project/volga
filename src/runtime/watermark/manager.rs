use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Array, Int64Array, TimestampMillisecondArray};
use arrow::record_batch::RecordBatch;
use tokio::sync::Mutex;

use crate::common::message::{Message, WatermarkMessage};

use super::confg::{TimeHint, WatermarkAssignConfig};

#[derive(Debug, Default, Clone)]
struct UpstreamWatermarkProgress {
    max_event_time_seen_ms: u64,
    last_emitted_wm_ms: u64,
    dirty: bool,
    last_emit_at: Option<Instant>,
}

/// Stateful event-time watermark generator.
///
/// Progress is tracked **per upstream vertex id** (sources use the task's own id; map/preprocess
/// uses the message upstream id).
#[derive(Debug)]
pub struct WatermarkAssignerState {
    out_of_orderness_ms: u64,
    ts_column_name: String,
    emit_interval: Duration,
    per_upstream: HashMap<String, UpstreamWatermarkProgress>,
}

impl WatermarkAssignerState {
    pub fn new(cfg: WatermarkAssignConfig) -> Self {
        let TimeHint::ColumnName { name } = cfg.time_hint;
        Self {
            out_of_orderness_ms: cfg.out_of_orderness_ms,
            ts_column_name: name,
            emit_interval: cfg.emit_interval,
            per_upstream: HashMap::new(),
        }
    }

    fn candidate(progress: &UpstreamWatermarkProgress, out_of_orderness_ms: u64) -> u64 {
        progress
            .max_event_time_seen_ms
            .saturating_sub(out_of_orderness_ms)
    }

    fn is_due(progress: &UpstreamWatermarkProgress, interval: Duration, now: Instant) -> bool {
        if interval.is_zero() {
            return true;
        }
        match progress.last_emit_at {
            None => true,
            Some(t) => now.saturating_duration_since(t) >= interval,
        }
    }

    fn emit(
        progress: &mut UpstreamWatermarkProgress,
        upstream_vertex_id: &str,
        candidate: u64,
        now: Instant,
    ) -> WatermarkMessage {
        progress.last_emitted_wm_ms = candidate;
        progress.dirty = false;
        progress.last_emit_at = Some(now);
        WatermarkMessage::new(upstream_vertex_id.to_string(), candidate, None)
    }

    fn resolve_ts_column_index(&self, batch: &RecordBatch) -> usize {
        batch.schema().index_of(&self.ts_column_name).unwrap_or_else(|_| {
            panic!(
                "WatermarkAssign failed: missing column '{}' in schema {:?}",
                self.ts_column_name,
                batch.schema()
            )
        })
    }

    fn batch_max_ts_ms(&self, batch: &RecordBatch, ts_column_index: usize) -> Option<u64> {
        if batch.num_rows() == 0 {
            return None;
        }

        let col = batch.column(ts_column_index);
        if let Some(arr) = col.as_any().downcast_ref::<TimestampMillisecondArray>() {
            let mut max_v: Option<i64> = None;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    continue;
                }
                let v = arr.value(i);
                max_v = Some(max_v.map(|m| m.max(v)).unwrap_or(v));
            }
            return max_v.map(|v| {
                assert!(v >= 0, "Expected non-negative timestamp ms, got {}", v);
                v as u64
            });
        }

        if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
            let mut max_v: Option<i64> = None;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    continue;
                }
                let v = arr.value(i);
                max_v = Some(max_v.map(|m| m.max(v)).unwrap_or(v));
            }
            return max_v.map(|v| {
                assert!(v >= 0, "Expected non-negative timestamp ms, got {}", v);
                v as u64
            });
        }

        panic!(
            "WatermarkAssign failed: unsupported ts column type {:?} at index {}",
            col.data_type(),
            ts_column_index
        );
    }

    pub fn on_data_message(
        &mut self,
        upstream_vertex_id: &str,
        message: &Message,
    ) -> Option<WatermarkMessage> {
        self.on_data_message_at(upstream_vertex_id, message, Instant::now())
    }

    pub fn on_data_message_at(
        &mut self,
        upstream_vertex_id: &str,
        message: &Message,
        now: Instant,
    ) -> Option<WatermarkMessage> {
        let batch = message.record_batch();
        let ts_column_index = self.resolve_ts_column_index(batch);
        let Some(batch_max) = self.batch_max_ts_ms(batch, ts_column_index) else {
            return None;
        };

        let progress = self
            .per_upstream
            .entry(upstream_vertex_id.to_string())
            .or_default();

        progress.max_event_time_seen_ms = progress.max_event_time_seen_ms.max(batch_max);
        let candidate = Self::candidate(progress, self.out_of_orderness_ms);

        if candidate > progress.last_emitted_wm_ms {
            if Self::is_due(progress, self.emit_interval, now) {
                Some(Self::emit(progress, upstream_vertex_id, candidate, now))
            } else {
                progress.dirty = true;
                None
            }
        } else {
            None
        }
    }

    pub fn try_emit_due(&mut self, now: Instant) -> Vec<WatermarkMessage> {
        let interval = self.emit_interval;
        let ooo = self.out_of_orderness_ms;
        let mut out = Vec::new();
        for (upstream_id, progress) in self.per_upstream.iter_mut() {
            let candidate = Self::candidate(progress, ooo);
            if progress.dirty
                && candidate > progress.last_emitted_wm_ms
                && Self::is_due(progress, interval, now)
            {
                out.push(Self::emit(progress, upstream_id, candidate, now));
            }
        }
        out
    }

    /// Publish any advanced candidate regardless of interval (EOF / max watermark).
    pub fn flush(&mut self, now: Instant) -> Vec<WatermarkMessage> {
        let ooo = self.out_of_orderness_ms;
        let mut out = Vec::new();
        for (upstream_id, progress) in self.per_upstream.iter_mut() {
            let candidate = Self::candidate(progress, ooo);
            if candidate > progress.last_emitted_wm_ms {
                out.push(Self::emit(progress, upstream_id, candidate, now));
            }
        }
        out
    }

    pub fn next_emit_deadline(&self) -> Option<Instant> {
        if self.emit_interval.is_zero() {
            return None;
        }
        self.per_upstream
            .values()
            .filter(|p| p.dirty)
            .filter_map(|p| p.last_emit_at.map(|t| t + self.emit_interval))
            .min()
    }
}

#[derive(Debug)]
pub struct WatermarkManager {
    idle_timeout_ms: Option<u64>,
    upstream_vertices: Vec<String>,
    last_seen_processing_time: HashMap<String, Instant>,
    upstream_watermarks: Arc<Mutex<HashMap<String, u64>>>,
    current_watermark: Arc<AtomicU64>,
    assigner: Option<WatermarkAssignerState>,
}

impl WatermarkManager {
    pub fn new(
        watermark_assign: Option<WatermarkAssignConfig>,
        upstream_vertices: Vec<String>,
        upstream_watermarks: Arc<Mutex<HashMap<String, u64>>>,
        current_watermark: Arc<AtomicU64>,
    ) -> Self {
        let idle_timeout_ms = watermark_assign.as_ref().and_then(|cfg| cfg.idle_timeout_ms);
        let assigner = watermark_assign.map(WatermarkAssignerState::new);
        let last_seen_processing_time = upstream_vertices
            .iter()
            .map(|u| (u.clone(), Instant::now()))
            .collect();
        Self {
            idle_timeout_ms,
            upstream_vertices,
            last_seen_processing_time,
            upstream_watermarks,
            current_watermark,
            assigner,
        }
    }

    pub fn assigner_enabled(&self) -> bool {
        self.assigner.is_some()
    }

    pub fn on_data_message(&mut self, upstream_vertex_id: &str, message: &Message) -> Option<WatermarkMessage> {
        let now = Instant::now();
        self.last_seen_processing_time
            .insert(upstream_vertex_id.to_string(), now);
        self.assigner
            .as_mut()
            .and_then(|assigner| assigner.on_data_message_at(upstream_vertex_id, message, now))
    }

    pub fn try_emit_due(&mut self, now: Instant) -> Vec<WatermarkMessage> {
        self.assigner
            .as_mut()
            .map(|assigner| assigner.try_emit_due(now))
            .unwrap_or_default()
    }

    pub fn flush_pending(&mut self, now: Instant) -> Vec<WatermarkMessage> {
        self.assigner
            .as_mut()
            .map(|assigner| assigner.flush(now))
            .unwrap_or_default()
    }

    pub fn next_emit_deadline(&self) -> Option<Instant> {
        self.assigner
            .as_ref()
            .and_then(|assigner| assigner.next_emit_deadline())
    }

    pub async fn merge_watermark(&mut self, watermark: WatermarkMessage) -> Option<u64> {
        if let Some(upstream_id) = watermark.metadata.upstream_vertex_id.as_ref() {
            self.last_seen_processing_time
                .insert(upstream_id.clone(), Instant::now());
        }
        let active_upstreams = self.active_upstreams(Instant::now());
        advance_watermark_min(
            watermark,
            &active_upstreams,
            self.upstream_watermarks.clone(),
            self.current_watermark.clone(),
        )
        .await
    }

    fn active_upstreams(&self, now: Instant) -> Vec<String> {
        if let Some(timeout_ms) = self.idle_timeout_ms {
            let timeout = Duration::from_millis(timeout_ms);
            self.upstream_vertices
                .iter()
                .filter(|u| {
                    self.last_seen_processing_time
                        .get(*u)
                        .map(|t| now.duration_since(*t) <= timeout)
                        .unwrap_or(false)
                })
                .cloned()
                .collect()
        } else {
            self.upstream_vertices.clone()
        }
    }
}

/// Merge upstream watermarks by taking the minimum across all upstream vertices.
///
/// Returns `Some(new_watermark_value)` when the task watermark advanced, otherwise `None`.
pub async fn advance_watermark_min(
    watermark: WatermarkMessage,
    active_upstreams: &[String],
    upstream_watermarks: Arc<Mutex<HashMap<String, u64>>>,
    current_watermark: Arc<AtomicU64>,
) -> Option<u64> {
    // Sources (no incoming edges): just advance monotonically.
    if active_upstreams.is_empty() {
        let current_wm = current_watermark.load(Ordering::SeqCst);
        if watermark.watermark_value > current_wm {
            current_watermark.store(watermark.watermark_value, Ordering::SeqCst);
            return Some(watermark.watermark_value);
        }
        return None;
    }

    let upstream_id = watermark
        .metadata
        .upstream_vertex_id
        .clone()
        .unwrap_or_else(|| panic!("Watermark must have upstream_vertex_id set: {:?}", watermark));

    let mut upstream_wms = upstream_watermarks.lock().await;
    if let Some(&prev) = upstream_wms.get(&upstream_id) {
        assert!(
            watermark.watermark_value >= prev,
            "Watermark must not decrease: received {} but previous was {} from upstream {}",
            watermark.watermark_value,
            prev,
            upstream_id
        );
    }
    upstream_wms.insert(upstream_id, watermark.watermark_value);

    if !active_upstreams
        .iter()
        .all(|u| upstream_wms.contains_key(u))
    {
        return None;
    }

    let min_watermark = active_upstreams
        .iter()
        .map(|u| *upstream_wms.get(u).expect("missing upstream watermark"))
        .min()
        .unwrap_or(0);

    let current_wm = current_watermark.load(Ordering::SeqCst);
    if min_watermark > current_wm {
        current_watermark.store(min_watermark, Ordering::SeqCst);
        Some(min_watermark)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use futures::stream;
    use futures::StreamExt;

        use crate::runtime::observability::snapshot_types::StreamTaskStatus;
        use crate::runtime::stream_task::{CheckpointAligner, StreamTask};
    use crate::transport::transport_client::DataReaderControl;
    use std::sync::atomic::AtomicU8;
    use std::time::{Duration, Instant};

    #[test]
    fn assigner_tracks_per_upstream_independently() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("ts_ms", DataType::Int64, false),
        ]));
        let b = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["A"])) as ArrayRef,
                Arc::new(Int64Array::from(vec![100_i64])) as ArrayRef,
            ],
        )
        .unwrap();
        let msg = Message::new(None, b, None, None);

        let cfg = WatermarkAssignConfig {
            out_of_orderness_ms: 0,
            time_hint: TimeHint::ColumnName {
                name: "ts_ms".to_string(),
            },
            idle_timeout_ms: Some(WatermarkAssignConfig::DEFAULT_IDLE_TIMEOUT_MS),
            emit_interval: Duration::ZERO,
        };
        let mut assigner = WatermarkAssignerState::new(cfg);

        let wm1 = assigner.on_data_message("upA", &msg).unwrap();
        assert_eq!(wm1.metadata.upstream_vertex_id.as_deref(), Some("upA"));
        assert_eq!(wm1.watermark_value, 100);

        let wm2 = assigner.on_data_message("upB", &msg).unwrap();
        assert_eq!(wm2.metadata.upstream_vertex_id.as_deref(), Some("upB"));
        assert_eq!(wm2.watermark_value, 100);
    }

    fn ts_message(ts_ms: i64) -> Message {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("ts_ms", DataType::Int64, false),
        ]));
        let b = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["A"])) as ArrayRef,
                Arc::new(Int64Array::from(vec![ts_ms])) as ArrayRef,
            ],
        )
        .unwrap();
        Message::new(None, b, None, None)
    }

    fn assigner_with_interval(interval: Duration) -> WatermarkAssignerState {
        WatermarkAssignerState::new(
            WatermarkAssignConfig::new(
                0,
                TimeHint::ColumnName {
                    name: "ts_ms".to_string(),
                },
            )
            .with_emit_interval(interval),
        )
    }

    #[test]
    fn zero_interval_emits_on_every_advance() {
        let mut assigner = assigner_with_interval(Duration::ZERO);
        let t0 = Instant::now();
        let wm1 = assigner
            .on_data_message_at("u", &ts_message(100), t0)
            .unwrap();
        assert_eq!(wm1.watermark_value, 100);
        let wm2 = assigner
            .on_data_message_at("u", &ts_message(150), t0)
            .unwrap();
        assert_eq!(wm2.watermark_value, 150);
    }

    #[test]
    fn coalesce_holds_until_interval() {
        let mut assigner = assigner_with_interval(Duration::from_millis(200));
        let t0 = Instant::now();
        let first = assigner
            .on_data_message_at("u", &ts_message(100), t0)
            .unwrap();
        assert_eq!(first.watermark_value, 100);

        let held = assigner.on_data_message_at(
            "u",
            &ts_message(150),
            t0 + Duration::from_millis(10),
        );
        assert!(held.is_none());
        assert!(assigner
            .try_emit_due(t0 + Duration::from_millis(10))
            .is_empty());

        let due = assigner.try_emit_due(t0 + Duration::from_millis(200));
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].watermark_value, 150);
    }

    #[test]
    fn timer_does_not_emit_when_not_dirty() {
        let mut assigner = assigner_with_interval(Duration::from_millis(50));
        let t0 = Instant::now();
        assert!(assigner
            .on_data_message_at("u", &ts_message(100), t0)
            .is_some());
        assert!(assigner
            .try_emit_due(t0 + Duration::from_millis(50))
            .is_empty());
        assert!(assigner.next_emit_deadline().is_none());
    }

    #[test]
    fn flush_emits_dirty_before_interval() {
        let mut assigner = assigner_with_interval(Duration::from_millis(200));
        let t0 = Instant::now();
        assigner
            .on_data_message_at("u", &ts_message(100), t0)
            .unwrap();
        assert!(assigner
            .on_data_message_at("u", &ts_message(180), t0 + Duration::from_millis(5))
            .is_none());
        let flushed = assigner.flush(t0 + Duration::from_millis(5));
        assert_eq!(flushed.len(), 1);
        assert_eq!(flushed[0].watermark_value, 180);
    }

    #[test]
    fn data_path_emits_when_interval_elapsed() {
        let mut assigner = assigner_with_interval(Duration::from_millis(200));
        let t0 = Instant::now();
        assigner
            .on_data_message_at("u", &ts_message(100), t0)
            .unwrap();
        assert!(assigner
            .on_data_message_at("u", &ts_message(120), t0 + Duration::from_millis(10))
            .is_none());
        let late = assigner
            .on_data_message_at("u", &ts_message(300), t0 + Duration::from_millis(200))
            .unwrap();
        assert_eq!(late.watermark_value, 300);
    }

    #[tokio::test]
    async fn stream_task_preprocess_injects_watermarks_after_data() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("ts_ms", DataType::Int64, false),
        ]));

        let b1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["A", "A"])) as ArrayRef,
                Arc::new(Int64Array::from(vec![100_i64, 120_i64])) as ArrayRef,
            ],
        )
        .unwrap();
        let b2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["A"])) as ArrayRef,
                Arc::new(Int64Array::from(vec![250_i64])) as ArrayRef,
            ],
        )
        .unwrap();

        let input = Box::pin(stream::iter(vec![
            Message::new(Some("u0".to_string()), b1, None, None),
            Message::new(Some("u0".to_string()), b2, None, None),
        ]));

        let cfg = WatermarkAssignConfig::new(
            0,
            TimeHint::ColumnName {
                name: "ts_ms".to_string(),
            },
        );

        let mut out = StreamTask::create_preprocessed_input_stream(
            input,
            Arc::<str>::from("v0"),
            Some(cfg),
            vec!["u0".to_string()],
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU8::new(StreamTaskStatus::Running as u8)),
            CheckpointAligner::new(&["u0".to_string()], DataReaderControl::empty_for_test()),
            None,
            0,
        );

        let mut seen = Vec::new();
        while let Some(m) = out.next().await {
            seen.push(m);
        }

        assert!(matches!(seen[0], Message::Regular(_)));
        assert!(matches!(seen[1], Message::Watermark(_)));
        assert!(matches!(seen[2], Message::Regular(_)));
        assert!(matches!(seen[3], Message::Watermark(_)));

        let wm1 = match &seen[1] {
            Message::Watermark(w) => w.watermark_value,
            _ => unreachable!(),
        };
        let wm2 = match &seen[3] {
            Message::Watermark(w) => w.watermark_value,
            _ => unreachable!(),
        };

        assert_eq!(wm1, 120);
        assert_eq!(wm2, 250);
    }

    #[tokio::test]
    async fn stream_task_preprocess_is_per_upstream_and_min_merged() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("ts_ms", DataType::Int64, false),
        ]));

        let b_u0 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["A"])) as ArrayRef,
                Arc::new(Int64Array::from(vec![100_i64])) as ArrayRef,
            ],
        )
        .unwrap();
        let b_u1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["B"])) as ArrayRef,
                Arc::new(Int64Array::from(vec![50_i64])) as ArrayRef,
            ],
        )
        .unwrap();

        let m0 = Message::new(Some("u0".to_string()), b_u0, None, None);
        let m1 = Message::new(Some("u1".to_string()), b_u1, None, None);

        let input = Box::pin(stream::iter(vec![m0, m1]));
        let cfg = WatermarkAssignConfig::new(
            0,
            TimeHint::ColumnName {
                name: "ts_ms".to_string(),
            },
        );

        let mut out = StreamTask::create_preprocessed_input_stream(
            input,
            Arc::<str>::from("v0"),
            Some(cfg),
            vec!["u0".to_string(), "u1".to_string()],
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU8::new(StreamTaskStatus::Running as u8)),
            CheckpointAligner::new(
                &["u0".to_string(), "u1".to_string()],
                DataReaderControl::empty_for_test(),
            ),
            None,
            0,
        );

        let mut seen = Vec::new();
        while let Some(m) = out.next().await {
            seen.push(m);
        }

        assert!(matches!(seen[0], Message::Regular(_)));
        assert!(matches!(seen[1], Message::Regular(_)));
        assert!(matches!(seen[2], Message::Watermark(_)));

        let wm = match &seen[2] {
            Message::Watermark(w) => w.watermark_value,
            _ => unreachable!(),
        };
        assert_eq!(wm, 50);
    }

    #[tokio::test]
    async fn idle_upstream_is_excluded_from_min_merge() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("ts_ms", DataType::Int64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["A"])) as ArrayRef,
                Arc::new(Int64Array::from(vec![100_i64])) as ArrayRef,
            ],
        )
        .unwrap();

        let input = Box::pin(async_stream::stream! {
            tokio::time::sleep(Duration::from_millis(5)).await;
            yield Message::new(Some("u0".to_string()), batch, None, None);
        });

        let mut cfg = WatermarkAssignConfig::new(
            0,
            TimeHint::ColumnName {
                name: "ts_ms".to_string(),
            },
        );
        cfg.idle_timeout_ms = Some(1);

        let mut out = StreamTask::create_preprocessed_input_stream(
            input,
            Arc::<str>::from("v0"),
            Some(cfg),
            vec!["u0".to_string(), "u1".to_string()],
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU8::new(StreamTaskStatus::Running as u8)),
            CheckpointAligner::new(
                &["u0".to_string(), "u1".to_string()],
                DataReaderControl::empty_for_test(),
            ),
            None,
            0,
        );

        let mut seen = Vec::new();
        while let Some(m) = out.next().await {
            seen.push(m);
        }

        assert!(matches!(seen[0], Message::Regular(_)));
        assert!(matches!(seen[1], Message::Watermark(_)));
    }

    #[tokio::test]
    async fn idle_timeout_disabled_blocks_min_merge() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("ts_ms", DataType::Int64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["A"])) as ArrayRef,
                Arc::new(Int64Array::from(vec![100_i64])) as ArrayRef,
            ],
        )
        .unwrap();

        let input = Box::pin(async_stream::stream! {
            tokio::time::sleep(Duration::from_millis(5)).await;
            yield Message::new(Some("u0".to_string()), batch, None, None);
        });

        let mut cfg = WatermarkAssignConfig::new(
            0,
            TimeHint::ColumnName {
                name: "ts_ms".to_string(),
            },
        );
        cfg.idle_timeout_ms = None;

        let mut out = StreamTask::create_preprocessed_input_stream(
            input,
            Arc::<str>::from("v0"),
            Some(cfg),
            vec!["u0".to_string(), "u1".to_string()],
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU8::new(StreamTaskStatus::Running as u8)),
            CheckpointAligner::new(
                &["u0".to_string(), "u1".to_string()],
                DataReaderControl::empty_for_test(),
            ),
            None,
            0,
        );

        let mut seen = Vec::new();
        while let Some(m) = out.next().await {
            seen.push(m);
        }

        assert!(matches!(seen[0], Message::Regular(_)));
        assert_eq!(seen.len(), 1);
    }

    fn ts_up(upstream: &str, ts_ms: i64) -> Message {
        let mut m = ts_message(ts_ms);
        m.set_upstream_vertex_id(upstream.to_string());
        m
    }

    fn preprocess_assign(
        input: crate::runtime::operators::operator::MessageStream,
        interval: Duration,
    ) -> crate::runtime::operators::operator::MessageStream {
        let cfg = WatermarkAssignConfig::new(
            0,
            TimeHint::ColumnName {
                name: "ts_ms".to_string(),
            },
        )
        .with_emit_interval(interval);
        StreamTask::create_preprocessed_input_stream(
            input,
            Arc::<str>::from("v0"),
            Some(cfg),
            vec!["u0".to_string()],
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU8::new(StreamTaskStatus::Running as u8)),
            CheckpointAligner::new(&["u0".to_string()], DataReaderControl::empty_for_test()),
            None,
            0,
        )
    }

    #[tokio::test]
    async fn stream_task_preprocess_timer_emits_held_watermark_without_next_record() {
        let interval = Duration::from_millis(50);
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let mut out = preprocess_assign(
            Box::pin(tokio_stream::wrappers::UnboundedReceiverStream::new(rx)),
            interval,
        );

        tx.send(ts_up("u0", 100)).unwrap();
        assert!(matches!(out.next().await, Some(Message::Regular(_))));
        match out.next().await {
            Some(Message::Watermark(w)) => assert_eq!(w.watermark_value, 100),
            other => panic!("expected first watermark, got {other:?}"),
        }

        tx.send(ts_up("u0", 150)).unwrap();
        assert!(matches!(out.next().await, Some(Message::Regular(_))));

        let held = tokio::time::timeout(Duration::from_millis(500), out.next())
            .await
            .expect("timer should emit held watermark with no following record")
            .expect("stream ended");
        match held {
            Message::Watermark(w) => assert_eq!(w.watermark_value, 150),
            other => panic!("expected coalesced watermark, got {other:?}"),
        }
        drop(tx);
    }

    #[tokio::test]
    async fn stream_task_preprocess_flushes_held_watermark_before_aligned_barrier() {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let mut out = preprocess_assign(
            Box::pin(tokio_stream::wrappers::UnboundedReceiverStream::new(rx)),
            Duration::from_secs(30),
        );

        tx.send(ts_up("u0", 100)).unwrap();
        assert!(matches!(out.next().await, Some(Message::Regular(_))));
        match out.next().await {
            Some(Message::Watermark(w)) => assert_eq!(w.watermark_value, 100),
            other => panic!("expected first watermark, got {other:?}"),
        }

        tx.send(ts_up("u0", 180)).unwrap();
        assert!(matches!(out.next().await, Some(Message::Regular(_))));

        tx.send(Message::CheckpointBarrier(
            crate::common::message::CheckpointBarrierMessage::new(
                "u0".to_string(),
                1,
                0,
                None,
            ),
        ))
        .unwrap();

        match out.next().await {
            Some(Message::Watermark(w)) => assert_eq!(w.watermark_value, 180),
            other => panic!("expected flushed watermark before barrier, got {other:?}"),
        }
        assert!(
            matches!(out.next().await, Some(Message::CheckpointBarrier(_))),
            "aligned barrier should follow flushed watermark"
        );
        drop(tx);
    }
}

