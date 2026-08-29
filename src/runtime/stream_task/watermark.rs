use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Array, Int64Array, TimestampMillisecondArray};
use arrow::record_batch::RecordBatch;
use tokio::sync::Mutex;

use crate::common::message::{Message, WatermarkMessage};

use crate::runtime::watermark::{TimeHint, WatermarkAssignConfig};

#[derive(Debug, Default, Clone)]
struct UpstreamWatermarkProgress {
    max_event_time_seen_ms: u64,
    last_emitted_wm_ms: u64,
    last_emit_at: Option<Instant>,
}

/// Stateful event-time watermark generator.
///
/// Progress is tracked **per upstream vertex id** (sources use the task's own id;
/// processors use the message upstream id).
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
        progress.last_emit_at = Some(now);
        WatermarkMessage::new(upstream_vertex_id.to_string(), candidate, None)
    }

    fn unpublished(progress: &UpstreamWatermarkProgress, out_of_orderness_ms: u64) -> bool {
        Self::candidate(progress, out_of_orderness_ms) > progress.last_emitted_wm_ms
    }

    fn maybe_emit(
        progress: &mut UpstreamWatermarkProgress,
        upstream_vertex_id: &str,
        out_of_orderness_ms: u64,
        interval: Duration,
        now: Instant,
        force: bool,
    ) -> Option<WatermarkMessage> {
        let candidate = Self::candidate(progress, out_of_orderness_ms);
        if candidate > progress.last_emitted_wm_ms && (force || Self::is_due(progress, interval, now))
        {
            Some(Self::emit(progress, upstream_vertex_id, candidate, now))
        } else {
            None
        }
    }

    fn emit_from_upstreams(&mut self, now: Instant, force: bool) -> Vec<WatermarkMessage> {
        let interval = self.emit_interval;
        let ooo = self.out_of_orderness_ms;
        let mut out = Vec::new();
        for (upstream_id, progress) in self.per_upstream.iter_mut() {
            if let Some(wm) = Self::maybe_emit(progress, upstream_id, ooo, interval, now, force) {
                out.push(wm);
            }
        }
        out
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
        Self::maybe_emit(
            progress,
            upstream_vertex_id,
            self.out_of_orderness_ms,
            self.emit_interval,
            now,
            false,
        )
    }

    pub fn try_emit_due(&mut self, now: Instant) -> Vec<WatermarkMessage> {
        self.emit_from_upstreams(now, false)
    }

    /// Publish any advanced candidate regardless of interval (EOF / barrier / max).
    pub fn flush(&mut self, now: Instant) -> Vec<WatermarkMessage> {
        self.emit_from_upstreams(now, true)
    }

    pub fn next_emit_deadline(&self) -> Option<Instant> {
        if self.emit_interval.is_zero() {
            return None;
        }
        let ooo = self.out_of_orderness_ms;
        self.per_upstream
            .values()
            .filter(|p| Self::unpublished(p, ooo))
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

    pub fn try_emit_due(&mut self) -> Vec<WatermarkMessage> {
        let now = Instant::now();
        self.assigner
            .as_mut()
            .map(|assigner| assigner.try_emit_due(now))
            .unwrap_or_default()
    }

    pub fn flush_pending(&mut self) -> Vec<WatermarkMessage> {
        let now = Instant::now();
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

