use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arrow::array::{Array, Int64Array, TimestampMillisecondArray};
use arrow::record_batch::RecordBatch;
use tokio::sync::Mutex;

use crate::common::message::{Message, WatermarkMessage};
use crate::runtime::operators::operator::OperatorConfig;

use datafusion::physical_plan::expressions::Column;

use super::confg::{TimeHint, WatermarkAssignConfig};

#[derive(Debug, Clone)]
enum TsColumnSpec {
    Index(usize),
    ColumnName(String),
}

#[derive(Debug, Default, Clone)]
struct UpstreamWatermarkProgress {
    max_event_time_seen_ms: u64,
    last_emitted_wm_ms: u64,
}

/// Stateful event-time watermark generator.
///
/// Progress is tracked **per upstream vertex id** so we can attach this generator to a downstream
/// task (e.g. Window) while still simulating independent upstream watermark production.
#[derive(Debug)]
pub struct WatermarkAssignerState {
    out_of_orderness_ms: u64,
    ts_column: TsColumnSpec,
    per_upstream: HashMap<String, UpstreamWatermarkProgress>,
}

impl WatermarkAssignerState {
    pub fn new(cfg: WatermarkAssignConfig, operator_config: Option<&OperatorConfig>) -> Self {
        let ts_column = match cfg.time_hint {
            Some(TimeHint::WindowOrderByColumn) => TsColumnSpec::Index(
                Self::derive_window_ts_column_index(
                    operator_config.expect("WindowOrderByColumn requires operator_config"),
                ),
            ),
            Some(TimeHint::ColumnName { name }) => TsColumnSpec::ColumnName(name),
            None => {
                // Auto-resolve only for Window vertices.
                TsColumnSpec::Index(Self::derive_window_ts_column_index(
                    operator_config.expect("Auto-resolve requires operator_config"),
                ))
            }
        };

        Self {
            out_of_orderness_ms: cfg.out_of_orderness_ms,
            ts_column,
            per_upstream: HashMap::new(),
        }
    }

    fn derive_window_ts_column_index(operator_config: &OperatorConfig) -> usize {
        let OperatorConfig::WindowConfig(window_cfg) = operator_config else {
            panic!("WatermarkAssign auto-resolve requires Window vertex or explicit time hint");
        };
        window_cfg.window_exec.window_expr()[0]
            .order_by()[0]
            .expr
            .as_any()
            .downcast_ref::<Column>()
            .expect("Expected Column expression in Window ORDER BY")
            .index()
    }

    fn resolve_ts_column_index(&self, batch: &RecordBatch) -> usize {
        match &self.ts_column {
            TsColumnSpec::Index(idx) => *idx,
            TsColumnSpec::ColumnName(name) => batch
                .schema()
                .index_of(name)
                .unwrap_or_else(|_| {
                    panic!(
                        "WatermarkAssign failed: missing column '{}' in schema {:?}",
                        name,
                        batch.schema()
                    )
                }),
        }
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
        let candidate = progress
            .max_event_time_seen_ms
            .saturating_sub(self.out_of_orderness_ms);

        if candidate > progress.last_emitted_wm_ms {
            progress.last_emitted_wm_ms = candidate;
            Some(WatermarkMessage::new(
                upstream_vertex_id.to_string(),
                candidate,
                None,
            ))
        } else {
            None
        }
    }
}

/// Merge upstream watermarks by taking the minimum across all upstream vertices.
///
/// Returns `Some(new_watermark_value)` when the task watermark advanced, otherwise `None`.
pub async fn advance_watermark_min(
    watermark: WatermarkMessage,
    upstream_vertices: &[String],
    upstream_watermarks: Arc<Mutex<HashMap<String, u64>>>,
    current_watermark: Arc<AtomicU64>,
) -> Option<u64> {
    // Sources (no incoming edges): just advance monotonically.
    if upstream_vertices.is_empty() {
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

    if !upstream_vertices
        .iter()
        .all(|u| upstream_wms.contains_key(u))
    {
        return None;
    }

    let min_watermark = upstream_vertices
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

    use crate::runtime::stream_task::{CheckpointAligner, StreamTask, StreamTaskStatus};
    use crate::transport::transport_client::DataReaderControl;
    use std::sync::atomic::AtomicU8;

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
            time_hint: Some(TimeHint::ColumnName {
                name: "ts_ms".to_string(),
            }),
        };
        let mut assigner = WatermarkAssignerState::new(cfg, None);

        let wm1 = assigner.on_data_message("upA", &msg).unwrap();
        assert_eq!(wm1.metadata.upstream_vertex_id.as_deref(), Some("upA"));
        assert_eq!(wm1.watermark_value, 100);

        let wm2 = assigner.on_data_message("upB", &msg).unwrap();
        assert_eq!(wm2.metadata.upstream_vertex_id.as_deref(), Some("upB"));
        assert_eq!(wm2.watermark_value, 100);
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
            Some(TimeHint::ColumnName {
                name: "ts_ms".to_string(),
            }),
        );

        let mut out = StreamTask::create_preprocessed_input_stream(
            input,
            Arc::<str>::from("v0"),
            OperatorConfig::MapConfig(crate::runtime::functions::map::MapFunction::new_custom(
                crate::common::test_utils::IdentityMapFunction,
            )),
            Some(cfg),
            vec!["u0".to_string()],
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU8::new(StreamTaskStatus::Running as u8)),
            CheckpointAligner::new(&["u0".to_string()], DataReaderControl::empty_for_test()),
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
            Some(TimeHint::ColumnName {
                name: "ts_ms".to_string(),
            }),
        );

        let mut out = StreamTask::create_preprocessed_input_stream(
            input,
            Arc::<str>::from("v0"),
            OperatorConfig::MapConfig(crate::runtime::functions::map::MapFunction::new_custom(
                crate::common::test_utils::IdentityMapFunction,
            )),
            Some(cfg),
            vec!["u0".to_string(), "u1".to_string()],
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU8::new(StreamTaskStatus::Running as u8)),
            CheckpointAligner::new(
                &["u0".to_string(), "u1".to_string()],
                DataReaderControl::empty_for_test(),
            ),
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
}

