//! Conservative data-envelope estimation **without** a prior meta read.
//!
//! ## Why
//!
//! Eval used to live-get meta, then snapshot/load tiles+raw. That two-step can tear
//! (acc @ E, data @ E+1) and costs an extra round-trip. Instead:
//!
//! 1. Estimate a **safe** `[from, to]` from watermarks / request times / window lengths
//! 2. One `snapshot_partition`: meta + `plan_coverage` (tiles interior, raw edges)
//! 3. Decide slide vs rebuild from the meta in that view; evaluate in memory
//!
//! Overfetch is almost entirely **extra tile keys** (raw edges stay < one min-gran).
//! Ingest does **not** use these helpers (meta + touched-tile RMW + atomic write).
//!
//! ## Formulas
//!
//! | Path | Left bound | Right bound |
//! |------|------------|-------------|
//! | WO advance | `wm − max_wl − pad` | `wm` (`advance_to`) |
//! | WRO rebuild-only | `min(T) − wl` | `max(T)` |
//! | WRO slide-capable | `min(T) − 2·wl` | `max(T)` |
//!
//! ### WRO `2·wl` (slide-capable)
//!
//! Slide is only valid when `processed ≥ T − wl` (window still overlaps acc).
//! Leave then starts at `processed − wl ≥ T − 2·wl`. Without reading `processed`,
//! `[T − 2·wl, T]` covers leave∪add; if meta later forces rebuild, the extra `+wl`
//! of tiles is unused overfetch.
//!
//! ### WO `pad`
//!
//! Hot path: `processed ≈` recent wm → `[wm − max_wl, wm]` is enough.
//! `pad` is usually `spec.lateness` (retention horizon): widens the left side so
//! moderate catch-up stays inside one read. Extreme cold catch-up may still
//! underfetch; callers widen once after meta (see [`cold_needed_from`]).

use crate::runtime::operators::window::cursor::Cursor;
use crate::runtime::operators::window::store::meta_store::KeyState;

/// Inclusive time envelope for one partition load.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Envelope {
    pub from: Cursor,
    pub to: Cursor,
}

impl Envelope {
    pub fn new(from: Cursor, to: Cursor) -> Self {
        Self { from, to }
    }
}

/// WO watermark advance — no meta.
///
/// - `max_wl`: max RANGE length among windows on this key
/// - `retention_pad_ms`: usually `spec.lateness`; `None` → `0` (hot-path sized)
pub fn estimate_wo_advance(
    advance_to: Cursor,
    max_wl: i64,
    retention_pad_ms: Option<i64>,
) -> Envelope {
    let pad = retention_pad_ms.unwrap_or(0).max(0);
    let left = advance_to
        .ts
        .saturating_sub(max_wl.max(0))
        .saturating_sub(pad);
    Envelope::new(Cursor::new(left, 0), advance_to)
}

/// Window start floor: `prev − wl`, else `first_ingested − wl`, else unbounded.
pub fn window_start_floor(
    prev: Option<Cursor>,
    first_ingested: Option<Cursor>,
    wl: i64,
) -> Option<i64> {
    let wl = wl.max(0);
    match prev {
        Some(p) => Some(p.ts.saturating_sub(wl)),
        None => first_ingested.map(|f| f.ts.saturating_sub(wl)),
    }
}

/// Lower bound needed after meta is known (`prev − wl` or cold `first_ingested − wl`).
/// Unbounded cold (no `first_ingested`) → `i64::MIN`.
pub fn cold_needed_from(key_state: &KeyState, max_wl: i64) -> i64 {
    window_start_floor(key_state.processed_pos, key_state.first_ingested, max_wl)
        .unwrap_or(i64::MIN)
}

/// One window’s contribution to a WRO envelope (spec-time; not runtime slide decision).
#[derive(Debug, Clone, Copy)]
pub struct WroWindowBound {
    pub wl: i64,
    /// [`crate::runtime::operators::window::eval::can_slide`] — retractable agg.
    pub slide_capable: bool,
}

/// WRO point batch — no meta. Union over windows; slide-capable use `2·wl`.
pub fn estimate_wro(point_ts: &[i64], windows: &[WroWindowBound]) -> Option<Envelope> {
    if point_ts.is_empty() || windows.is_empty() {
        return None;
    }
    let min_t = *point_ts.iter().min().expect("non-empty");
    let max_t = *point_ts.iter().max().expect("non-empty");

    let mut from_ts = i64::MAX;
    for w in windows {
        let wl = w.wl.max(0);
        let left = if w.slide_capable {
            min_t.saturating_sub(wl.saturating_mul(2))
        } else {
            min_t.saturating_sub(wl)
        };
        from_ts = from_ts.min(left);
    }
    if from_ts == i64::MAX {
        return None;
    }
    Some(Envelope::new(
        Cursor::new(from_ts, 0),
        Cursor::new(max_t, u64::MAX),
    ))
}
