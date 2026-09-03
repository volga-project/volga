use datafusion::logical_expr::{WindowFrame, WindowFrameBound, WindowFrameUnits};

use crate::runtime::operators::window::model::TimeGranularity;

/// RANGE window length in milliseconds.
/// Month components use the same fixed 30-day month as [`TimeGranularity::Months`],
/// so a month-based frame stays an exact multiple of month tiles.
pub fn get_window_length_ms(window_frame: &WindowFrame) -> i64 {
    assert_eq!(
        window_frame.units,
        WindowFrameUnits::Range,
        "only RANGE windows are supported"
    );
    match &window_frame.start_bound {
        WindowFrameBound::Preceding(value) => match value {
            datafusion::scalar::ScalarValue::IntervalMonthDayNano(Some(v)) => {
                (v.nanoseconds / 1_000_000)
                    + (v.days as i64 * TimeGranularity::Days(1).to_millis())
                    + (v.months as i64 * TimeGranularity::Months(1).to_millis())
            }
            datafusion::scalar::ScalarValue::UInt64(Some(v)) => *v as i64,
            datafusion::scalar::ScalarValue::Int64(Some(v)) => *v,
            _ => panic!("Unsupported window frame bound type: {:?}", value),
        },
        _ => panic!(
            "Unsupported window frame start bound: {:?}",
            window_frame.start_bound
        ),
    }
}

pub fn require_range_frame(window_frame: &WindowFrame) {
    if window_frame.units != WindowFrameUnits::Range {
        panic!(
            "ROWS windows are not supported; use RANGE (got {:?})",
            window_frame.units
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::IntervalMonthDayNano;
    use datafusion::scalar::ScalarValue;

    const DAY_MS: i64 = 24 * 60 * 60 * 1000;

    /// `INTERVAL '<n>' <unit>` planned as a PRECEDING RANGE bound.
    fn interval_frame(months: i32, days: i32, nanoseconds: i64) -> WindowFrame {
        WindowFrame::new_bounds(
            WindowFrameUnits::Range,
            WindowFrameBound::Preceding(ScalarValue::IntervalMonthDayNano(Some(
                IntervalMonthDayNano::new(months, days, nanoseconds),
            ))),
            WindowFrameBound::CurrentRow,
        )
    }

    #[test]
    fn sub_day_intervals_use_nanoseconds() {
        // INTERVAL '1000' MILLISECOND
        assert_eq!(
            get_window_length_ms(&interval_frame(0, 0, 1_000_000_000)),
            1_000
        );
        // INTERVAL '5' MINUTE
        assert_eq!(
            get_window_length_ms(&interval_frame(0, 0, 300_000_000_000)),
            300_000
        );
    }

    #[test]
    fn day_intervals_use_days() {
        // INTERVAL '7' DAY
        assert_eq!(get_window_length_ms(&interval_frame(0, 7, 0)), 7 * DAY_MS);
    }

    #[test]
    fn month_intervals_use_months() {
        // INTERVAL '1' MONTH
        assert_eq!(get_window_length_ms(&interval_frame(1, 0, 0)), 30 * DAY_MS);
        // INTERVAL '1' YEAR is planned as 12 months
        assert_eq!(
            get_window_length_ms(&interval_frame(12, 0, 0)),
            12 * 30 * DAY_MS
        );
    }

    #[test]
    fn month_length_matches_month_tile_granularity() {
        assert_eq!(
            get_window_length_ms(&interval_frame(1, 0, 0)),
            TimeGranularity::Months(1).to_millis()
        );
    }

    #[test]
    fn all_interval_components_are_summed() {
        assert_eq!(
            get_window_length_ms(&interval_frame(1, 2, 3_000_000)),
            30 * DAY_MS + 2 * DAY_MS + 3
        );
    }

    #[test]
    fn integer_bounds_are_millis() {
        let frame = WindowFrame::new_bounds(
            WindowFrameUnits::Range,
            WindowFrameBound::Preceding(ScalarValue::Int64(Some(5_000))),
            WindowFrameBound::CurrentRow,
        );
        assert_eq!(get_window_length_ms(&frame), 5_000);
    }
}
