use datafusion::logical_expr::{WindowFrame, WindowFrameBound, WindowFrameUnits};

/// RANGE window length in milliseconds.
pub fn get_window_length_ms(window_frame: &WindowFrame) -> i64 {
    assert_eq!(
        window_frame.units,
        WindowFrameUnits::Range,
        "only RANGE windows are supported"
    );
    match &window_frame.start_bound {
        WindowFrameBound::Preceding(value) => match value {
            datafusion::scalar::ScalarValue::IntervalMonthDayNano(Some(v)) => {
                (v.nanoseconds / 1_000_000) + (v.days as i64 * 24 * 60 * 60 * 1000)
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
