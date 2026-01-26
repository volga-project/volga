use datafusion::scalar::ScalarValue;

pub(crate) fn format_float(value: f64) -> String {
    let s = format!("{value:.6}");
    let s = s.trim_end_matches('0').trim_end_matches('.');
    if s.is_empty() {
        "0".to_string()
    } else {
        s.to_string()
    }
}

pub(crate) fn scalar_to_string(value: &ScalarValue) -> Option<String> {
    match value {
        ScalarValue::Null => None,
        ScalarValue::Boolean(Some(v)) => Some(v.to_string()),
        ScalarValue::Int8(Some(v)) => Some(v.to_string()),
        ScalarValue::Int16(Some(v)) => Some(v.to_string()),
        ScalarValue::Int32(Some(v)) => Some(v.to_string()),
        ScalarValue::Int64(Some(v)) => Some(v.to_string()),
        ScalarValue::UInt8(Some(v)) => Some(v.to_string()),
        ScalarValue::UInt16(Some(v)) => Some(v.to_string()),
        ScalarValue::UInt32(Some(v)) => Some(v.to_string()),
        ScalarValue::UInt64(Some(v)) => Some(v.to_string()),
        ScalarValue::Float32(Some(v)) => Some(format_float(*v as f64)),
        ScalarValue::Float64(Some(v)) => Some(format_float(*v)),
        ScalarValue::Utf8(Some(v)) => Some(v.clone()),
        ScalarValue::LargeUtf8(Some(v)) => Some(v.clone()),
        ScalarValue::Utf8View(Some(v)) => Some(v.clone()),
        ScalarValue::Decimal128(Some(v), p, s) => Some(format!("{v}({p},{s})")),
        ScalarValue::Decimal256(Some(v), p, s) => Some(format!("{v}({p},{s})")),
        _ => None,
    }
}

pub(crate) fn join_csv(parts: &[String]) -> String {
    if parts.is_empty() {
        String::new()
    } else {
        parts.join(",")
    }
}
