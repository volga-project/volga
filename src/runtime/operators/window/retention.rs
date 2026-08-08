//! WO retention floor helpers.

/// Data retention floor for WO cleanup after watermark `W`:
/// `W - max_window_length - lateness`.
///
/// Raw rows with `ts < floor` and tiles fully below the floor may be pruned.
/// Consumed triggers (`fire_at.ts <= W`) are always dropped separately.
pub fn wo_retention_floor(watermark: i64, max_window_length_ms: i64, lateness_ms: i64) -> i64 {
    let lateness_ms = lateness_ms.max(0);
    let max_window_length_ms = max_window_length_ms.max(0);
    watermark
        .saturating_sub(max_window_length_ms)
        .saturating_sub(lateness_ms)
}

#[cfg(test)]
mod tests {
    use super::wo_retention_floor;

    #[test]
    fn floor_subtracts_window_and_lateness() {
        assert_eq!(wo_retention_floor(10_000, 5_000, 0), 5_000);
        assert_eq!(wo_retention_floor(10_000, 5_000, 2_000), 3_000);
    }

    #[test]
    fn negative_lateness_treated_as_zero() {
        assert_eq!(wo_retention_floor(10_000, 5_000, -100), 5_000);
    }

    #[test]
    fn saturates_at_i64_min() {
        assert_eq!(wo_retention_floor(i64::MIN + 10, 100, 0), i64::MIN);
    }
}
