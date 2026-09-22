pub const RAW_BUCKET_MS: i64 = 60_000;
pub const TRIGGER_BUCKET_MS: i64 = 60_000;
pub const TRIGGER_SHARD_COUNT: usize = 32;

pub const TABLES: &[&str] = &[
    r#"CREATE TABLE IF NOT EXISTS window_kg_buckets (
        namespace blob,
        key_group int,
        bucket_start bigint,
        business_key blob,
        PRIMARY KEY ((namespace, key_group), bucket_start, business_key)
    ) WITH CLUSTERING ORDER BY (bucket_start ASC, business_key ASC)"#,
    r#"CREATE TABLE IF NOT EXISTS window_raw (
        namespace blob,
        key_group int,
        business_key blob,
        bucket_start bigint,
        event_ts bigint,
        seq_no bigint,
        attempt bigint,
        epoch bigint,
        payload blob,
        PRIMARY KEY ((namespace, key_group, business_key, bucket_start), event_ts, seq_no, attempt, epoch)
    ) WITH CLUSTERING ORDER BY (event_ts ASC, seq_no ASC, attempt DESC, epoch DESC)"#,
    r#"CREATE TABLE IF NOT EXISTS window_tiles (
        namespace blob,
        key_group int,
        business_key blob,
        granularity_ms bigint,
        bucket_start bigint,
        tile_start bigint,
        attempt bigint,
        epoch bigint,
        payload blob,
        PRIMARY KEY ((namespace, key_group, business_key, granularity_ms, bucket_start), tile_start, attempt, epoch)
    ) WITH CLUSTERING ORDER BY (tile_start ASC, attempt DESC, epoch DESC)"#,
    r#"CREATE TABLE IF NOT EXISTS window_key_states (
        namespace blob,
        key_group int,
        business_key blob,
        attempt bigint,
        epoch bigint,
        key_state blob,
        PRIMARY KEY ((namespace, key_group, business_key), attempt, epoch)
    ) WITH CLUSTERING ORDER BY (attempt DESC, epoch DESC)"#,
    r#"CREATE TABLE IF NOT EXISTS window_triggers (
        namespace blob,
        bucket_start bigint,
        kg_shard int,
        fire_ts bigint,
        fire_seq bigint,
        business_key blob,
        trigger_kind tinyint,
        window_id bigint,
        key_group int,
        attempt bigint,
        epoch bigint,
        PRIMARY KEY ((namespace, bucket_start, kg_shard), fire_ts, fire_seq, business_key, trigger_kind, window_id, attempt, epoch)
    ) WITH CLUSTERING ORDER BY (fire_ts ASC, fire_seq ASC, business_key ASC, trigger_kind ASC, window_id ASC, attempt DESC, epoch DESC)"#,
    r#"CREATE TABLE IF NOT EXISTS window_kg_meta (
        namespace blob,
        key_group int,
        cur_attempt bigint,
        cut blob,
        prev_cut blob,
        prev_checkpoint_id bigint,
        committed_wm bigint,
        retention_floor bigint,
        checkpoint_id bigint,
        PRIMARY KEY ((namespace, key_group))
    )"#,
];

pub fn align_down(ts: i64, width: i64) -> i64 {
    if width <= 0 {
        return ts;
    }
    let ts = ts.max(0);
    (ts / width) * width
}

pub fn kg_shard(key_group: usize, max_parallelism: usize) -> i32 {
    ((key_group * TRIGGER_SHARD_COUNT) / max_parallelism.max(1)) as i32
}

/// Inclusive timestamp range → bucket starts, step `width` (60s for raw/tiles/triggers).
pub fn time_buckets(from_ts: i64, last_included_ts: i64, width: i64) -> Vec<i64> {
    if width <= 0 || last_included_ts < from_ts {
        return Vec::new();
    }
    let mut bucket = align_down(from_ts, width);
    let end = align_down(last_included_ts, width);
    if bucket > end {
        return Vec::new();
    }
    let mut out = Vec::new();
    while bucket <= end {
        out.push(bucket);
        let Some(next) = bucket.checked_add(width) else {
            break;
        };
        bucket = next;
    }
    out
}

/// Last event timestamp included by exclusive cursor `to`.
pub fn last_included_ts(to: crate::runtime::operators::window::model::Cursor) -> i64 {
    if to.seq_no > 0 {
        to.ts
    } else {
        to.ts.saturating_sub(1)
    }
}

#[cfg(test)]
mod bucket_tests {
    use super::*;
    use crate::runtime::operators::window::model::Cursor;

    #[test]
    fn raw_boundary_with_seq_includes_that_bucket() {
        let to = Cursor::new(60_000, 2);
        assert_eq!(
            time_buckets(50_000, last_included_ts(to), RAW_BUCKET_MS),
            vec![0, 60_000]
        );
    }

    #[test]
    fn after_timestamp_excludes_boundary_bucket() {
        let to = Cursor::after_timestamp(60_000);
        assert_eq!(to, Cursor::new(60_001, 0));
        assert_eq!(
            time_buckets(50_000, last_included_ts(to), RAW_BUCKET_MS),
            vec![0, 60_000]
        );
        let to = Cursor::new(60_000, 0);
        assert_eq!(
            time_buckets(50_000, last_included_ts(to), RAW_BUCKET_MS),
            vec![0]
        );
    }

    #[test]
    fn tile_run_crosses_minute() {
        assert_eq!(
            time_buckets(1_000, 120_000 - 1, RAW_BUCKET_MS),
            vec![0, 60_000]
        );
    }
}
