pub const RAW_BUCKET_MS: i64 = 60_000;
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
        tile_start bigint,
        attempt bigint,
        epoch bigint,
        payload blob,
        PRIMARY KEY ((namespace, key_group, business_key, granularity_ms), tile_start, attempt, epoch)
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
        kg_shard int,
        fire_ts bigint,
        fire_seq bigint,
        business_key blob,
        trigger_kind tinyint,
        window_id bigint,
        key_group int,
        attempt bigint,
        epoch bigint,
        PRIMARY KEY ((namespace, kg_shard), fire_ts, fire_seq, business_key, trigger_kind, window_id, attempt, epoch)
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

/// Shards whose every key group lies in `range`. A trigger range delete cannot
/// name `key_group`, so GC may delete a shard only when this task owns it all.
pub fn fully_owned_trigger_shards(
    range: crate::common::KeyGroupRange,
    max_parallelism: usize,
) -> Vec<i32> {
    use std::collections::BTreeSet;
    let max_parallelism = max_parallelism.max(1);
    let mut touched = BTreeSet::new();
    for kg in range.start..range.end.min(max_parallelism) {
        touched.insert(kg_shard(kg, max_parallelism));
    }
    touched
        .into_iter()
        .filter(|&shard| {
            (0..max_parallelism)
                .all(|kg| kg_shard(kg, max_parallelism) != shard || range.contains(kg))
        })
        .collect()
}

/// Inclusive timestamp range → bucket starts, step `width` (60s for raw).
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
    fn bucket_partition_keys() {
        let cases: &[(&str, i64, i64, &[i64])] = &[
            ("two minutes [0, 120000)", 0, 120_000 - 1, &[0, 60_000]),
            (
                "raw [Cursor(50000, 0), Cursor(60000, 2))",
                50_000,
                last_included_ts(Cursor::new(60_000, 2)),
                &[0, 60_000],
            ),
            (
                "after_timestamp(60000)",
                0,
                last_included_ts(Cursor::after_timestamp(60_000)),
                &[0, 60_000],
            ),
            (
                "Cursor(60000, 0) coverage tail",
                50_000,
                last_included_ts(Cursor::new(60_000, 0)),
                &[0],
            ),
        ];
        for &(name, from, last, want) in cases {
            let got = time_buckets(from, last, RAW_BUCKET_MS);
            assert_eq!(got, want, "{name}");
            assert!(
                !got.contains(&120_000),
                "{name} must stop before bucket 120000"
            );
        }
    }

    #[test]
    fn trigger_gc_skips_a_shared_shard() {
        use crate::common::KeyGroupRange;
        assert_eq!(
            fully_owned_trigger_shards(KeyGroupRange::new(5, 6), 32),
            vec![5]
        );
        assert!(fully_owned_trigger_shards(KeyGroupRange::new(0, 1), 128).is_empty());
        assert_eq!(
            fully_owned_trigger_shards(KeyGroupRange::new(0, 4), 128),
            vec![0]
        );
    }
}
