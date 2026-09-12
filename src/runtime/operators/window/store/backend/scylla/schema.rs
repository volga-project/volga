pub const RAW_BUCKET_MS: i64 = 60_000;
pub const TRIGGER_BUCKET_MS: i64 = 60_000;
pub const TRIGGER_SHARD_COUNT: usize = 32;

pub const TABLES: &[&str] = &[
    r#"CREATE TABLE IF NOT EXISTS window_head (
        namespace blob,
        key_group int,
        business_key blob,
        owner_writer blob,
        writer_attempt blob,
        writer_epoch bigint,
        serving_attempt blob,
        serving_epoch bigint,
        PRIMARY KEY ((namespace, key_group, business_key))
    )"#,
    r#"CREATE TABLE IF NOT EXISTS window_recovery_bases (
        namespace blob,
        recovery_attempt blob,
        range_start int,
        range_end int,
        base_attempt blob,
        base_epoch bigint,
        PRIMARY KEY ((namespace, recovery_attempt), range_start)
    )"#,
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
        attempt blob,
        epoch bigint,
        payload blob,
        PRIMARY KEY ((namespace, key_group, business_key, bucket_start), event_ts, seq_no, attempt, epoch)
    ) WITH CLUSTERING ORDER BY (event_ts ASC, seq_no ASC, attempt ASC, epoch DESC)"#,
    r#"CREATE TABLE IF NOT EXISTS window_tiles (
        namespace blob,
        key_group int,
        business_key blob,
        granularity_ms bigint,
        bucket_start bigint,
        tile_start bigint,
        attempt blob,
        epoch bigint,
        payload blob,
        PRIMARY KEY ((namespace, key_group, business_key, granularity_ms, bucket_start), tile_start, attempt, epoch)
    ) WITH CLUSTERING ORDER BY (tile_start ASC, attempt ASC, epoch DESC)"#,
    r#"CREATE TABLE IF NOT EXISTS window_key_states (
        namespace blob,
        key_group int,
        business_key blob,
        attempt blob,
        epoch bigint,
        key_state blob,
        PRIMARY KEY ((namespace, key_group, business_key), attempt, epoch)
    ) WITH CLUSTERING ORDER BY (attempt ASC, epoch DESC)"#,
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
        attempt blob,
        epoch bigint,
        PRIMARY KEY ((namespace, bucket_start, kg_shard), fire_ts, fire_seq, business_key, trigger_kind, window_id, attempt, epoch)
    ) WITH CLUSTERING ORDER BY (fire_ts ASC, fire_seq ASC, business_key ASC, trigger_kind ASC, window_id ASC, attempt ASC, epoch DESC)"#,
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
