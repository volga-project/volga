use crate::common::Key;
use crate::runtime::operators::window::cursor::Cursor;
use crate::runtime::operators::window::state::tile::TimeGranularity;

/// Logical namespace shared by WO and WRO SortedKV clients.
///
/// Byte key layout uses `{kind}/{ns}/{key_hash}/…`. Co-location id is
/// [`partition_key`] (`{ns}/{key_hash}`) — Scylla PK; see SortedKV README.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StateNamespace {
    pub bytes: Vec<u8>,
}

impl StateNamespace {
    pub fn new(s: impl AsRef<[u8]>) -> Self {
        Self {
            bytes: s.as_ref().to_vec(),
        }
    }
}

fn key_hash(key: &Key) -> u64 {
    key.hash()
}

/// Opaque co-location id for one business key: `{ns}/{key_hash}`.
///
/// Used with [`crate::storage::SortedKV::snapshot_partition`]. Not a SortedKV
/// scan prefix (KV keys are kind-first: `raw|tile|meta/{ns}/{hash}/…`).
pub fn partition_key(ns: &StateNamespace, key: &Key) -> Vec<u8> {
    let mut out = Vec::with_capacity(ns.bytes.len() + 10);
    out.extend_from_slice(&ns.bytes);
    out.push(b'/');
    out.extend_from_slice(&be_u64(key_hash(key)));
    out
}

fn be_u64(v: u64) -> [u8; 8] {
    v.to_be_bytes()
}

/// Order-preserving i64 → big-endian (XOR sign bit so negatives sort before positives).
fn be_i64(v: i64) -> [u8; 8] {
    ((v as u64) ^ (1u64 << 63)).to_be_bytes()
}

pub(crate) fn decode_i64(bytes: [u8; 8]) -> i64 {
    (u64::from_be_bytes(bytes) ^ (1u64 << 63)) as i64
}

pub(crate) fn decode_u64(bytes: [u8; 8]) -> u64 {
    u64::from_be_bytes(bytes)
}

/// Align `ts` down to bucket start (`G` ms).
pub fn align_bucket(ts: i64, bucket_ms: i64) -> i64 {
    if bucket_ms <= 0 {
        return ts;
    }
    ts.div_euclid(bucket_ms) * bucket_ms
}

/// `raw/{ns}/{key_hash}/{bucket_ts}/{ts}/{seq_no}` — one event row per KV entry.
///
/// `bucket_ts` enables envelope loads to scan only overlapping time buckets.
/// Row identity is `(ts, seq_no)`; no multi-row packing / `seg_id`
/// (optional later: https://github.com/volga-project/volga/issues/155).
pub fn raw_event_key(
    ns: &StateNamespace,
    key: &Key,
    bucket_ts: i64,
    ts: i64,
    seq_no: u64,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(ns.bytes.len() + 56);
    out.extend_from_slice(b"raw/");
    out.extend_from_slice(&ns.bytes);
    out.push(b'/');
    out.extend_from_slice(&be_u64(key_hash(key)));
    out.push(b'/');
    out.extend_from_slice(&be_i64(bucket_ts));
    out.push(b'/');
    out.extend_from_slice(&be_i64(ts));
    out.push(b'/');
    out.extend_from_slice(&be_u64(seq_no));
    out
}

pub fn raw_cursor_key(
    ns: &StateNamespace,
    key: &Key,
    c: Cursor,
    bucket_ms: i64,
) -> Vec<u8> {
    let bucket_ts = align_bucket(c.ts, bucket_ms);
    raw_event_key(ns, key, bucket_ts, c.ts, c.seq_no)
}

/// Prefix of all rows in one time bucket: `raw/{ns}/{hash}/{bucket_ts}/`.
pub fn raw_bucket_prefix(ns: &StateNamespace, key: &Key, bucket_ts: i64) -> Vec<u8> {
    let mut out = Vec::with_capacity(ns.bytes.len() + 40);
    out.extend_from_slice(b"raw/");
    out.extend_from_slice(&ns.bytes);
    out.push(b'/');
    out.extend_from_slice(&be_u64(key_hash(key)));
    out.push(b'/');
    out.extend_from_slice(&be_i64(bucket_ts));
    out.push(b'/');
    out
}

/// Inclusive-from scan start for envelope `[from, to]`.
pub fn raw_scan_start(
    ns: &StateNamespace,
    key: &Key,
    from: Cursor,
    bucket_ms: i64,
) -> Vec<u8> {
    if from.ts == i64::MIN {
        raw_prefix(ns, key)
    } else {
        raw_cursor_key(ns, key, from, bucket_ms)
    }
}

/// Exclusive scan end: first key of the bucket after `to`'s bucket.
pub fn raw_scan_end_exclusive(
    ns: &StateNamespace,
    key: &Key,
    to: Cursor,
    bucket_ms: i64,
) -> Vec<u8> {
    let b = bucket_ms.max(1);
    let end_bucket = align_bucket(to.ts, b).saturating_add(b);
    raw_bucket_prefix(ns, key, end_bucket)
}

pub fn raw_prefix(ns: &StateNamespace, key: &Key) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(b"raw/");
    out.extend_from_slice(&ns.bytes);
    out.push(b'/');
    out.extend_from_slice(&be_u64(key_hash(key)));
    out.push(b'/');
    out
}

/// Parse `(ts, seq_no)` from a raw event key (ignores `bucket_ts`).
pub fn parse_cursor_from_raw_key(key: &[u8]) -> Option<Cursor> {
    let mut parts = key.split(|&b| b == b'/');
    let _ = parts.next()?; // raw
    let _ = parts.next()?; // ns
    let _hash = parts.next()?;
    let _bucket = parts.next()?;
    let ts_b = parts.next()?;
    let seq_b = parts.next()?;
    if ts_b.len() != 8 || seq_b.len() != 8 {
        return None;
    }
    let mut ts_arr = [0u8; 8];
    let mut seq_arr = [0u8; 8];
    ts_arr.copy_from_slice(ts_b);
    seq_arr.copy_from_slice(seq_b);
    Some(Cursor::new(decode_i64(ts_arr), decode_u64(seq_arr)))
}

/// Parse `bucket_ts` from a raw event key.
pub fn parse_bucket_ts_from_raw_key(key: &[u8]) -> Option<i64> {
    let mut parts = key.split(|&b| b == b'/');
    let _ = parts.next()?; // raw
    let _ = parts.next()?; // ns
    let _hash = parts.next()?;
    let bucket = parts.next()?;
    if bucket.len() != 8 {
        return None;
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(bucket);
    Some(decode_i64(arr))
}

/// `tile/{ns}/{key_hash}/{gran_millis}/{tile_start}` — shared across all window defs.
pub fn tile_key(
    ns: &StateNamespace,
    key: &Key,
    gran: TimeGranularity,
    tile_start: i64,
) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(b"tile/");
    out.extend_from_slice(&ns.bytes);
    out.push(b'/');
    out.extend_from_slice(&be_u64(key_hash(key)));
    out.push(b'/');
    out.extend_from_slice(&be_i64(gran.to_millis()));
    out.push(b'/');
    out.extend_from_slice(&be_i64(tile_start));
    out
}

pub fn tile_scan_start(
    ns: &StateNamespace,
    key: &Key,
    gran: TimeGranularity,
    start: i64,
) -> Vec<u8> {
    tile_key(ns, key, gran, start)
}

pub fn tile_scan_end(
    ns: &StateNamespace,
    key: &Key,
    gran: TimeGranularity,
    end_exclusive: i64,
) -> Vec<u8> {
    tile_key(ns, key, gran, end_exclusive)
}

pub fn tile_key_prefix(ns: &StateNamespace, key: &Key) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(b"tile/");
    out.extend_from_slice(&ns.bytes);
    out.push(b'/');
    out.extend_from_slice(&be_u64(key_hash(key)));
    out.push(b'/');
    out
}

/// `meta/{ns}/{key_hash}` — single KeyState blob per stream key.
pub fn key_state_key(ns: &StateNamespace, key: &Key) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(b"meta/");
    out.extend_from_slice(&ns.bytes);
    out.push(b'/');
    out.extend_from_slice(&be_u64(key_hash(key)));
    out
}
