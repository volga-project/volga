use crate::common::Key;
use crate::runtime::operators::window::cursor::Cursor;
use crate::runtime::operators::window::state::tile::TimeGranularity;

/// Logical namespace shared by WO and WRO SortedKV clients.
///
/// Byte key layout uses `{kind}/{ns}/{key_hash}/…`. For Scylla, map
/// `(ns, key_hash)` → partition key so raw/tile/meta share one partition
/// (see `runtime/operators/window/README.md`).
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

    pub fn from_task(task_id: &str) -> Self {
        Self::new(task_id.as_bytes())
    }
}

fn key_hash(key: &Key) -> u64 {
    key.hash()
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

/// `raw/{ns}/{key_hash}/{ts}/{seq}`
pub fn raw_key(ns: &StateNamespace, key: &Key, cursor: Cursor) -> Vec<u8> {
    let mut out = Vec::with_capacity(ns.bytes.len() + 1 + 8 + 1 + 8 + 1 + 8 + 4);
    out.extend_from_slice(b"raw/");
    out.extend_from_slice(&ns.bytes);
    out.push(b'/');
    out.extend_from_slice(&be_u64(key_hash(key)));
    out.push(b'/');
    out.extend_from_slice(&be_i64(cursor.ts));
    out.push(b'/');
    out.extend_from_slice(&be_u64(cursor.seq_no));
    out
}

pub fn raw_scan_start(ns: &StateNamespace, key: &Key, from: Cursor) -> Vec<u8> {
    raw_key(ns, key, from)
}

pub fn raw_scan_end(ns: &StateNamespace, key: &Key, to_exclusive_ts: i64) -> Vec<u8> {
    raw_key(
        ns,
        key,
        Cursor::new(to_exclusive_ts, 0),
    )
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
