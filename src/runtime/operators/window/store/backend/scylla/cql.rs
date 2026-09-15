use anyhow::{anyhow, Result};
use futures::future::try_join_all;
use scylla::client::session::Session;
use scylla::deserialize::row::ColumnIterator;
use scylla::deserialize::value::DeserializeValue;
use scylla::response::query_result::QueryResult;
use scylla::statement::batch::{Batch, BatchType};
use scylla::statement::prepared::PreparedStatement;

pub(super) const INSERT_RAW: &str = "INSERT INTO window_raw (namespace, key_group, business_key, bucket_start, event_ts, seq_no, attempt, epoch, payload) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
pub(super) const INSERT_KG_BUCKETS: &str = "INSERT INTO window_kg_buckets (namespace, key_group, bucket_start, business_key) VALUES (?, ?, ?, ?)";
pub(super) const INSERT_TILES: &str = "INSERT INTO window_tiles (namespace, key_group, business_key, granularity_ms, bucket_start, tile_start, attempt, epoch, payload) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
pub(super) const INSERT_KEY_STATES: &str = "INSERT INTO window_key_states (namespace, key_group, business_key, attempt, epoch, key_state) VALUES (?, ?, ?, ?, ?, ?)";
pub(super) const INSERT_TRIGGERS: &str = "INSERT INTO window_triggers (namespace, bucket_start, kg_shard, fire_ts, fire_seq, business_key, trigger_kind, window_id, key_group, attempt, epoch) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
pub(super) const SELECT_KEY_STATE: &str = "SELECT attempt, epoch, key_state FROM window_key_states WHERE namespace = ? AND key_group = ? AND business_key = ?";
pub(super) const SELECT_RAW: &str = "SELECT event_ts, seq_no, attempt, epoch, payload FROM window_raw WHERE namespace = ? AND key_group = ? AND business_key = ? AND bucket_start = ? AND event_ts >= ? AND event_ts <= ?";
pub(super) const SELECT_TILES: &str = "SELECT tile_start, attempt, epoch, payload FROM window_tiles WHERE namespace = ? AND key_group = ? AND business_key = ? AND granularity_ms = ? AND bucket_start = ? AND tile_start >= ? AND tile_start < ?";
pub(super) const SELECT_TRIGGERS: &str = "SELECT fire_ts, fire_seq, business_key, trigger_kind, window_id, key_group, attempt, epoch FROM window_triggers WHERE namespace = ? AND bucket_start = ? AND kg_shard = ? AND (fire_ts, fire_seq, business_key, trigger_kind, window_id, attempt, epoch) > (?, ?, ?, ?, ?, ?, ?) AND fire_ts <= ? LIMIT ?";
pub(super) const SELECT_LEASE: &str = "SELECT owner_writer, serving_attempt, serving_epoch, serving_wm, prev_attempt, prev_epoch FROM window_kg_lease WHERE namespace = ? AND key_group = ?";
pub(super) const INSERT_LEASE_IF_NOT_EXISTS: &str = "INSERT INTO window_kg_lease (namespace, key_group, owner_writer, serving_attempt, serving_epoch, serving_wm, prev_attempt, prev_epoch) VALUES (?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS";
pub(super) const STEAL_LEASE: &str = "UPDATE window_kg_lease SET owner_writer = ?, prev_attempt = ?, prev_epoch = ? WHERE namespace = ? AND key_group = ? IF owner_writer = ?";
pub(super) const PUBLISH_LEASE: &str = "UPDATE window_kg_lease SET serving_attempt = ?, serving_epoch = ?, serving_wm = ? WHERE namespace = ? AND key_group = ? IF owner_writer = ?";
pub(super) const SELECT_KG_BUCKETS: &str = "SELECT bucket_start, business_key FROM window_kg_buckets WHERE namespace = ? AND key_group = ?";
pub(super) const SELECT_KEY_STATE_VERSIONS: &str = "SELECT attempt, epoch FROM window_key_states WHERE namespace = ? AND key_group = ? AND business_key = ?";
pub(super) const SELECT_RAW_VERSIONS: &str = "SELECT event_ts, seq_no, attempt, epoch FROM window_raw WHERE namespace = ? AND key_group = ? AND business_key = ? AND bucket_start = ?";
pub(super) const SELECT_TILE_VERSIONS: &str = "SELECT tile_start, attempt, epoch FROM window_tiles WHERE namespace = ? AND key_group = ? AND business_key = ? AND granularity_ms = ? AND bucket_start = ?";
pub(super) const DELETE_RAW: &str = "DELETE FROM window_raw WHERE namespace = ? AND key_group = ? AND business_key = ? AND bucket_start = ?";
pub(super) const DELETE_TILES: &str = "DELETE FROM window_tiles WHERE namespace = ? AND key_group = ? AND business_key = ? AND granularity_ms = ? AND bucket_start = ?";
pub(super) const DELETE_KG_BUCKETS: &str = "DELETE FROM window_kg_buckets WHERE namespace = ? AND key_group = ? AND bucket_start = ? AND business_key = ?";
pub(super) const DELETE_KEY_STATE_VERSION: &str = "DELETE FROM window_key_states WHERE namespace = ? AND key_group = ? AND business_key = ? AND attempt = ? AND epoch = ?";
pub(super) const DELETE_RAW_VERSION: &str = "DELETE FROM window_raw WHERE namespace = ? AND key_group = ? AND business_key = ? AND bucket_start = ? AND event_ts = ? AND seq_no = ? AND attempt = ? AND epoch = ?";
pub(super) const DELETE_TILE_VERSION: &str = "DELETE FROM window_tiles WHERE namespace = ? AND key_group = ? AND business_key = ? AND granularity_ms = ? AND bucket_start = ? AND tile_start = ? AND attempt = ? AND epoch = ?";

#[derive(Clone)]
pub(super) struct PreparedDml {
    pub(super) insert_raw: PreparedStatement,
    pub(super) insert_kg_buckets: PreparedStatement,
    pub(super) insert_tiles: PreparedStatement,
    pub(super) insert_key_states: PreparedStatement,
    pub(super) insert_triggers: PreparedStatement,
    pub(super) select_key_state: PreparedStatement,
    pub(super) select_raw: PreparedStatement,
    pub(super) select_tiles: PreparedStatement,
    pub(super) select_triggers: PreparedStatement,
    pub(super) select_lease: PreparedStatement,
    pub(super) insert_lease_if_not_exists: PreparedStatement,
    pub(super) steal_lease: PreparedStatement,
    pub(super) publish_lease: PreparedStatement,
    pub(super) select_kg_buckets: PreparedStatement,
    pub(super) select_key_state_versions: PreparedStatement,
    pub(super) select_raw_versions: PreparedStatement,
    pub(super) select_tile_versions: PreparedStatement,
    pub(super) delete_raw: PreparedStatement,
    pub(super) delete_tiles: PreparedStatement,
    pub(super) delete_kg_buckets: PreparedStatement,
    pub(super) delete_key_state_version: PreparedStatement,
    pub(super) delete_raw_version: PreparedStatement,
    pub(super) delete_tile_version: PreparedStatement,
}

pub(super) async fn prepare_stmts<const N: usize>(
    session: &Session,
    cql: [&'static str; N],
) -> Result<[PreparedStatement; N]> {
    try_join_all(cql.map(|s| session.prepare(s)))
        .await?
        .try_into()
        .map_err(|v: Vec<_>| anyhow!("expected {N} prepared statements, got {}", v.len()))
}

pub(super) async fn unlogged_batch(
    session: &Session,
    stmt: &PreparedStatement,
    values: Vec<impl scylla::serialize::row::SerializeRow>,
) -> Result<()> {
    if values.is_empty() {
        return Ok(());
    }
    let mut batch = Batch::new(BatchType::Unlogged);
    for _ in 0..values.len() {
        batch.append_statement(stmt.clone());
    }
    session.batch(&batch, values).await?;
    Ok(())
}

pub(super) fn encode_owner_writer(attempt: &[u8], vertex: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + attempt.len() + vertex.len());
    out.extend_from_slice(&(attempt.len() as u32).to_be_bytes());
    out.extend_from_slice(attempt);
    out.extend_from_slice(vertex);
    out
}

pub(super) fn lwt_applied(result: QueryResult) -> Result<bool> {
    let rows = result.into_rows_result()?;
    let Some(mut cols) = rows
        .maybe_first_row::<ColumnIterator>()
        .map_err(|e| anyhow!("{e}"))?
    else {
        return Ok(false);
    };
    let col = cols
        .next()
        .ok_or_else(|| anyhow!("LWT result missing [applied]"))?
        .map_err(|e| anyhow!("{e}"))?;
    bool::deserialize(col.spec.typ(), col.slice).map_err(|e| anyhow!("{e}"))
}
