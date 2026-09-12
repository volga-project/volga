use anyhow::{anyhow, Result};
use futures::future::try_join_all;
use scylla::client::session::Session;
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
pub(super) const SELECT_TRIGGERS: &str = "SELECT fire_ts, fire_seq, business_key, trigger_kind, window_id, key_group, attempt, epoch FROM window_triggers WHERE namespace = ? AND bucket_start = ? AND kg_shard = ? AND fire_ts >= ? AND fire_ts <= ? LIMIT ?";
pub(super) const SELECT_TRIGGERS_AFTER: &str = "SELECT fire_ts, fire_seq, business_key, trigger_kind, window_id, key_group, attempt, epoch FROM window_triggers WHERE namespace = ? AND bucket_start = ? AND kg_shard = ? AND (fire_ts, fire_seq, business_key, trigger_kind, window_id, attempt, epoch) > (?, ?, ?, ?, ?, ?, ?) AND fire_ts <= ? LIMIT ?";

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
    pub(super) select_triggers_after: PreparedStatement,
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
