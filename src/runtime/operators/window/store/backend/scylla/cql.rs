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
pub(super) const UPDATE_HEAD_STEAL_OWNER: &str = "UPDATE window_head SET owner_writer = ? WHERE namespace = ? AND key_group = ? AND business_key = ? IF owner_writer = ?";
pub(super) const UPDATE_HEAD_PROMOTE_SERVING: &str = "UPDATE window_head SET writer_attempt = ?, writer_epoch = ?, serving_attempt = ?, serving_epoch = ? WHERE namespace = ? AND key_group = ? AND business_key = ? IF owner_writer = ?";
pub(super) const INSERT_HEAD_IF_NOT_EXISTS: &str = "INSERT INTO window_head (namespace, key_group, business_key, owner_writer, writer_attempt, writer_epoch, serving_attempt, serving_epoch) VALUES (?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS";
pub(super) const INSERT_RECOVERY_BASES: &str = "INSERT INTO window_recovery_bases (namespace, recovery_attempt, range_start, range_end, base_attempt, base_epoch) VALUES (?, ?, ?, ?, ?, ?)";
pub(super) const SELECT_KEY_STATE: &str = "SELECT attempt, epoch, key_state FROM window_key_states WHERE namespace = ? AND key_group = ? AND business_key = ?";
pub(super) const SELECT_RAW: &str = "SELECT event_ts, seq_no, attempt, epoch, payload FROM window_raw WHERE namespace = ? AND key_group = ? AND business_key = ? AND bucket_start = ? AND event_ts >= ? AND event_ts <= ?";
pub(super) const SELECT_TILES: &str = "SELECT tile_start, attempt, epoch, payload FROM window_tiles WHERE namespace = ? AND key_group = ? AND business_key = ? AND granularity_ms = ? AND bucket_start = ? AND tile_start >= ? AND tile_start < ?";
pub(super) const SELECT_TRIGGERS: &str = "SELECT fire_ts, fire_seq, business_key, trigger_kind, window_id, key_group, attempt, epoch FROM window_triggers WHERE namespace = ? AND bucket_start = ? AND kg_shard = ? AND fire_ts >= ? AND fire_ts <= ? LIMIT ?";
pub(super) const SELECT_TRIGGERS_AFTER: &str = "SELECT fire_ts, fire_seq, business_key, trigger_kind, window_id, key_group, attempt, epoch FROM window_triggers WHERE namespace = ? AND bucket_start = ? AND kg_shard = ? AND (fire_ts, fire_seq, business_key, trigger_kind, window_id, attempt, epoch) > (?, ?, ?, ?, ?, ?, ?) AND fire_ts <= ? LIMIT ?";
pub(super) const SELECT_KG_BUCKETS: &str = "SELECT bucket_start, business_key FROM window_kg_buckets WHERE namespace = ? AND key_group = ?";
pub(super) const SELECT_HEAD_VERSIONS: &str = "SELECT writer_attempt, writer_epoch, serving_attempt, serving_epoch FROM window_head WHERE namespace = ? AND key_group = ? AND business_key = ?";
pub(super) const SELECT_RECOVERY_BASES: &str = "SELECT range_start, range_end, base_attempt, base_epoch FROM window_recovery_bases WHERE namespace = ? AND recovery_attempt = ?";
pub(super) const SELECT_KEY_STATE_VERSIONS: &str = "SELECT attempt, epoch FROM window_key_states WHERE namespace = ? AND key_group = ? AND business_key = ?";
pub(super) const SELECT_RAW_VERSIONS: &str = "SELECT event_ts, seq_no, attempt, epoch FROM window_raw WHERE namespace = ? AND key_group = ? AND business_key = ? AND bucket_start = ?";
pub(super) const SELECT_TILE_VERSIONS: &str = "SELECT tile_start, attempt, epoch FROM window_tiles WHERE namespace = ? AND key_group = ? AND business_key = ? AND granularity_ms = ? AND bucket_start = ?";
pub(super) const DELETE_RAW: &str = "DELETE FROM window_raw WHERE namespace = ? AND key_group = ? AND business_key = ? AND bucket_start = ?";
pub(super) const DELETE_TILES: &str = "DELETE FROM window_tiles WHERE namespace = ? AND key_group = ? AND business_key = ? AND granularity_ms = ? AND bucket_start = ?";
pub(super) const DELETE_KG_BUCKETS: &str = "DELETE FROM window_kg_buckets WHERE namespace = ? AND key_group = ? AND bucket_start = ? AND business_key = ?";
pub(super) const DELETE_KEY_STATE_VERSION: &str = "DELETE FROM window_key_states WHERE namespace = ? AND key_group = ? AND business_key = ? AND attempt = ? AND epoch = ?";
pub(super) const DELETE_RAW_VERSION: &str = "DELETE FROM window_raw WHERE namespace = ? AND key_group = ? AND business_key = ? AND bucket_start = ? AND event_ts = ? AND seq_no = ? AND attempt = ? AND epoch = ?";
pub(super) const DELETE_TILE_VERSION: &str = "DELETE FROM window_tiles WHERE namespace = ? AND key_group = ? AND business_key = ? AND granularity_ms = ? AND bucket_start = ? AND tile_start = ? AND attempt = ? AND epoch = ?";
pub(super) const SELECT_HEAD: &str = "SELECT serving_attempt, serving_epoch FROM window_head WHERE namespace = ? AND key_group = ? AND business_key = ?";

#[derive(Clone)]
pub(super) struct PreparedDml {
    pub(super) insert_raw: PreparedStatement,
    pub(super) insert_kg_buckets: PreparedStatement,
    pub(super) insert_tiles: PreparedStatement,
    pub(super) insert_key_states: PreparedStatement,
    pub(super) insert_triggers: PreparedStatement,
    pub(super) steal_head_if_owner: PreparedStatement,
    pub(super) promote_head_if_owner: PreparedStatement,
    pub(super) insert_head_if_not_exists: PreparedStatement,
    pub(super) insert_recovery_bases: PreparedStatement,
    pub(super) select_key_state: PreparedStatement,
    pub(super) select_raw: PreparedStatement,
    pub(super) select_tiles: PreparedStatement,
    pub(super) select_triggers: PreparedStatement,
    pub(super) select_triggers_after: PreparedStatement,
    pub(super) select_kg_buckets: PreparedStatement,
    pub(super) select_head_versions: PreparedStatement,
    pub(super) select_recovery_bases: PreparedStatement,
    pub(super) select_key_state_versions: PreparedStatement,
    pub(super) select_raw_versions: PreparedStatement,
    pub(super) select_tile_versions: PreparedStatement,
    pub(super) delete_raw: PreparedStatement,
    pub(super) delete_tiles: PreparedStatement,
    pub(super) delete_kg_buckets: PreparedStatement,
    pub(super) delete_key_state_version: PreparedStatement,
    pub(super) delete_raw_version: PreparedStatement,
    pub(super) delete_tile_version: PreparedStatement,
    pub(super) select_head: PreparedStatement,
}

impl PreparedDml {
    /// `DefaultRetryPolicy` only retries writes on connection/overload when
    /// this is set. Same-PK INSERTs/DELETEs/SELECTs are safe. Head LWT stays
    /// unset — SERIAL is not retried, and a second Paxos round is not a retry.
    pub(super) fn mark_idempotent_dml(&mut self) {
        for stmt in [
            &mut self.insert_raw,
            &mut self.insert_kg_buckets,
            &mut self.insert_tiles,
            &mut self.insert_key_states,
            &mut self.insert_triggers,
            &mut self.insert_recovery_bases,
            &mut self.select_key_state,
            &mut self.select_raw,
            &mut self.select_tiles,
            &mut self.select_triggers,
            &mut self.select_triggers_after,
            &mut self.select_kg_buckets,
            &mut self.select_head_versions,
            &mut self.select_recovery_bases,
            &mut self.select_key_state_versions,
            &mut self.select_raw_versions,
            &mut self.select_tile_versions,
            &mut self.delete_raw,
            &mut self.delete_tiles,
            &mut self.delete_kg_buckets,
            &mut self.delete_key_state_version,
            &mut self.delete_raw_version,
            &mut self.delete_tile_version,
            &mut self.select_head,
        ] {
            stmt.set_is_idempotent(true);
        }
    }
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
    batch.set_is_idempotent(true);
    for _ in 0..values.len() {
        batch.append_statement(stmt.clone());
    }
    session.batch(&batch, values).await?;
    Ok(())
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum HeadClaim {
    /// No head row. First durable write inserts owner + serving.
    Empty,
    /// Restore pin exists; owner not stolen yet.
    Steal,
    /// We own the row; serving is still the previous cut.
    Fenced,
    /// Caught up; waiting for the publish cadence to promote serving.
    Pending,
    /// We own the row; serving has been published at least once this attempt.
    Ours,
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
