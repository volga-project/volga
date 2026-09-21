//! `window_kg_meta`: the streaming → serving seam (request mode only).
//!
//! Two statements, never merged: (A) publish moves the cut; (B) take_attempt
//! does not. `prev_cut` always comes from the row being replaced.

use anyhow::Result;
use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;

use crate::runtime::operators::window::store::backend::version::{Attempt, CutHistory};

use super::store::ScyllaWindowStoreClient;

pub(super) struct MetaCuts {
    pub cur_attempt: Attempt,
    pub cut: CutHistory,
    pub prev_cut: CutHistory,
}

fn decode_cut(bytes: Option<Vec<u8>>) -> Result<CutHistory> {
    match bytes {
        None => Ok(CutHistory::empty()),
        Some(b) if b.is_empty() => Ok(CutHistory::empty()),
        Some(b) => Ok(CutHistory::decode(&b)?),
    }
}

pub(super) async fn load_cuts(
    client: &ScyllaWindowStoreClient,
    kg: i32,
) -> Result<Option<MetaCuts>> {
    let session = client.inner.session();
    let prepared = client.inner.prepared().await?;
    let ns = client.scope.namespace.bytes.as_slice();
    let result = session
        .execute_unpaged(&prepared.select_meta, (ns.to_vec(), kg))
        .await?;
    let rows = result.into_rows_result()?;
    let Some((cur_attempt, cut, prev_cut, _prev_cp, _wm, _floor, _cp_id)) =
        rows.maybe_first_row::<(
            Option<i64>,
            Option<Vec<u8>>,
            Option<Vec<u8>>,
            Option<i64>,
            Option<i64>,
            Option<i64>,
            Option<i64>,
        )>()?
    else {
        return Ok(None);
    };
    Ok(Some(MetaCuts {
        cur_attempt: cur_attempt.unwrap_or(0) as Attempt,
        cut: decode_cut(cut)?,
        prev_cut: decode_cut(prev_cut)?,
    }))
}

const CAS_RETRIES: usize = 8;

pub(super) struct PublishPayload {
    pub cut: CutHistory,
    pub committed_wm: Option<i64>,
    pub retention_floor: Option<i64>,
    pub checkpoint_id: u64,
}

struct MetaRow {
    cur_attempt: Option<i64>,
    cut: Option<Vec<u8>>,
    checkpoint_id: Option<i64>,
}

fn as_i64(v: u64) -> Result<i64> {
    i64::try_from(v).map_err(|_| anyhow::anyhow!("value {v} does not fit in CQL bigint"))
}

async fn lwt_applied(
    session: &Session,
    stmt: &PreparedStatement,
    values: impl scylla::serialize::row::SerializeRow,
) -> Result<bool> {
    let result = session.execute_unpaged(stmt, values).await?;
    let rows = result.into_rows_result()?;
    Ok(rows.maybe_first_row::<(bool,)>()?.map(|(a,)| a).unwrap_or(true))
}

async fn select_row(
    session: &Session,
    stmt: &PreparedStatement,
    ns: &[u8],
    kg: i32,
) -> Result<Option<MetaRow>> {
    let result = session
        .execute_unpaged(stmt, (ns.to_vec(), kg))
        .await?;
    let rows = result.into_rows_result()?;
    let Some((cur_attempt, cut, _prev_cut, _prev_cp, _wm, _floor, checkpoint_id)) =
        rows.maybe_first_row::<(
            Option<i64>,
            Option<Vec<u8>>,
            Option<Vec<u8>>,
            Option<i64>,
            Option<i64>,
            Option<i64>,
            Option<i64>,
        )>()?
    else {
        return Ok(None);
    };
    Ok(Some(MetaRow {
        cur_attempt,
        cut,
        checkpoint_id,
    }))
}

/// (A) publish the cut of a completed checkpoint. Never used with an unchanged cut.
pub(super) async fn publish(
    client: &ScyllaWindowStoreClient,
    kg: i32,
    payload: &PublishPayload,
) -> Result<()> {
    let session = client.inner.session();
    let prepared = client.inner.prepared().await?;
    let ns = client.scope.namespace.bytes.as_slice();
    let me = as_i64(client.my_attempt())?;
    let cp_id = as_i64(payload.checkpoint_id)?;
    let cut = payload.cut.encode()?;
    for _ in 0..CAS_RETRIES {
        match select_row(session.as_ref(), &prepared.select_meta, ns, kg).await? {
            None => {
                if insert_first(
                    session.as_ref(),
                    &prepared.insert_meta,
                    ns,
                    kg,
                    me,
                    &cut,
                    payload.committed_wm,
                    payload.retention_floor,
                    cp_id,
                )
                .await?
                {
                    return Ok(());
                }
            }
            Some(row) => {
                if row.checkpoint_id.is_some_and(|id| id >= cp_id) {
                    return Ok(());
                }
                if cas_publish(
                    session.as_ref(),
                    &prepared.publish_meta,
                    ns,
                    kg,
                    me,
                    &row,
                    &cut,
                    payload.committed_wm,
                    payload.retention_floor,
                    cp_id,
                )
                .await?
                {
                    return Ok(());
                }
            }
        }
    }
    anyhow::bail!("window_kg_meta publish CAS did not apply after {CAS_RETRIES} retries")
}

/// (B) take `cur_attempt` without moving the cut. No-op if the row is absent.
pub(super) async fn take_attempt(client: &ScyllaWindowStoreClient, kg: i32) -> Result<()> {
    let session = client.inner.session();
    let prepared = client.inner.prepared().await?;
    let ns = client.scope.namespace.bytes.as_slice();
    let me = as_i64(client.my_attempt())?;
    for _ in 0..CAS_RETRIES {
        let Some(row) = select_row(session.as_ref(), &prepared.select_meta, ns, kg).await? else {
            return Ok(());
        };
        if row.cur_attempt.is_some_and(|cur| cur >= me) {
            return Ok(());
        }
        if lwt_applied(
            session.as_ref(),
            &prepared.take_attempt,
            (me, ns.to_vec(), kg, me),
        )
        .await?
        {
            return Ok(());
        }
    }
    anyhow::bail!("window_kg_meta take_attempt CAS did not apply after {CAS_RETRIES} retries")
}

/// Trigger 2: publish the restored cut if the row is behind it, else take the attempt.
pub(super) async fn heal_or_take(
    client: &ScyllaWindowStoreClient,
    kg: i32,
    restored_cut: &CutHistory,
    committed_wm: Option<i64>,
    retention_floor: Option<i64>,
    restored_checkpoint_id: Option<u64>,
) -> Result<()> {
    let Some(restored_id) = restored_checkpoint_id else {
        return take_attempt(client, kg).await;
    };
    let session = client.inner.session();
    let prepared = client.inner.prepared().await?;
    let ns = client.scope.namespace.bytes.as_slice();
    let row = select_row(session.as_ref(), &prepared.select_meta, ns, kg).await?;
    let behind = match row {
        None => true,
        Some(row) => row.checkpoint_id.unwrap_or(0) < as_i64(restored_id)?,
    };
    if behind {
        publish(
            client,
            kg,
            &PublishPayload {
                cut: restored_cut.clone(),
                committed_wm,
                retention_floor,
                checkpoint_id: restored_id,
            },
        )
        .await
    } else {
        take_attempt(client, kg).await
    }
}

async fn insert_first(
    session: &Session,
    stmt: &PreparedStatement,
    ns: &[u8],
    kg: i32,
    me: i64,
    cut: &[u8],
    committed_wm: Option<i64>,
    retention_floor: Option<i64>,
    checkpoint_id: i64,
) -> Result<bool> {
    lwt_applied(
        session,
        stmt,
        (
            ns.to_vec(),
            kg,
            me,
            cut.to_vec(),
            Option::<Vec<u8>>::None,
            Option::<i64>::None,
            committed_wm,
            retention_floor,
            checkpoint_id,
        ),
    )
    .await
}

async fn cas_publish(
    session: &Session,
    stmt: &PreparedStatement,
    ns: &[u8],
    kg: i32,
    me: i64,
    row: &MetaRow,
    cut: &[u8],
    committed_wm: Option<i64>,
    retention_floor: Option<i64>,
    checkpoint_id: i64,
) -> Result<bool> {
    lwt_applied(
        session,
        stmt,
        (
            me,
            row.cut.clone(),
            row.checkpoint_id,
            cut.to_vec(),
            committed_wm,
            retention_floor,
            checkpoint_id,
            ns.to_vec(),
            kg,
            me,
            row.checkpoint_id,
        ),
    )
    .await
}
