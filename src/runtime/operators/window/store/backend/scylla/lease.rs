use std::time::Instant;

use anyhow::Result;

use crate::runtime::operators::window::state::WATERMARK_UNSET;

use super::cql::{encode_owner_writer, lwt_applied};
use super::store::ScyllaWindowStoreClient;

pub(super) struct LeaseRow {
    pub(super) owner: Vec<u8>,
    pub(super) serving_attempt: Vec<u8>,
    pub(super) serving_epoch: i64,
    pub(super) serving_wm: i64,
    pub(super) prev_attempt: Vec<u8>,
    pub(super) prev_epoch: i64,
}

pub(super) fn publish_allowed(current_wm: i64, serving_wm: i64) -> bool {
    current_wm != WATERMARK_UNSET && current_wm >= serving_wm
}

pub(super) async fn load_lease(client: &ScyllaWindowStoreClient, kg: i32) -> Result<Option<LeaseRow>> {
    let session = client.inner.session();
    let prepared = client.inner.prepared().await?;
    let result = session
        .execute_unpaged(
            &prepared.select_lease,
            (client.scope.namespace.bytes.clone(), kg),
        )
        .await?;
    let rows = result.into_rows_result()?;
    let mut iter = rows.rows::<(Vec<u8>, Vec<u8>, i64, i64, Vec<u8>, i64)>()?;
    let Some(row) = iter.next().transpose()? else {
        return Ok(None);
    };
    let (owner, serving_attempt, serving_epoch, serving_wm, prev_attempt, prev_epoch) = row;
    Ok(Some(LeaseRow {
        owner,
        serving_attempt,
        serving_epoch,
        serving_wm,
        prev_attempt,
        prev_epoch,
    }))
}

/// Lazy steal on first write to this group. Does not move `serving_*`.
pub(super) async fn steal(client: &ScyllaWindowStoreClient, kg: i32) -> Result<()> {
    if !client.mark_stolen(kg) {
        return Ok(());
    }
    let me = encode_owner_writer(&client.scope.attempt, &client.scope.writer_id.0);
    let session = client.inner.session();
    let prepared = client.inner.prepared().await?;
    let ns = client.scope.namespace.bytes.clone();
    match load_lease(client, kg).await? {
        None => {
            let applied = lwt_applied(
                session
                    .execute_unpaged(
                        &prepared.insert_lease_if_not_exists,
                        (
                            ns,
                            kg,
                            me,
                            Vec::<u8>::new(),
                            0i64,
                            WATERMARK_UNSET,
                            Vec::<u8>::new(),
                            0i64,
                        ),
                    )
                    .await?,
            )?;
            if !applied {
                anyhow::bail!("window_kg_lease is owned by another writer; refusing to steal");
            }
            Ok(())
        }
        Some(row) if row.owner == me => Ok(()),
        Some(row) => {
            let applied = lwt_applied(
                session
                    .execute_unpaged(
                        &prepared.steal_lease,
                        (
                            me,
                            row.serving_attempt,
                            row.serving_epoch,
                            ns,
                            kg,
                            row.owner,
                        ),
                    )
                    .await?,
            )?;
            if !applied {
                anyhow::bail!("window_kg_lease is owned by another writer; refusing to steal");
            }
            Ok(())
        }
    }
}

/// One publish function. Gate: `current_wm >= serving_wm`. Owner CAS fence.
pub(super) async fn publish(client: &ScyllaWindowStoreClient, kg: i32, epoch: i64) -> Result<()> {
    let current_wm = client.current_wm();
    let Some(row) = load_lease(client, kg).await? else {
        return Ok(());
    };
    if !publish_allowed(current_wm, row.serving_wm) {
        return Ok(());
    }
    let me = encode_owner_writer(&client.scope.attempt, &client.scope.writer_id.0);
    let session = client.inner.session();
    let prepared = client.inner.prepared().await?;
    let applied = lwt_applied(
        session
            .execute_unpaged(
                &prepared.publish_lease,
                (
                    client.scope.attempt.clone(),
                    epoch,
                    current_wm,
                    client.scope.namespace.bytes.clone(),
                    kg,
                    me,
                ),
            )
            .await?,
    )?;
    if !applied {
        anyhow::bail!("window_kg_lease is owned by another writer; refusing to publish");
    }
    client.note_published(kg, Instant::now());
    Ok(())
}

pub(super) async fn after_write(client: &ScyllaWindowStoreClient, kg: i32, epoch: i64) -> Result<()> {
    let Some(cadence) = client.serving_publish() else {
        return Ok(());
    };
    steal(client, kg).await?;
    if cadence.promote_on_ingest(client.last_published_at(kg)) {
        publish(client, kg, epoch).await?;
    }
    Ok(())
}

pub(super) async fn flush_stolen(client: &ScyllaWindowStoreClient) -> Result<()> {
    if client.serving_publish().is_none() {
        return Ok(());
    }
    let epoch = client.writer_epoch();
    if epoch <= 0 {
        return Ok(());
    }
    for kg in client.stolen_groups() {
        publish(client, kg, epoch).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publish_gate_needs_watermark_at_or_past_serving() {
        assert!(!publish_allowed(WATERMARK_UNSET, WATERMARK_UNSET));
        assert!(!publish_allowed(WATERMARK_UNSET, 10));
        assert!(!publish_allowed(9, 10));
        assert!(publish_allowed(10, 10));
        assert!(publish_allowed(11, 10));
        assert!(publish_allowed(0, WATERMARK_UNSET));
    }
}
