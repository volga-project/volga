use anyhow::Result;

use crate::runtime::operators::window::model::{PartitionKey, RawRun, TileMap, TileRun};
use crate::runtime::operators::window::store::backend::{
    StateVersion, WindowOperatorStore, WindowStoreTaskScope,
};
use crate::runtime::operators::window::store::data::WindowData;

use super::store::ScyllaWindowStore;

pub(super) async fn load_window_data(
    store: &ScyllaWindowStore,
    partition: &PartitionKey,
    raw_runs: &[RawRun],
    tile_runs: &[TileRun],
) -> Result<WindowData> {
    let session = store.session();
    let prepared = store.prepared().await?;
    let max_p = store.config.max_parallelism.unwrap_or(1).max(1);
    let kg = partition.key_group(max_p) as i32;
    let head = session
        .execute_unpaged(
            &prepared.select_head,
            (
                partition.namespace.clone(),
                kg,
                partition.business_key.clone(),
            ),
        )
        .await?;
    let rows = head.into_rows_result()?;
    let mut serving: Option<(Vec<u8>, i64)> = None;
    for row in rows.rows::<(Vec<u8>, i64)>()? {
        serving = Some(row?);
    }
    let Some((serving_attempt, serving_epoch)) = serving else {
        return Ok(WindowData::new(Vec::new(), TileMap::new()));
    };
    let client = store.client(WindowStoreTaskScope {
        namespace: crate::runtime::operators::window::model::StateNamespace {
            bytes: partition.namespace.clone(),
        },
        max_parallelism: max_p,
        key_group_range: crate::common::KeyGroupRange::full(max_p),
        writer_id: crate::runtime::operators::window::store::backend::WriterId(Vec::new()),
        attempt: Vec::new(),
    });
    *client.restore_base.lock().await = Some(StateVersion {
        attempt: serving_attempt,
        epoch: serving_epoch.max(0) as u64,
    });
    // Pin to serving. Serving stays behind the writer until catch-up promote.
    let (raw, tiles) = tokio::try_join!(
        client.load_raw(partition, raw_runs),
        client.load_tiles(partition, tile_runs),
    )?;
    Ok(WindowData::new(raw, tiles))
}
