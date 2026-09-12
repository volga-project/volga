use std::collections::BTreeMap;

use anyhow::Result;
use futures::future::try_join_all;
use futures::stream::BoxStream;

use crate::runtime::consts::{
    runtime_consts, WINDOW_PROCESS_KEY_CONCURRENCY, WINDOW_PROCESS_PAGE_SIZE,
};
use crate::runtime::operators::window::model::{Cursor, PartitionKey, WindowTrigger};

use super::{DueWindowWork, TriggerResume, WindowOperatorStore};

pub type DueWorkStream<'a> = BoxStream<'a, Result<Vec<DueWindowWork>>>;

/// Visible triggers to fetch per store hop: page cap, at least the key pool.
pub fn trigger_fetch_limit() -> usize {
    let page = runtime_consts().u64(WINDOW_PROCESS_PAGE_SIZE).max(1) as usize;
    let concurrency = runtime_consts().u64(WINDOW_PROCESS_KEY_CONCURRENCY).max(1) as usize;
    page.max(concurrency)
}

pub async fn group_due_work(
    store: &dyn WindowOperatorStore,
    selected: Vec<WindowTrigger>,
) -> Result<Vec<DueWindowWork>> {
    let mut grouped: BTreeMap<PartitionKey, Vec<WindowTrigger>> = BTreeMap::new();
    for trigger in selected {
        grouped
            .entry(trigger.partition.clone())
            .or_default()
            .push(trigger);
    }
    try_join_all(grouped.into_iter().map(|(partition, triggers)| async move {
        let key_state = store.load_key_state(&partition).await?;
        Ok(DueWindowWork {
            partition,
            key_state,
            triggers,
        })
    }))
    .await
}

/// Operator-owned due stream: store only pages triggers.
pub fn stream_due<'a>(
    store: &'a dyn WindowOperatorStore,
    after: Option<Cursor>,
    through: Cursor,
) -> DueWorkStream<'a> {
    let limit = trigger_fetch_limit();
    Box::pin(futures::stream::try_unfold(
        Some(None::<TriggerResume>),
        move |state| async move {
            let Some(resume) = state else {
                return Ok(None);
            };
            let (triggers, next) = store
                .load_triggers(after, through, resume.as_ref(), limit)
                .await?;
            if triggers.is_empty() {
                return Ok(None);
            }
            let work = group_due_work(store, triggers).await?;
            Ok(Some((work, next.map(Some))))
        },
    ))
}
