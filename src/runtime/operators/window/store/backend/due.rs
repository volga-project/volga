use std::collections::BTreeMap;

use anyhow::Result;

use crate::runtime::consts::{runtime_consts, WINDOW_PROCESS_PAGE_SIZE};
use crate::runtime::operators::window::model::{Cursor, PartitionKey, WindowTrigger};

use super::{DueWindowWork, WindowOperatorStore};

/// Store hop size: `window.process_page_size`.
pub fn trigger_page_size() -> usize {
    runtime_consts().u64(WINDOW_PROCESS_PAGE_SIZE).max(1) as usize
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
    let mut work = Vec::with_capacity(grouped.len());
    for (partition, triggers) in grouped {
        let key_state = store.load_key_state(&partition).await?;
        work.push(DueWindowWork {
            partition,
            key_state,
            triggers,
        });
    }
    Ok(work)
}

/// Drain `(after, through]` until `next` is `None`. Empty hops with a resume continue.
pub async fn collect_due(
    store: &dyn WindowOperatorStore,
    after: Option<Cursor>,
    through: Cursor,
) -> Result<Vec<DueWindowWork>> {
    let limit = trigger_page_size();
    let mut resume = None;
    let mut out = Vec::new();
    loop {
        let (triggers, next) = store
            .load_triggers(after, through, resume.as_ref(), limit)
            .await?;
        if !triggers.is_empty() {
            out.extend(group_due_work(store, triggers).await?);
        }
        match next {
            Some(token) => resume = Some(token),
            None => break,
        }
    }
    Ok(out)
}
