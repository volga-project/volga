use anyhow::Result;

use crate::runtime::operators::window::model::{Cursor, WindowTrigger};

use super::WindowOperatorStore;

/// Drain `(after, through]` for tests. The operator calls `load_triggers` once.
pub async fn collect_triggers(
    store: &dyn WindowOperatorStore,
    after: Option<Cursor>,
    through: Cursor,
) -> Result<Vec<WindowTrigger>> {
    store.load_triggers(after, through).await
}
