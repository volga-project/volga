//! Worker-scoped state resource accounting (scaffold).
//!
//! Quotas / pressure wiring lands separately. Hot paths must not block on this.

/// Placeholder for future admission / pressure tracking.
#[derive(Debug, Default)]
pub struct StateResourceTracker;

impl StateResourceTracker {
    pub fn new() -> Self {
        Self
    }
}
