//! Totally ordered `(attempt, epoch)` and the per-key-group cut history.
//!
//! Normative body is `STORE_DESIGN.md`. This module is the engine contract.

use serde::{Deserialize, Serialize};

pub type Attempt = u64;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Version {
    pub attempt: Attempt,
    pub epoch: u64,
}

/// One per key group. Sorted ascending by attempt; attempts unique.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CutHistory {
    entries: Vec<Version>,
}

impl CutHistory {
    /// v1 does not retire entries; fail rather than let the list grow without bound.
    pub const MAX_ENTRIES: usize = 1024;

    pub fn empty() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub fn entries(&self) -> &[Version] {
        &self.entries
    }

    pub fn encode(&self) -> Result<Vec<u8>, bincode::Error> {
        bincode::serialize(self)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, bincode::Error> {
        bincode::deserialize(bytes)
    }

    pub fn allows(&self, v: Version) -> bool {
        match self.entries.binary_search_by_key(&v.attempt, |e| e.attempt) {
            Ok(i) => v.epoch <= self.entries[i].epoch,
            Err(_) => false,
        }
    }

    /// Add or raise **only the caller's own** entry. Never touches another
    /// attempt's entry. `None` contributes nothing — never inserts `(me, 0)`.
    pub fn advance(&self, me: Attempt, acked_prefix: Option<u64>) -> CutHistory {
        let mut next = self.clone();
        let Some(epoch) = acked_prefix else {
            return next;
        };
        match next.entries.binary_search_by_key(&me, |e| e.attempt) {
            Ok(i) => {
                assert!(
                    next.entries[i].epoch <= epoch,
                    "own prefix must not go backwards"
                );
                next.entries[i].epoch = epoch;
            }
            Err(i) => {
                assert!(
                    next.entries.get(i).map_or(true, |e| e.attempt > me),
                    "attempt must dominate every inherited entry",
                );
                next.entries.insert(i, Version { attempt: me, epoch });
            }
        }
        next
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn advance_none_is_noop() {
        let cut = CutHistory::empty().advance(1, Some(4));
        assert_eq!(cut.advance(2, None), cut);
        assert!(!cut.allows(Version {
            attempt: 2,
            epoch: 0
        }));
    }

    #[test]
    fn allows_is_per_attempt_prefix() {
        let cut = CutHistory::empty().advance(1, Some(10));
        assert!(cut.allows(Version {
            attempt: 1,
            epoch: 10
        }));
        assert!(!cut.allows(Version {
            attempt: 1,
            epoch: 11
        }));
        assert!(!cut.allows(Version {
            attempt: 2,
            epoch: 0
        }));
    }

    #[test]
    fn advance_does_not_touch_other_attempts() {
        let cut = CutHistory::empty().advance(1, Some(10)).advance(2, Some(3));
        assert_eq!(cut.entries()[0].epoch, 10);
        assert_eq!(cut.entries()[1].epoch, 3);
    }

    #[test]
    fn encode_round_trips() {
        let cut = CutHistory::empty().advance(1, Some(4));
        let bytes = cut.encode().unwrap();
        assert_eq!(CutHistory::decode(&bytes).unwrap(), cut);
    }
}
