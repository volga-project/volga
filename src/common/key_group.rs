//! Key-group assignment: stable key identity independent of current parallelism.
//!
//! ```text
//! key_group = hash % max_parallelism
//! subtask   = key_group * p / max_parallelism
//! ```
//!
//! Callers must pass `max_parallelism >= parallelism >= 1`.

use serde::{Deserialize, Serialize};

/// Contiguous Flink-style assignment: `start` inclusive, `end` exclusive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct KeyGroupRange {
    pub start: usize,
    pub end: usize,
}

impl KeyGroupRange {
    pub fn new(start: usize, end: usize) -> Self {
        assert!(start <= end, "key-group range start {start} must be <= end {end}");
        Self { start, end }
    }

    pub fn full(max_parallelism: usize) -> Self {
        assert!(max_parallelism >= 1, "max_parallelism must be >= 1");
        Self {
            start: 0,
            end: max_parallelism,
        }
    }

    pub fn contains(self, key_group: usize) -> bool {
        key_group >= self.start && key_group < self.end
    }

    pub fn is_empty(self) -> bool {
        self.start == self.end
    }

    /// Contiguous groups owned by `task_index` at `(parallelism, max_parallelism)`.
    pub fn for_subtask(
        task_index: usize,
        parallelism: usize,
        max_parallelism: usize,
    ) -> Self {
        range_for_subtask(task_index, parallelism, max_parallelism)
    }
}

/// Contiguous groups owned by `task_index` at `(parallelism, max_parallelism)`.
pub fn range_for_subtask(
    task_index: usize,
    parallelism: usize,
    max_parallelism: usize,
) -> KeyGroupRange {
    assert!(parallelism >= 1, "parallelism must be >= 1");
    assert!(
        max_parallelism >= parallelism,
        "max_parallelism ({max_parallelism}) must be >= parallelism ({parallelism})"
    );
    assert!(
        task_index < parallelism,
        "task_index {task_index} must be < parallelism {parallelism}"
    );
    let start = (task_index * max_parallelism + parallelism - 1) / parallelism;
    let end_inclusive = ((task_index + 1) * max_parallelism - 1) / parallelism;
    KeyGroupRange::new(start, end_inclusive + 1)
}

/// Key group for a job-lifetime hash.
pub fn key_group_of(hash: u64, max_parallelism: usize) -> usize {
    assert!(max_parallelism >= 1, "max_parallelism must be >= 1");
    (hash % max_parallelism as u64) as usize
}

/// Subtask that owns `key_group` at the given parallelism.
pub fn subtask_of(key_group: usize, parallelism: usize, max_parallelism: usize) -> usize {
    assert!(parallelism >= 1, "parallelism must be >= 1");
    assert!(
        max_parallelism >= parallelism,
        "max_parallelism ({max_parallelism}) must be >= parallelism ({parallelism})"
    );
    assert!(
        key_group < max_parallelism,
        "key_group {key_group} must be < max_parallelism {max_parallelism}"
    );
    (key_group * parallelism) / max_parallelism
}

/// Subtask for a hash: `hash → key_group → subtask`.
pub fn subtask_for_hash(hash: u64, parallelism: usize, max_parallelism: usize) -> usize {
    subtask_of(key_group_of(hash, max_parallelism), parallelism, max_parallelism)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn max_p_equals_p_matches_hash_mod_p() {
        for p in [1usize, 2, 4, 7, 16] {
            for hash in [0u64, 1, 7, 16, 17, 64, 127, 128, u64::MAX] {
                assert_eq!(
                    subtask_for_hash(hash, p, p),
                    (hash % p as u64) as usize,
                    "hash={hash} p={p}"
                );
            }
        }
    }

    #[test]
    fn max_p_gt_p_keeps_key_group_stable() {
        let hash = 64u64;
        let kg = key_group_of(hash, 128);
        assert_eq!(kg, 64);
        assert_eq!(subtask_of(kg, 2, 128), 1);
        assert_eq!(subtask_of(kg, 4, 128), 2);
        assert_ne!(subtask_for_hash(hash, 2, 128), (hash % 2) as usize);
    }

    #[test]
    fn range_for_subtask_matches_subtask_of() {
        for max_parallelism in [1usize, 2, 3, 4, 7, 16, 128] {
            for parallelism in 1..=max_parallelism.min(16) {
                for task_index in 0..parallelism {
                    let range = range_for_subtask(task_index, parallelism, max_parallelism);
                    for key_group in 0..max_parallelism {
                        assert_eq!(
                            range.contains(key_group),
                            subtask_of(key_group, parallelism, max_parallelism) == task_index,
                            "kg={key_group} task={task_index} p={parallelism} max_p={max_parallelism}"
                        );
                    }
                }
            }
        }
    }
}
