//! Key-group assignment: stable key identity independent of current parallelism.
//!
//! ```text
//! key_group = hash % max_parallelism
//! subtask   = key_group * p / max_parallelism
//! ```
//!
//! `key_group_range` is the inverse of `subtask_of`. Use the ceiling form so
//! `kg ∈ range(subtask_of(kg))` when `p` does not divide `max_parallelism`.
//! Do not use `i * max_p / p`.

use std::ops::RangeInclusive;

/// Key group id in `0..max_parallelism`.
pub type KeyGroupId = u32;

/// Key group for a job-lifetime hash.
pub fn key_group_of(hash: u64, max_parallelism: usize) -> KeyGroupId {
    let max_p = max_parallelism.max(1);
    (hash % max_p as u64) as KeyGroupId
}

/// Subtask that owns `key_group` at the given parallelism.
pub fn subtask_of(key_group: KeyGroupId, parallelism: usize, max_parallelism: usize) -> usize {
    let p = parallelism.max(1);
    let max_p = max_parallelism.max(p);
    debug_assert!((key_group as usize) < max_p);
    (key_group as usize * p) / max_p
}

/// Subtask for a hash: `hash → key_group → subtask`.
pub fn subtask_for_hash(hash: u64, parallelism: usize, max_parallelism: usize) -> usize {
    subtask_of(key_group_of(hash, max_parallelism.max(parallelism.max(1))), parallelism, max_parallelism)
}

/// Inclusive key-group range owned by `task_index`.
///
/// Inverse of [`subtask_of`]: `key_group_of(h) ∈ key_group_range(subtask_of(kg), …)`.
pub fn key_group_range(
    task_index: usize,
    parallelism: usize,
    max_parallelism: usize,
) -> RangeInclusive<KeyGroupId> {
    let p = parallelism.max(1);
    let max_p = max_parallelism.max(p);
    debug_assert!(task_index < p);
    let start = (task_index * max_p + p - 1) / p;
    let end = ((task_index + 1) * max_p - 1) / p;
    debug_assert!(start <= end);
    start as KeyGroupId..=end as KeyGroupId
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
    fn range_is_inverse_of_subtask() {
        for max_p in [1usize, 2, 3, 8, 17, 128] {
            for p in 1..=max_p {
                for kg in 0..max_p as KeyGroupId {
                    let dest = subtask_of(kg, p, max_p);
                    let range = key_group_range(dest, p, max_p);
                    assert!(
                        range.contains(&kg),
                        "kg={kg} dest={dest} range={range:?} p={p} max_p={max_p}"
                    );
                }
            }
        }
    }

    #[test]
    fn ranges_cover_all_groups_without_overlap() {
        let max_p = 128usize;
        let p = 3usize;
        let mut seen = vec![false; max_p];
        for i in 0..p {
            for kg in key_group_range(i, p, max_p) {
                assert!(!seen[kg as usize], "kg {kg} assigned twice");
                seen[kg as usize] = true;
                assert_eq!(subtask_of(kg, p, max_p), i);
            }
        }
        assert!(seen.iter().all(|&s| s));
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
}
