//! Key-group assignment: stable key identity independent of current parallelism.
//!
//! ```text
//! key_group = hash % max_parallelism
//! subtask   = key_group * p / max_parallelism
//! ```
//!
//! Callers must pass `max_parallelism >= parallelism >= 1`.

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
}
