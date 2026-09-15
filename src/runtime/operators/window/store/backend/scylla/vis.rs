use crate::runtime::operators::window::store::backend::StateVersion;

/// WO overlay: `me | (cp.attempt ∧ E ≤ cp_E)`. No checkpoint ⇒ me only.
pub(super) fn overlay_visible(
    me: &[u8],
    cp: Option<&StateVersion>,
    attempt: &[u8],
    epoch: i64,
) -> bool {
    attempt == me
        || cp.is_some_and(|cp| attempt == cp.attempt.as_slice() && epoch <= cp.epoch as i64)
}

/// Same cell → `max(E)`, then `max(attempt)`.
pub(super) fn cell_newer(epoch: i64, attempt: &[u8], best_epoch: i64, best_attempt: &[u8]) -> bool {
    epoch > best_epoch || (epoch == best_epoch && attempt > best_attempt)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cp(attempt: &[u8], epoch: u64) -> StateVersion {
        StateVersion {
            attempt: attempt.to_vec(),
            epoch,
        }
    }

    #[test]
    fn overlay_is_me_until_checkpoint() {
        assert!(overlay_visible(b"b", None, b"b", 99));
        assert!(!overlay_visible(b"b", None, b"a", 1));
    }

    #[test]
    fn overlay_includes_checkpoint_cut() {
        let cp = cp(b"a", 10);
        assert!(overlay_visible(b"b", Some(&cp), b"b", 50));
        assert!(overlay_visible(b"b", Some(&cp), b"a", 10));
        assert!(!overlay_visible(b"b", Some(&cp), b"a", 11));
        assert!(!overlay_visible(b"b", Some(&cp), b"z", 1));
    }

    #[test]
    fn cell_newer_epoch_then_attempt() {
        assert!(cell_newer(2, b"a", 1, b"z"));
        assert!(!cell_newer(1, b"z", 2, b"a"));
        assert!(cell_newer(5, b"b", 5, b"a"));
        assert!(!cell_newer(5, b"a", 5, b"b"));
    }
}
