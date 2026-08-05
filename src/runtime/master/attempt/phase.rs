use std::sync::atomic::{AtomicU8, Ordering};

/// Current execution-attempt run phase. Shared so StopSources can set `Draining`
/// while the run loop owns `Running` / `Checkpointing` transitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(in crate::runtime::master) enum AttemptPhase {
    Running = 0,
    Checkpointing = 1,
    Draining = 2,
}

impl AttemptPhase {
    fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::Running,
            1 => Self::Checkpointing,
            _ => Self::Draining,
        }
    }
}

pub(in crate::runtime::master) struct AtomicAttemptPhase(AtomicU8);

impl AtomicAttemptPhase {
    pub(in crate::runtime::master) fn new(phase: AttemptPhase) -> Self {
        Self(AtomicU8::new(phase as u8))
    }

    pub(in crate::runtime::master) fn get(&self) -> AttemptPhase {
        AttemptPhase::from_u8(self.0.load(Ordering::SeqCst))
    }

    pub(in crate::runtime::master) fn set(&self, phase: AttemptPhase) {
        self.0.store(phase as u8, Ordering::SeqCst);
    }

    /// `Checkpointing` → `Running`, but do not overwrite `Draining`.
    pub(in crate::runtime::master) fn finish_checkpoint(&self) {
        let _ = self.0.compare_exchange(
            AttemptPhase::Checkpointing as u8,
            AttemptPhase::Running as u8,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
    }
}
