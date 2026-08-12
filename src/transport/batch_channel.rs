use std::sync::{
    atomic::{AtomicU32, AtomicU64, Ordering},
    Arc, Mutex,
};
use std::time::Instant;

use tokio::sync::{
    mpsc::{error::SendError, unbounded_channel, UnboundedReceiver, UnboundedSender},
    Notify,
};

use crate::common::Message;

/// Exclusive wall-clock time spent blocked on tx queue space.
///
/// Parallel `send`s share one instance: overlapping waits count once (task-level BP),
/// matching Flink-style backpressured time and the queue-fill ratio published by
/// [`crate::transport::transport_client::DataWriter`] on the same write path.
#[derive(Debug, Default)]
pub struct OutputBackpressure {
    accumulated_ns: AtomicU64,
    state: Mutex<WaitState>,
}

#[derive(Debug, Default)]
struct WaitState {
    waiters: u32,
    started: Option<Instant>,
}

impl OutputBackpressure {
    pub fn new() -> Self {
        Self::default()
    }

    fn begin_wait(&self) -> WaitGuard<'_> {
        {
            let mut g = self.state.lock().expect("output backpressure");
            if g.waiters == 0 {
                g.started = Some(Instant::now());
            }
            g.waiters = g.waiters.saturating_add(1);
        }
        WaitGuard(self)
    }

    /// Take accumulated blocked nanos for the current metrics window (includes an
    /// in-flight wait slice, which continues into the next window).
    pub fn take_ns(&self) -> u64 {
        let mut g = self.state.lock().expect("output backpressure");
        let mut total = self.accumulated_ns.swap(0, Ordering::Relaxed);
        if g.waiters > 0 {
            if let Some(started) = g.started.replace(Instant::now()) {
                total = total.saturating_add(started.elapsed().as_nanos() as u64);
            }
        }
        total
    }
}

struct WaitGuard<'a>(&'a OutputBackpressure);

impl Drop for WaitGuard<'_> {
    fn drop(&mut self) {
        let mut g = self.0.state.lock().expect("output backpressure");
        g.waiters = g.waiters.saturating_sub(1);
        if g.waiters == 0 {
            if let Some(started) = g.started.take() {
                self.0
                    .accumulated_ns
                    .fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
            }
        }
    }
}

// Arroyo-style bounded batch channel
// uses batch-size as a bound
#[derive(Debug, Clone)]
pub struct BatchSender {
    size: u32,
    tx: UnboundedSender<Message>,
    queued_messages: Arc<AtomicU32>,
    queued_bytes: Arc<AtomicU64>,
    notify: Arc<Notify>,
}

#[inline]
fn message_count(message: &Message, size: u32) -> u32 {
    (message.num_records() as u32).min(size)
}

#[inline]
fn message_bytes(message: &Message) -> u64 {
    message.get_memory_size() as u64
}

impl BatchSender {
    /// Queue fill ratio in `[0, 1)` used for `volga_stream_task_backpressure_ratio`.
    pub fn backpressure_ratio(&self) -> f64 {
        let size = self.size as f64;
        let remaining = self.capacity() as f64;
        1.0 - (remaining + 1.0) / (size + 1.0)
    }

    /// Send `message`. When `bp` is set, queue-full waits are recorded there.
    pub async fn send(
        &self,
        message: Message,
        bp: Option<&OutputBackpressure>,
    ) -> Result<(), SendError<Message>> {
        // Ensure that every message is sendable, even if it's bigger than our max size
        let count = message_count(&message, self.size);
        loop {
            if self.tx.is_closed() {
                return Err(SendError(message));
            }

            let cur = self.queued_messages.load(Ordering::Acquire);
            if cur as usize + count as usize <= self.size as usize {
                match self.queued_messages.compare_exchange(
                    cur,
                    cur + count,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => {
                        self.queued_bytes
                            .fetch_add(message_bytes(&message), Ordering::AcqRel);
                        return self.tx.send(message);
                    }
                    Err(_) => {
                        // try again
                        continue;
                    }
                }
            } else {
                // not enough room in the queue, wait to be notified that the receiver has
                // consumed
                let _wait = bp.map(|b| b.begin_wait());
                self.notify.notified().await;
            }
        }
    }

    pub fn capacity(&self) -> u32 {
        self.size
            .saturating_sub(self.queued_messages.load(Ordering::Relaxed))
    }

    pub fn queued_bytes(&self) -> u64 {
        self.queued_bytes.load(Ordering::Relaxed)
    }

    pub fn size(&self) -> u32 {
        self.size
    }
}

#[derive(Debug)]
pub struct BatchReceiver {
    size: u32,
    rx: UnboundedReceiver<Message>,
    queued_messages: Arc<AtomicU32>,
    queued_bytes: Arc<AtomicU64>,
    notify: Arc<Notify>,
}

impl BatchReceiver {
    pub async fn recv(&mut self) -> Option<Message> {
        let item = self.rx.recv().await;
        if let Some(item) = &item {
            let count = message_count(item, self.size);
            self.queued_messages.fetch_sub(count, Ordering::SeqCst);
            self.queued_bytes
                .fetch_sub(message_bytes(item), Ordering::AcqRel);
            self.notify.notify_waiters();
        }
        item
    }
}

pub fn batch_bounded_channel(size: u32) -> (BatchSender, BatchReceiver) {
    let (tx, rx) = unbounded_channel();
    let notify = Arc::new(Notify::new());
    let queued_messages = Arc::new(AtomicU32::new(0));
    let queued_bytes = Arc::new(AtomicU64::new(0));
    (
        BatchSender {
            size,
            tx,
            queued_messages: queued_messages.clone(),
            queued_bytes: queued_bytes.clone(),
            notify: notify.clone(),
        },
        BatchReceiver {
            size,
            rx,
            notify,
            queued_bytes,
            queued_messages,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::message::WatermarkMessage;
    use std::time::Duration;
    use tokio::time::{sleep, timeout};

    fn wm(i: u64) -> Message {
        Message::Watermark(WatermarkMessage::new("t".to_string(), i, Some(0)))
    }

    fn assert_ns_near(actual: u64, expected: Duration, lo_frac: f64, hi_frac: f64) {
        let expected_ns = expected.as_nanos() as f64;
        let lo = (expected_ns * lo_frac) as u64;
        let hi = (expected_ns * hi_frac) as u64;
        assert!(
            actual >= lo && actual <= hi,
            "blocked_ns={actual} not in [{lo}, {hi}] for expected {expected:?}"
        );
    }

    #[test]
    fn backpressure_ratio_empty_and_full() {
        let (tx, _rx) = batch_bounded_channel(2);
        assert!((tx.backpressure_ratio() - 0.0).abs() < 1e-9);

        // Fill: each watermark counts as 1 toward the bound.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            tx.send(wm(1), None).await.unwrap();
            tx.send(wm(2), None).await.unwrap();
        });
        assert_eq!(tx.capacity(), 0);
        // 1 - 1/(2+1) = 2/3
        assert!((tx.backpressure_ratio() - (2.0 / 3.0)).abs() < 1e-9);
    }

    #[tokio::test]
    async fn fast_send_records_no_blocked_time() {
        let (tx, mut rx) = batch_bounded_channel(4);
        let bp = OutputBackpressure::new();
        tx.send(wm(1), Some(&bp)).await.unwrap();
        assert_eq!(bp.take_ns(), 0);
        assert!(rx.recv().await.is_some());
    }

    #[tokio::test]
    async fn blocked_send_records_wait_near_hold_time() {
        let (tx, mut rx) = batch_bounded_channel(1);
        let bp = Arc::new(OutputBackpressure::new());
        tx.send(wm(1), None).await.unwrap();
        // size=1 full → 1 - 1/(1+1) = 0.5
        assert!((tx.backpressure_ratio() - 0.5).abs() < 1e-9);

        let hold = Duration::from_millis(80);
        let sender = tx.clone();
        let bp_send = bp.clone();
        let blocked = tokio::spawn(async move { sender.send(wm(2), Some(&bp_send)).await });

        sleep(hold).await;
        assert!(rx.recv().await.is_some());
        blocked.await.unwrap().unwrap();

        let ns = bp.take_ns();
        assert_ns_near(ns, hold, 0.5, 2.5);
    }

    #[tokio::test]
    async fn parallel_waits_count_exclusively() {
        let (tx, mut rx) = batch_bounded_channel(1);
        let bp = Arc::new(OutputBackpressure::new());
        tx.send(wm(0), None).await.unwrap();

        let hold = Duration::from_millis(80);
        let mut joins = Vec::new();
        for i in 1..=2 {
            let sender = tx.clone();
            let bp_send = bp.clone();
            joins.push(tokio::spawn(async move {
                sender.send(wm(i), Some(&bp_send)).await
            }));
        }

        sleep(hold).await;
        // Still both waiting: in-flight exclusive slice ≈ hold, not 2×hold.
        let mid = bp.take_ns();
        assert_ns_near(mid, hold, 0.5, 1.75);

        assert!(rx.recv().await.is_some());
        assert!(rx.recv().await.is_some());
        for j in joins {
            j.await.unwrap().unwrap();
        }
        // Residual after mid take should be small (drain latency), not another full hold.
        assert!(bp.take_ns() < Duration::from_millis(40).as_nanos() as u64);
    }

    #[tokio::test]
    async fn timeout_cancel_still_accounts_blocked_time() {
        let (tx, _rx) = batch_bounded_channel(1);
        let bp = OutputBackpressure::new();
        tx.send(wm(1), None).await.unwrap();

        let wait = Duration::from_millis(50);
        let err = timeout(wait, tx.send(wm(2), Some(&bp))).await;
        assert!(err.is_err(), "send should time out while queue is full");

        let ns = bp.take_ns();
        assert_ns_near(ns, wait, 0.5, 2.5);
        // No double-count after drop.
        assert_eq!(bp.take_ns(), 0);
    }
}
