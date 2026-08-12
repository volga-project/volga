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
