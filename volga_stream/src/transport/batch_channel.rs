use std::sync::{atomic::{AtomicU32, AtomicU64, Ordering}, Arc};

use tokio::sync::{mpsc::{error::SendError, unbounded_channel, UnboundedReceiver, UnboundedSender}, Notify};

use crate::common::Message;


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
    pub async fn send(&self, message: Message) -> Result<(), SendError<Message>> {
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