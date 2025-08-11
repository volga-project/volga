use crate::common::message::{Message};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, AtomicU64};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use std::time::{Duration, Instant};
use arrow::compute::concat_batches;
use std::collections::HashSet;
use tokio_util::sync::CancellationToken;

// TODO use DataFusion's BatchCoalescer

#[derive(Debug, Clone)]
pub struct BatcherConfig {
    pub batch_size: usize,
    pub max_memory_bytes: usize,
    pub flush_interval_ms: u64,
    pub send_timeout_ms: u64,
}

// TODO figure out why disabling flushing can 5x throughput
impl Default for BatcherConfig {
    fn default() -> Self {
        Self {
            batch_size: 1, // TODO changing this fails tests
            max_memory_bytes: 100 * 1024 * 1024, // 100MB
            flush_interval_ms: 100, // 100ms
            send_timeout_ms: 1000, // 1 second
        }
    }
}

#[derive(Debug, Clone)]
pub struct Batcher {
    config: BatcherConfig,
    regular_queues: HashMap<String, Arc<Mutex<Vec<Message>>>>,
    keyed_queues: HashMap<String, Arc<Mutex<HashMap<u64, Arc<Mutex<Vec<Message>>>>>>>,
    current_memory_bytes: Arc<AtomicUsize>,
    last_flush: Arc<AtomicU64>,
    last_memory_sync: Arc<AtomicU64>,
    senders: HashMap<String, mpsc::Sender<Message>>,

    flush_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    memory_sync_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    cancel_token: Arc<CancellationToken>,
}

impl Batcher {
    pub fn new(config: BatcherConfig, senders: HashMap<String, mpsc::Sender<Message>>) -> Self {
        let mut regular_queues: HashMap<String, Arc<Mutex<Vec<Message>>>> = HashMap::new();
        let mut keyed_queues: HashMap<String, Arc<Mutex<HashMap<u64, Arc<Mutex<Vec<Message>>>>>>> = HashMap::new();
        for channel_id in senders.keys() {
            regular_queues.insert(channel_id.clone(), Arc::new(Mutex::new(Vec::new())));
            keyed_queues.insert(channel_id.clone(), Arc::new(Mutex::new(HashMap::new())));
        }

        let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
        Self {
            config,
            regular_queues,
            keyed_queues,
            current_memory_bytes: Arc::new(AtomicUsize::new(0)),
            last_flush: Arc::new(AtomicU64::new(now)),
            last_memory_sync: Arc::new(AtomicU64::new(now)),
            senders,

            flush_handle: Arc::new(Mutex::new(None)),
            memory_sync_handle: Arc::new(Mutex::new(None)),
            cancel_token: Arc::new(CancellationToken::new()),
        }
    }

    // TODO remove keyes on flush
    pub async fn write_message(&mut self, channel_id: &String, message: Message) -> Result<(), mpsc::error::SendError<Message>> {
        // For watermark, flush channel immediatelly and pass watermark through
        if let Message::Watermark(_) = message {
            self.flush_channel(channel_id).await?;
            let sender = self.senders.get(channel_id).unwrap();
            let timeout_duration = Duration::from_millis(self.config.send_timeout_ms);
            match tokio::time::timeout(timeout_duration, sender.send(message.clone())).await {
                Ok(result) => return result,
                Err(_) => return Err(mpsc::error::SendError(message)),
            }
        }

        let message_size = message.get_memory_size();
        
        // Check if we need to flush due to memory pressure
        if self.should_flush_due_to_memory_pressure(message_size) {
            self.flush_all().await?;
        }
        
        let sender = self.senders.get(channel_id).unwrap();
        
        match &message {
            Message::Keyed(keyed_msg) => {
                let hash = keyed_msg.key.hash;
                let channel_keyed_queues = self.keyed_queues.get(channel_id).unwrap();
                let mut keyed_queues_guard = channel_keyed_queues.lock().await;
                if !keyed_queues_guard.contains_key(&hash) {
                    keyed_queues_guard.insert(hash, Arc::new(Mutex::new(Vec::new())));
                }
                let keyed_queue = keyed_queues_guard.get_mut(&hash).unwrap();
                let mut keyed_queue_guard = keyed_queue.lock().await;
                return Self::handle_queue_with_message(message, keyed_queue_guard.as_mut(), sender, self.config.batch_size, self.current_memory_bytes.clone(), self.config.send_timeout_ms).await
            }
            Message::Regular(_) => {
                let regular_queue = self.regular_queues.get(channel_id).unwrap();
                let mut regular_queue_guard = regular_queue.lock().await;
                return Self::handle_queue_with_message(message, regular_queue_guard.as_mut(), sender, self.config.batch_size, self.current_memory_bytes.clone(), self.config.send_timeout_ms).await
            }
            Message::Watermark(_) => {
                panic!("No batching for watermarks")
            }
        }
    }

    async fn handle_queue_with_message(
        message: Message, 
        queue: &mut Vec<Message>, 
        sender: &mpsc::Sender<Message>,
        batch_size: usize, 
        current_memory_bytes: Arc<AtomicUsize>,
        send_timeout_ms: u64
    ) -> Result<(), mpsc::error::SendError<Message>> {
        let message_size = message.get_memory_size();
        queue.push(message);
        Self::inc_memory_usage(message_size, current_memory_bytes.clone());
                
        let (has_full_batch, _) = Self::get_front_batch_start(queue.as_mut(), batch_size);
        if has_full_batch {
            Self::batch_and_flush(queue.as_mut(), batch_size, sender, current_memory_bytes, send_timeout_ms).await?;
        }
        Ok(())
    }

    fn get_front_batch_start(queue: &mut Vec<Message>, batch_size: usize) -> (bool, usize) {
        if queue.len() == 0 {
            panic!("Can not pop empty queue");
        }

        let mut i = queue.len() - 1;
        let mut total_size = 0;
        loop {
            total_size += queue[i].record_batch().num_rows();
            if total_size >= batch_size {
                break;
            }
            if i == 0 {
                break;
            }
            i -= 1;
        }

        // println!("Get front {:?}, batch_size: {}, queue_len: {}", total_size, batch_size, queue.len());

        // true if we have full batch, false otherwise
        if total_size >= batch_size {
            (true, i)
        } else {
            (false, i)
        }
    }

    // TODO remove key from keyed queues
    async fn batch_and_flush(
        queue: &mut Vec<Message>, 
        batch_size: usize, 
        sender: &mpsc::Sender<Message>,
        current_memory_bytes: Arc<AtomicUsize>,
        send_timeout_ms: u64
    ) -> Result<(), mpsc::error::SendError<Message>> {
        while queue.len() != 0 {
            let (_, batch_start) = Self::get_front_batch_start(queue, batch_size);
            let messages_to_batch: Vec<Message> = queue.drain(batch_start..=(queue.len() - 1)).collect();
            let flushed_memory: usize = messages_to_batch.iter().map(|m| m.get_memory_size()).sum();
            
            let batched_message = Self::batch_messages(messages_to_batch);
            let timeout_duration = Duration::from_millis(send_timeout_ms);
            match tokio::time::timeout(timeout_duration, sender.send(batched_message.clone())).await {
                Ok(result) => result?,
                Err(_) => return Err(mpsc::error::SendError(batched_message)),
            }
            Self::dec_memory_usage(flushed_memory, current_memory_bytes.clone());
        }

        Ok(())
    }

    // TODO fix memory tracking
    fn inc_memory_usage(size: usize, current_memory_bytes: Arc<AtomicUsize>) {
        // current_memory_bytes.fetch_add(size, std::sync::atomic::Ordering::Relaxed);
    }

    fn dec_memory_usage(size: usize, current_memory_bytes: Arc<AtomicUsize>) {
        // current_memory_bytes.fetch_sub(size, std::sync::atomic::Ordering::Relaxed);
    }

    fn batch_messages(messages: Vec<Message>) -> Message {
        if messages.is_empty() {
            panic!("Cannot batch empty message list");
        }

        let record_batches: Vec<arrow::record_batch::RecordBatch> = messages
            .iter()
            .map(|m| m.record_batch().clone())
            .collect();

        let batched_record_batch = concat_batches(
            &record_batches[0].schema(),
            &record_batches
        ).expect("Failed to concat record batches");

        let first_message = &messages[0];

        // ingest_timestamp of a batch should be max among all
        let ingest_timestamp = messages.iter().map(|m| m.ingest_timestamp().unwrap()).max().unwrap();

        let batched_message = match first_message {
            Message::Regular(_) => {
                Message::new(
                    first_message.upstream_vertex_id().clone(),
                    batched_record_batch,
                    Some(ingest_timestamp)
                )
            }
            Message::Keyed(_) => {
                // TODO assert all keys are the same
                let key = first_message.key().expect("Keyed message should have key").clone();
                Message::new_keyed(
                    first_message.upstream_vertex_id().clone(),
                    batched_record_batch,
                    key,
                    Some(ingest_timestamp)
                )
            }
            Message::Watermark(_) => {
                panic!("No batching for watermarks")
            }
        };

        batched_message
    }

    pub async fn flush_channel(&mut self, channel_id: &String) -> Result<(), mpsc::error::SendError<Message>> {
        let sender = self.senders.get(channel_id).unwrap();
        let mut num_flushed = 0;
        if let Some(regular_queue) = self.regular_queues.get(channel_id) {
            let mut regular_queue_guard = regular_queue.lock().await;
            num_flushed += regular_queue_guard.len();
            if !regular_queue_guard.is_empty() {
                Self::batch_and_flush(regular_queue_guard.as_mut(), self.config.batch_size, sender, self.current_memory_bytes.clone(), self.config.send_timeout_ms).await?;
            }
            drop(regular_queue_guard);
        } else {
            panic!("No reg queue");
        }

        if let Some(keyed_queues) = self.keyed_queues.get_mut(channel_id) {
            let mut keyed_queues_guard = keyed_queues.lock().await;
            // println!("flush_channel: found {} keyed queues for channel {}", keyed_queues_guard.len(), channel_id);
            for (hash, queue) in keyed_queues_guard.iter_mut() {
                // println!("flush_channel: checking keyed queue with hash {}, address {:p}", hash, queue.as_ref());
                let mut keyed_queue_guard = queue.lock().await;
                // println!("flush_channel: keyed queue hash {} length = {}", hash, keyed_queue_guard.len());
                num_flushed += keyed_queue_guard.len();
            
                if !keyed_queue_guard.is_empty() {
                    // println!("flush_channel: flushing keyed queue hash {} with {} messages", hash, keyed_queue_guard.len());
                    let sender = self.senders.get(channel_id).unwrap();
                    Self::batch_and_flush(keyed_queue_guard.as_mut(), self.config.batch_size, sender, self.current_memory_bytes.clone(), self.config.send_timeout_ms).await?;
                } else {
                    // println!("flush_channel: keyed queue hash {} is empty", hash);
                }
                drop(keyed_queue_guard);
            }
        } else {
            panic!("No keyed queue");
        }

        Ok(())
    }

    // TODO parallel flush
    pub async fn flush_all(&mut self) -> Result<(), mpsc::error::SendError<Message>> {
        let mut all_channels: HashSet<String> = HashSet::new();
        all_channels.extend(self.keyed_queues.keys().cloned());
        all_channels.extend(self.regular_queues.keys().cloned());

        // TODO disabeling this 10x throughput
        for channel_id in all_channels {
            self.flush_channel(&channel_id).await?;
        }
        
        self.last_flush.store(std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    // although we have inc/dec for memory tracker on each message added/flushed
    // we still periodically call this function to avoid accidental drift
    async fn sync_memory_tracker(&mut self) {
        let mut actual_memory = 0;
        
        // Calculate memory from regular queues
        for queue in self.regular_queues.values() {
            let queue_guard = queue.lock().await;
            actual_memory += queue_guard.iter().map(|m| m.get_memory_size()).sum::<usize>();
        }
        
        // Calculate memory from keyed queues
        for channel_queues in self.keyed_queues.values() {
            let channel_queues_guard = channel_queues.lock().await;
            for queue in channel_queues_guard.values() {
                let queue_guard = queue.lock().await;
                actual_memory += queue_guard.iter().map(|m| m.get_memory_size()).sum::<usize>();
            }
        }
        
        // Update the atomic counter with actual memory usage
        self.current_memory_bytes.store(actual_memory, std::sync::atomic::Ordering::Relaxed);
    }

    fn should_flush_due_to_memory_pressure(&self, new_message_size: usize) -> bool {
        let total_memory = self.current_memory_bytes.load(std::sync::atomic::Ordering::Relaxed) + new_message_size;
        
        total_memory > self.config.max_memory_bytes
    }

    pub async fn start(&mut self) {
        // Reset cancellation token for new start
        self.cancel_token = Arc::new(CancellationToken::new());
        
        // Start the background flushing loop
        let config = self.config.clone();
        let last_flush = self.last_flush.clone();
        let mut batcher = self.clone();
        let cancel_token = self.cancel_token.clone();
        
        let flush_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(config.flush_interval_ms));
            
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        // Interval ticked, check if it's time to flush
                        let now_ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as u64;
                        let last_flush_ms = last_flush.load(std::sync::atomic::Ordering::Relaxed);
                        if now_ms >= last_flush_ms + config.flush_interval_ms {
                            if let Err(e) = batcher.flush_all().await {
                                eprintln!("Error in background flush: {:?}", e);
                            }
                            
                            last_flush.store(now_ms, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                    _ = cancel_token.cancelled() => {
                        // Cancellation signal received, break immediately
                        break;
                    }
                }
            }
        });

        let mut batcher = self.clone();
        let last_memory_sync = self.last_memory_sync.clone();
        let cancel_token = self.cancel_token.clone();
        
        let memory_sync_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(100)); // 100ms for memory sync
            
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        // Interval ticked, check if it's time to sync memory
                        let now_ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as u64;
                        let last_memory_sync_ms = last_memory_sync.load(std::sync::atomic::Ordering::Relaxed);

                        if now_ms >= last_memory_sync_ms + 100 {
                            batcher.sync_memory_tracker().await;
                            last_memory_sync.store(now_ms, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                    _ = cancel_token.cancelled() => {
                        // Cancellation signal received, break immediately
                        break;
                    }
                }
            }
        });
        
        // Store the handle
        *self.flush_handle.lock().await = Some(flush_handle);
        *self.memory_sync_handle.lock().await = Some(memory_sync_handle);
    }

    pub async fn flush_and_close(&mut self) -> Result<(), mpsc::error::SendError<Message>> {
        // Cancel background tasks
        self.cancel_token.cancel();
        
        // Wait for the background tasks to finish
        if let Some(handle) = self.flush_handle.lock().await.take() {
            if let Err(e) = handle.await {
                eprintln!("Error waiting for flush task: {:?}", e);
            }
        }

        if let Some(handle) = self.memory_sync_handle.lock().await.take() {
            if let Err(e) = handle.await {
                eprintln!("Error waiting for memory sync task: {:?}", e);
            }
        }
        
        // Perform final flush
        self.flush_all().await?;
        self.sync_memory_tracker().await;
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{Schema, Field, DataType};
    use rand::{random, Rng};
    use std::sync::Arc;
    use crate::common::{Key, MAX_WATERMARK_VALUE};

    fn create_test_record_batch(rows: usize) -> arrow::record_batch::RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("data", DataType::Utf8, false),
        ]));
        
        let ids: Vec<i64> = (0..rows as i64).collect();
        let data: Vec<String> = (0..rows).map(|i| format!("data_{}", i)).collect();
        
        arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(data)),
            ]
        ).unwrap()
    }

    fn create_test_message(rows: usize, timestamp: u64) -> Message {
        let record_batch = create_test_record_batch(rows);
        Message::new(
            Some("test_vertex".to_string()),
            record_batch,
            Some(timestamp)
        )
    }

    fn create_test_keyed_message(rows: usize, timestamp: u64, key: String) -> Message {
        let record_batch = create_test_record_batch(rows);
        
        // Create key batch from the string
        let key_schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
        ]));
        let key_batch = arrow::record_batch::RecordBatch::try_new(
            key_schema,
            vec![
                Arc::new(StringArray::from(vec![key.clone()])),
            ]
        ).unwrap();
        
        let key_obj = Key::new(key_batch).unwrap();
        Message::new_keyed(
            Some("test_vertex".to_string()),
            record_batch,
            key_obj,
            Some(timestamp)
        )
    }

    #[test]
    fn test_get_front_batch_start() {
        let mut queue = Vec::new();
        let batch_size = 10;
        
        // Single message queue
        queue.push(create_test_message(5, 100));
        let (has_batch, start) = Batcher::get_front_batch_start(&mut queue, batch_size);
        assert!(!has_batch);
        assert_eq!(start, 0);
        
        // Multiple messages that don't reach batch size
        queue.push(create_test_message(3, 101));
        queue.push(create_test_message(1, 102));
        let (has_batch, start) = Batcher::get_front_batch_start(&mut queue, batch_size);
        assert!(!has_batch);
        assert_eq!(start, 0);
        
        // Messages that reach batch size
        queue.push(create_test_message(1, 103));
        let (has_batch, start) = Batcher::get_front_batch_start(&mut queue, batch_size);
        assert!(has_batch);
        assert_eq!(start, 0);

        queue.push(create_test_message(3, 103));
        queue.push(create_test_message(3, 103));
        queue.push(create_test_message(3, 103));
        let (has_batch, start) = Batcher::get_front_batch_start(&mut queue, batch_size);
        assert!(has_batch);
        assert_eq!(start, 3);
    }

    #[test]
    fn test_concat_messages() {
        // Test regular message batching
        let messages = vec![
            create_test_message(2, 100),
            create_test_message(3, 101),
            create_test_message(1, 102),
        ];
        
        let batched = Batcher::batch_messages(messages);
        match batched {
            Message::Regular(base_msg) => {
                assert_eq!(base_msg.record_batch.num_rows(), 6); // 2+3+1
                assert_eq!(base_msg.metadata.ingest_timestamp, Some(102)); // Max timestamp
            }
            _ => panic!("Expected regular message"),
        }
        
        // Test keyed message batching
        let keyed_messages = vec![
            create_test_keyed_message(2, 100, "key1".to_string()),
            create_test_keyed_message(3, 101, "key1".to_string()),
        ];
        
        let batched = Batcher::batch_messages(keyed_messages);
        match batched {
            Message::Keyed(keyed_msg) => {
                assert_eq!(keyed_msg.base.record_batch.num_rows(), 5); // 2+3
                assert_eq!(keyed_msg.base.metadata.ingest_timestamp, Some(101)); // Max timestamp
            }
            _ => panic!("Expected keyed message"),
        }
    }

    #[tokio::test]
    async fn test_batcher_watermark_passthrough() {
        let (tx, mut rx) = mpsc::channel(100);
        let mut senders = HashMap::new();
        senders.insert("test_channel".to_string(), tx);
        
        let config = BatcherConfig::default();
        let mut batcher = Batcher::new(config, senders);
        
        // Add watermark message
        let watermark = Message::Watermark(crate::common::message::WatermarkMessage::new(
            "test".to_string(), 100, Some(100)
        ));
        
        batcher.write_message(&"test_channel".to_string(), watermark).await.unwrap();
        
        // Watermark should be sent immediately
        let received = rx.recv().await.unwrap();
        match received {
            Message::Watermark(_) => {},
            _ => panic!("Expected watermark message"),
        }
    }

    #[tokio::test]
    async fn test_batcher_regular_batching() {
        let (tx, mut rx) = mpsc::channel(100);
        let mut senders = HashMap::new();
        senders.insert("test_channel".to_string(), tx);
        
        let mut config = BatcherConfig::default();
        config.batch_size = 5; // Small batch size for testing
        let mut batcher = Batcher::new(config, senders);
        
        // Add messages that should be batched
        for i in 0..3 {
            let msg = create_test_message(2, 100 + i);
            batcher.write_message(&"test_channel".to_string(), msg).await.unwrap();
        }
        
        // Should receive one batched message
        let received = rx.recv().await.unwrap();
        match received {
            Message::Regular(base_msg) => {
                assert_eq!(base_msg.record_batch.num_rows(), 6); // 2+2+2
            }
            _ => panic!("Expected regular message"),
        }

        // Add messages should not be batched
        for i in 0..2 {
            let msg = create_test_message(2, 100 + i);
            batcher.write_message(&"test_channel".to_string(), msg).await.unwrap();
        }

        // assert not received
        let r = rx.try_recv();
        assert!(r.is_err());

        batcher.flush_all().await.unwrap();
        // Should receive one batched message
        let received = rx.recv().await.unwrap();
        match received {
            Message::Regular(base_msg) => {
                assert_eq!(base_msg.record_batch.num_rows(), 4); // 2+2
            }
            _ => panic!("Expected regular message"),
        }
        
    }

    #[tokio::test]
    async fn test_batcher_keyed_batching() {
        let (tx, mut rx) = mpsc::channel(100);
        let mut senders = HashMap::new();
        senders.insert("test_channel".to_string(), tx);
        
        let mut config = BatcherConfig::default();
        config.batch_size = 5;
        let mut batcher = Batcher::new(config, senders);
        

        let key1 = "key1".to_string();
        let key2 = "key2".to_string();
        // Add keyed messages with same key 1 
        for i in 0..3 {
            let msg = create_test_keyed_message(2, 100 + i, key1.clone());
            batcher.write_message(&"test_channel".to_string(), msg).await.unwrap();
        }

        // Add keyed messages with same key 2
        for i in 0..3 {
            let msg = create_test_keyed_message(2, 100 + i, key2.clone());
            batcher.write_message(&"test_channel".to_string(), msg).await.unwrap();
        }
        
        // Should receive one batched keyed message per key
        let mut received = Vec::new();
        received.push(rx.try_recv().unwrap());
        received.push(rx.try_recv().unwrap());

        let mut received_keys = Vec::new();
        for r in received {
            match r {
                Message::Keyed(keyed_msg) => {
                    let key_batch = keyed_msg.key.key_record_batch;
                    let key_array = key_batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
                    let key = key_array.value(0).to_string();
                    received_keys.push(key);

                    // both keys should have 6 rows
                    assert_eq!(keyed_msg.base.record_batch.num_rows(), 6); // 2+2+2
                }
                _ => panic!("Expected keyed message"),
            }
        }
        // both keys are received
        assert!(received_keys.len() == 2);
        assert!(received_keys.contains(&key1));
        assert!(received_keys.contains(&key2));

        // Add not-full batch msg and check flush
        for i in 0..2 {
            let msg = create_test_keyed_message(2, 100 + i, key1.clone());
            batcher.write_message(&"test_channel".to_string(), msg).await.unwrap();
        }

        for i in 0..2 {
            let msg = create_test_keyed_message(2, 100 + i, key2.clone());
            batcher.write_message(&"test_channel".to_string(), msg).await.unwrap();
        }

        // assert not received
        let r = rx.try_recv();
        assert!(r.is_err());

        batcher.flush_all().await.unwrap();

        // should received after flush
        let mut received = Vec::new();
        received.push(rx.try_recv().unwrap());
        received.push(rx.try_recv().unwrap());

        let mut received_keys = Vec::new();
        for r in received {
            match r {
                Message::Keyed(keyed_msg) => {
                    let key_batch = keyed_msg.key.key_record_batch;
                    let key_array = key_batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
                    let key = key_array.value(0).to_string();
                    received_keys.push(key);

                    // both keys should have 4 rows
                    assert_eq!(keyed_msg.base.record_batch.num_rows(), 4); // 2+2
                }
                _ => panic!("Expected keyed message"),
            }
        }

        // both keys are received
        assert!(received_keys.len() == 2);
        assert!(received_keys.contains(&key1));
        assert!(received_keys.contains(&key2));
    }

    #[tokio::test]
    async fn test_memory_tracking_accuracy() {
        let (tx, mut rx) = mpsc::channel(100);
        let mut senders = HashMap::new();
        senders.insert("test_channel".to_string(), tx);
        
        let mut config = BatcherConfig::default();
        config.batch_size = 10; // Large batch size so messages don't batch immediately
        let mut batcher = Batcher::new(config, senders);
        
        // Add several messages
        let mut expected_memory = 0;
        for i in 0..4 {
            let msg = create_test_message(2, 100 + i);
            let msg_size = msg.get_memory_size();
            expected_memory += msg_size;
            batcher.write_message(&"test_channel".to_string(), msg).await.unwrap();
        }
        
        // Check that memory tracking is accurate before correction
        let tracked_memory = batcher.current_memory_bytes.load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(tracked_memory, expected_memory, "Memory tracking should be accurate");
        
        // run manual sync and veryfy same results
        batcher.sync_memory_tracker().await;
        let tracked_memory = batcher.current_memory_bytes.load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(tracked_memory, expected_memory, "Memory tracking should be accurate");
        
        // trigger batching with large message
        let msg = create_test_message(8, 105);

        // this should batch 8 + 2 and flush memory
        batcher.write_message(&"test_channel".to_string(), msg).await.unwrap();
        let tracked_memory = batcher.current_memory_bytes.load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(tracked_memory, 0, "Memory tracking should be accurate");
    
        // run manual sync and veryfy same results
        batcher.sync_memory_tracker().await;
        let tracked_memory = batcher.current_memory_bytes.load(std::sync::atomic::Ordering::Relaxed);
                 assert_eq!(tracked_memory, 0, "Memory tracking should be accurate");
     }

    #[tokio::test]
    async fn test_watermark() {
        // Create channels for different types
        let (tx_regular, mut rx_regular) = mpsc::channel(100);
        let (tx_keyed, mut rx_keyed) = mpsc::channel(100);
        let (tx_mixed, mut rx_mixed) = mpsc::channel(100);
        
        let mut senders = HashMap::new();
        senders.insert("regular_channel".to_string(), tx_regular);
        senders.insert("keyed_channel".to_string(), tx_keyed);
        senders.insert("mixed_channel".to_string(), tx_mixed);
        
        // Configure batcher with large batch size so messages don't auto-flush
        let mut config = BatcherConfig::default();
        config.batch_size = 100; // Large batch size
        let mut batcher = Batcher::new(config, senders);
        
        // Regular channel - add 3 regular messages
        for i in 0..3 {
            let msg = create_test_message(2, 100 + i);
            batcher.write_message(&"regular_channel".to_string(), msg).await.unwrap();
        }
        
        // Keyed channel - add 3 keyed messages with same key
        for i in 0..3 {
            let msg = create_test_keyed_message(2, 200 + i, "key1".to_string());
            batcher.write_message(&"keyed_channel".to_string(), msg).await.unwrap();
        }
        
        // Mixed channel - add 2 regular and 2 keyed messages
        for i in 0..2 {
            let regular_msg = create_test_message(2, 300 + i);
            batcher.write_message(&"mixed_channel".to_string(), regular_msg).await.unwrap();
            
            let keyed_msg = create_test_keyed_message(2, 400 + i, "key2".to_string());
            batcher.write_message(&"mixed_channel".to_string(), keyed_msg).await.unwrap();
        }
        
        // Verify no messages have been sent yet (no auto-flush)
        assert!(rx_regular.try_recv().is_err(), "Regular channel should not have auto-flushed");
        assert!(rx_keyed.try_recv().is_err(), "Keyed channel should not have auto-flushed");
        assert!(rx_mixed.try_recv().is_err(), "Mixed channel should not have auto-flushed");
        
        let watermark_regular = Message::Watermark(crate::common::message::WatermarkMessage::new(
            "watermark_regular".to_string(), 1000, Some(1000)
        ));
        batcher.write_message(&"regular_channel".to_string(), watermark_regular).await.unwrap();
        
        let watermark_keyed = Message::Watermark(crate::common::message::WatermarkMessage::new(
            "watermark_keyed".to_string(), 2000, Some(2000)
        ));
        batcher.write_message(&"keyed_channel".to_string(), watermark_keyed).await.unwrap();
        
        let watermark_mixed = Message::Watermark(crate::common::message::WatermarkMessage::new(
            "watermark_mixed".to_string(), 3000, Some(3000)
        ));
        batcher.write_message(&"mixed_channel".to_string(), watermark_mixed).await.unwrap();
        
        // Verify results for each channel
        // Regular channel: should have 1 batched message + 1 watermark
        let regular_batch = rx_regular.recv().await.unwrap();
        match regular_batch {
            Message::Regular(base_msg) => {
                assert_eq!(base_msg.record_batch.num_rows(), 6); // 3 messages * 2 rows each
            }
            _ => panic!("Expected regular batched message"),
        }
        
        let regular_watermark = rx_regular.recv().await.unwrap();
        match regular_watermark {
            Message::Watermark(wm) => {
                assert_eq!(wm.watermark_value, 1000);
            }
            _ => panic!("Expected watermark message"),
        }
        
        // Keyed channel: should have 1 batched keyed message + 1 watermark
        let keyed_batch = rx_keyed.recv().await.unwrap();
        match keyed_batch {
            Message::Keyed(keyed_msg) => {
                assert_eq!(keyed_msg.base.record_batch.num_rows(), 6); // 3 messages * 2 rows each
                let key_batch = &keyed_msg.key.key_record_batch;
                let key_array = key_batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
                assert_eq!(key_array.value(0), "key1");
            }
            _ => panic!("Expected keyed batched message"),
        }
        
        let keyed_watermark = rx_keyed.recv().await.unwrap();
        match keyed_watermark {
            Message::Watermark(wm) => {
                assert_eq!(wm.watermark_value, 2000);
            }
            _ => panic!("Expected watermark message"),
        }
        
        // Mixed channel: should have 2 batched messages (1 regular + 1 keyed) + 1 watermark
        let mixed_regular_batch = rx_mixed.recv().await.unwrap();
        match mixed_regular_batch {
            Message::Regular(base_msg) => {
                assert_eq!(base_msg.record_batch.num_rows(), 4); // 2 messages * 2 rows each
            }
            _ => panic!("Expected regular batched message from mixed channel"),
        }
        
        let mixed_keyed_batch = rx_mixed.recv().await.unwrap();
        match mixed_keyed_batch {
            Message::Keyed(keyed_msg) => {
                assert_eq!(keyed_msg.base.record_batch.num_rows(), 4); // 2 messages * 2 rows each
                let key_batch = &keyed_msg.key.key_record_batch;
                let key_array = key_batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
                assert_eq!(key_array.value(0), "key2");
            }
            _ => panic!("Expected keyed batched message from mixed channel"),
        }
        
        let mixed_watermark = rx_mixed.recv().await.unwrap();
        match mixed_watermark {
            Message::Watermark(wm) => {
                assert_eq!(wm.watermark_value, 3000);
            }
            _ => panic!("Expected watermark message"),
        }
        
        // Verify no more messages
        assert!(rx_regular.try_recv().is_err(), "Regular channel should have no more messages");
        assert!(rx_keyed.try_recv().is_err(), "Keyed channel should have no more messages");
        assert!(rx_mixed.try_recv().is_err(), "Mixed channel should have no more messages");
    }

    
    async fn test_batcher_e2e(
        test_duration_secs: u64, 
        batch_size: usize, 
        max_message_rate_delay_ms: u64,
        flush_interval_ms: u64,
        max_memory_bytes: usize
    ) {
        println!("[DEBUG] Starting E2E test");
        // Test configuration
        let channels = vec!["channel1".to_string(), "channel2".to_string(), "channel3".to_string()];
        let keys_per_channel = 3;
        
        println!("[DEBUG] Test config: duration={}s, channels={:?}, keys_per_channel={}, batch_size={}", 
                test_duration_secs, channels, keys_per_channel, batch_size);
        
        // Create channels for each output
        let mut senders = HashMap::new();
        let mut receivers = HashMap::new();
        
        for channel_id in &channels {
            let (tx, rx) = mpsc::channel(1000);
            senders.insert(channel_id.clone(), tx);
            receivers.insert(channel_id.clone(), rx);
        }
        
        println!("[DEBUG] Created {} channels", channels.len());
        
        // Configure batcher
        let mut config = BatcherConfig::default();
        config.batch_size = batch_size;
        config.flush_interval_ms = flush_interval_ms;
        config.max_memory_bytes = max_memory_bytes;
        
        let mut batcher = Batcher::new(config, senders);
        
        // Start the batcher
        batcher.start().await;
        println!("[DEBUG] Batcher started");
        
        // Concurrent receiver task
        let receiver_handle = {
            let receivers = receivers;
            
            tokio::spawn(async move {
                println!("[DEBUG] Receiver task started");
                let mut receiver_tasks = Vec::new();
                let mut channel_messages_map: HashMap<String, HashMap<String, Vec<Message>>> = HashMap::new();
                let mut channel_watermarks_map: HashMap<String, Vec<Message>> = HashMap::new();
                
                for (channel_id, mut rx) in receivers {
                    let channel_id_clone = channel_id.clone();
                    let task = tokio::spawn(async move {
                        println!("[DEBUG] Receiver for channel {} started", channel_id_clone);
                        let mut messages_by_key: HashMap<String, Vec<Message>> = HashMap::new();
                        let mut watermarks = Vec::new();
                        let mut msg_count = 0;
                        
                        while let Some(msg) = rx.recv().await {
                            msg_count += 1;
                            if msg_count % 100 == 0 {
                                println!("[DEBUG] Channel {} received {} messages", channel_id_clone, msg_count);
                            }
                            
                            match &msg {
                                Message::Regular(_) => {
                                    messages_by_key.entry("regular".to_string())
                                        .or_insert_with(Vec::new)
                                        .push(msg);
                                }
                                Message::Keyed(keyed_msg) => {
                                    let key_batch = &keyed_msg.key.key_record_batch;
                                    let key_array = key_batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
                                    let key = key_array.value(0).to_string();
                                    messages_by_key.entry(key)
                                        .or_insert_with(Vec::new)
                                        .push(msg);
                                }
                                Message::Watermark(wm) => {
                                    let wm_value = wm.watermark_value;
                                    watermarks.push(msg);
                                    if wm_value == MAX_WATERMARK_VALUE {
                                        break;
                                    }
                                }
                            }
                        }
                        println!("[DEBUG] Channel {} receiver finished, total messages: {}", channel_id_clone, msg_count);
                        (channel_id_clone, messages_by_key, watermarks)
                    });
                    
                    receiver_tasks.push(task);
                }
                
                println!("[DEBUG] Waiting for all receiver tasks to finish");
                // Wait for all receivers to finish
                for task in receiver_tasks {
                    let (channel_id, messages_by_key, watermarks) = task.await.unwrap();
                    channel_messages_map.insert(channel_id.clone(), messages_by_key);
                    channel_watermarks_map.insert(channel_id, watermarks);
                }
                
                println!("[DEBUG] All receiver tasks finished");
                (channel_messages_map, channel_watermarks_map)
            })
        };
        
        let mut batcher_clone = batcher.clone();

        let message_generation_handle = {
            let channels = channels.clone();
            let mut sent_messages: HashMap<String, HashMap<String, Vec<Message>>> = HashMap::new();
            let mut sent_watermarks: HashMap<String, Vec<Message>> = HashMap::new();
            for channel_id in &channels {
                sent_messages.insert(channel_id.clone(), HashMap::new());
                sent_watermarks.insert(channel_id.clone(), Vec::new());
            }
            
            tokio::spawn(async move {
                println!("[DEBUG] Message generation task started");
                let start_time = Instant::now();
                let mut message_id = 0u64;
                let mut total_sent = 0;
                
                while start_time.elapsed() < Duration::from_secs(test_duration_secs) {
                    for channel_id in &channels {
                        // Generate regular message
                        let regular_msg = create_test_message(2, message_id);
                        let regular_key = "regular".to_string();
                        sent_messages.get_mut(channel_id).unwrap()
                            .entry(regular_key.clone())
                            .or_insert_with(Vec::new)
                            .push(regular_msg.clone());
                        
                        // Send to batcher immediately
                        batcher_clone.write_message(channel_id, regular_msg).await.unwrap();
                        total_sent += 1;
                        message_id += 1;
                        
                        // Generate keyed message (cycle through keys)
                        let key_index = (message_id as usize) % keys_per_channel;
                        let key = format!("key{}", key_index);
                        let keyed_msg = create_test_keyed_message(2, message_id, key.clone());
                        sent_messages.get_mut(channel_id).unwrap()
                            .entry(key.clone())
                            .or_insert_with(Vec::new)
                            .push(keyed_msg.clone());
                        
                        // Send to batcher immediately
                        batcher_clone.write_message(channel_id, keyed_msg).await.unwrap();
                        total_sent += 1;
                        message_id += 1;
                        
                        // Generate watermark every 10th message
                        if message_id % 10 == 0 {
                            let watermark = Message::Watermark(crate::common::message::WatermarkMessage::new(
                                format!("watermark_{}", message_id),
                                message_id,
                                Some(message_id)
                            ));
                            sent_watermarks.get_mut(channel_id).unwrap().push(watermark.clone());
                            
                            // Send watermark immediately
                            batcher_clone.write_message(channel_id, watermark).await.unwrap();
                            total_sent += 1;
                            message_id += 1;
                        }
                    }
                    
                    if total_sent % 100 == 0 && total_sent > 0 {
                        println!("[DEBUG] Message generation: sent {} messages", total_sent);
                    }
                    
                    if max_message_rate_delay_ms > 0 {
                        let delay = rand::thread_rng().gen_range(0, max_message_rate_delay_ms + 1);
                        tokio::time::sleep(Duration::from_millis(delay)).await;
                    }
                }

                // send max watermark to each channel to notify of finish
                for channel_id in &channels {
                    let watermark = Message::Watermark(crate::common::message::WatermarkMessage::new(
                        format!("watermark_{}", message_id),
                        MAX_WATERMARK_VALUE,
                        Some(message_id)
                    ));
                    sent_watermarks.get_mut(channel_id).unwrap().push(watermark.clone());
                    batcher_clone.write_message(channel_id, watermark).await.unwrap();
                    total_sent += 1;
                    message_id += 1;
                }
                
                println!("[DEBUG] Message generation finished, total sent: {}", total_sent);
                (sent_messages, sent_watermarks)
            })
        };
        
        // Wait for message generation to complete
        let (final_sent_messages, final_sent_watermarks) = message_generation_handle.await.unwrap();
        
        // Close the batcher
        batcher.flush_and_close().await.unwrap();
        
        // Wait for receiver task to complete
        println!("[DEBUG] Waiting for receiver to complete...");
        let (final_received_messages, final_received_watermarks) = receiver_handle.await.unwrap();
        
        println!("[DEBUG] Receiver completed");

        // Verify results
        for channel_id in &channels {
            let sent_messages = &final_sent_messages[channel_id];
            let received_messages = &final_received_messages[channel_id];
            let sent_watermarks = &final_sent_watermarks[channel_id];
            let received_watermarks = &final_received_watermarks[channel_id];
            
            println!("Channel {}: Processing verification...", channel_id);
            
            // Verify watermarks separately
            assert_eq!(received_watermarks.len(), sent_watermarks.len(),
                       "Channel {} should have received {} watermarks, got {}", 
                       channel_id, sent_watermarks.len(), received_watermarks.len());
            
            // Verify watermark content
            for (i, (sent_wm, received_wm)) in sent_watermarks.iter().zip(received_watermarks.iter()).enumerate() {
                match (sent_wm, received_wm) {
                    (Message::Watermark(sent), Message::Watermark(received)) => {
                        assert_eq!(sent.watermark_value, received.watermark_value,
                                   "Watermark {} value mismatch", i);
                    }
                    _ => panic!("Expected watermark messages"),
                }
            }
            
            // Verify messages per key
            for (key, sent_msgs) in sent_messages {
                let received_msgs = received_messages.get(key).unwrap();
                
                println!("Channel {} Key {}: Sent {} messages, Received {} batched messages", 
                        channel_id, key, sent_msgs.len(), received_msgs.len());
                
                // Verify we have at least one received message for each key
                assert!(!received_msgs.is_empty(), 
                       "Channel {} Key {} should have received at least one batched message", 
                       channel_id, key);
                
                // Verify record-by-record matching with synchronized indexes
                let mut sent_msg_index = 0;
                let mut sent_row_index = 0;
                let mut received_msg_index = 0;
                let mut received_row_index = 0;
                
                while sent_msg_index < sent_msgs.len() && received_msg_index < received_msgs.len() {
                    let sent_msg = &sent_msgs[sent_msg_index];
                    let received_msg = &received_msgs[received_msg_index];
                    
                    let sent_rows = match sent_msg {
                        Message::Regular(base_msg) => &base_msg.record_batch,
                        Message::Keyed(keyed_msg) => &keyed_msg.base.record_batch,
                        Message::Watermark(_) => panic!("Unexpected watermark in sent messages"),
                    };
                    
                    let received_rows = match received_msg {
                        Message::Regular(base_msg) => &base_msg.record_batch,
                        Message::Keyed(keyed_msg) => &keyed_msg.base.record_batch,
                        Message::Watermark(_) => panic!("Unexpected watermark in batched messages"),
                    };
                    
                    let sent_ids = sent_rows.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                    let sent_data = sent_rows.column(1).as_any().downcast_ref::<StringArray>().unwrap();
                    let received_ids = received_rows.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                    let received_data = received_rows.column(1).as_any().downcast_ref::<StringArray>().unwrap();
                    
                    // Verify current row matches
                    assert_eq!(received_ids.value(received_row_index), sent_ids.value(sent_row_index),
                               "Record ID mismatch at sent_msg={}, sent_row={}, received_msg={}, received_row={}", 
                               sent_msg_index, sent_row_index, received_msg_index, received_row_index);
                    assert_eq!(received_data.value(received_row_index), sent_data.value(sent_row_index),
                               "Record data mismatch at sent_msg={}, sent_row={}, received_msg={}, received_row={}", 
                               sent_msg_index, sent_row_index, received_msg_index, received_row_index);
                    
                    // Move to next row
                    sent_row_index += 1;
                    received_row_index += 1;
                    
                    // Move to next message if we've processed all rows in current message
                    if sent_row_index >= sent_rows.num_rows() {
                        sent_msg_index += 1;
                        sent_row_index = 0;
                    }
                    if received_row_index >= received_rows.num_rows() {
                        received_msg_index += 1;
                        received_row_index = 0;
                    }
                }
                
                // Verify we processed all sent messages
                assert_eq!(sent_msg_index, sent_msgs.len(),
                           "Channel {} Key {} should have processed all {} sent messages, processed {}", 
                           channel_id, key, sent_msgs.len(), sent_msg_index);
            }
        }
        
        println!("E2E test completed successfully!");
        println!("Total messages sent: {}", 
                final_sent_messages.values().map(|channel_msgs| 
                    channel_msgs.values().map(|msgs| msgs.len()).sum::<usize>()
                ).sum::<usize>());
        println!("Total watermarks sent: {}", 
                final_sent_watermarks.values().map(|msgs| msgs.len()).sum::<usize>());
        println!("Total batched messages received: {}", 
                final_received_messages.values().map(|channel_msgs| 
                    channel_msgs.values().map(|msgs| msgs.len()).sum::<usize>()
                ).sum::<usize>());
        println!("Total watermarks received: {}", 
                final_received_watermarks.values().map(|msgs| msgs.len()).sum::<usize>());
    }

    #[tokio::test]
    async fn test_batcher_e2e_delayed() {
        test_batcher_e2e(4, 5, 150, 100, 10*1024*1024).await
    }

    #[tokio::test]
    async fn test_batcher_e2e_nodelay() {
        test_batcher_e2e(4, 5, 0, 100, 10*1024*1024).await
    }
}