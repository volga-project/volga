use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Mutex};
use tokio::runtime::Runtime;
use std::sync::atomic::{AtomicU64, Ordering};
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{Schema, Field, DataType};
use std::sync::Arc as StdArc;
use crate::common::message::Message;

#[derive(Debug, Clone, Copy)]
enum MessageType {
    Simple,
    Arrow,
}

#[derive(Debug, Clone)]
struct TestMessage {
    id: u64,
    timestamp: u64,
    data: String,
}

impl TestMessage {
    fn new(id: u64, data_size: usize) -> Self {
        let data: String = (0..data_size).map(|_| rand::random::<char>()).collect();
        Self {
            id,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            data,
        }
    }
}

// Union type for both message types
type MessageUnion = (Option<TestMessage>, Option<Message>);

#[derive(Debug)]
struct ProcessingMetrics {
    messages_processed: AtomicU64,
    total_latency_ns: AtomicU64,
    start_time: Instant,
}

impl ProcessingMetrics {
    fn new() -> Self {
        Self {
            messages_processed: AtomicU64::new(0),
            total_latency_ns: AtomicU64::new(0),
            start_time: Instant::now(),
        }
    }

    fn record_message(&self, latency_ns: u64) {
        self.messages_processed.fetch_add(1, Ordering::Relaxed);
        self.total_latency_ns.fetch_add(latency_ns, Ordering::Relaxed);
    }

    fn get_stats(&self) -> (u64, f64, f64) {
        let processed = self.messages_processed.load(Ordering::Relaxed);
        let total_latency = self.total_latency_ns.load(Ordering::Relaxed);
        let elapsed_secs = self.start_time.elapsed().as_secs_f64();
        
        let throughput = if elapsed_secs > 0.0 {
            processed as f64 / elapsed_secs
        } else {
            0.0
        };
        
        let avg_latency_ms = if processed > 0 {
            (total_latency / processed) as f64 / 1_000_000.0
        } else {
            0.0
        };
        
        (processed, throughput, avg_latency_ms)
    }
}

fn create_test_record_batch(id: u64, data_size: usize) -> arrow::record_batch::RecordBatch {
    let schema = StdArc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("data", DataType::Utf8, false),
    ]));
    
    let data_string: String = (0..data_size).map(|_| rand::random::<char>()).collect();
    // let data_string = "x".repeat(data_size);
    
    arrow::record_batch::RecordBatch::try_new(
        schema,
        vec![
            StdArc::new(Int64Array::from(vec![id as i64])),
            StdArc::new(StringArray::from(vec![data_string])),
        ]
    ).unwrap()
}

async fn process_task(
    task_id: usize,
    mut input_rx: mpsc::Receiver<MessageUnion>,
    output_tx: mpsc::Sender<MessageUnion>,
    metrics: Arc<ProcessingMetrics>,
    processing_delay_ms: u64,
) {
    println!("[Task {}] Starting processing task", task_id);
    
    while let Some(mut message_union) = input_rx.recv().await {
        let start_time = Instant::now();
        
        // Simulate processing work
        if processing_delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(processing_delay_ms)).await;
        }
        
        // Process based on which message type is present
        let new_timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        
        match message_union {
            (Some(mut test_msg), None) => {
                // Simple message
                test_msg.timestamp = new_timestamp;
                message_union = (Some(test_msg), None);
            }
            (None, Some(mut arrow_msg)) => {
                // Arrow message
                arrow_msg.set_ingest_timestamp(new_timestamp);
                message_union = (None, Some(arrow_msg));
            }
            _ => panic!("Invalid message union state"),
        }
        
        // Send to next stage
        if let Err(e) = output_tx.send(message_union).await {
            eprintln!("[Task {}] Failed to send message: {}", task_id, e);
            break;
        }
        
        // Record metrics
        let latency_ns = start_time.elapsed().as_nanos() as u64;
        metrics.record_message(latency_ns);
    }
    
    println!("[Task {}] Processing task finished", task_id);
}

async fn writer_task(
    num_messages: u64,
    message_size: usize,
    tx: mpsc::Sender<MessageUnion>,
    metrics: Arc<ProcessingMetrics>,
    message_type: MessageType,
) {
    println!("[Writer] Starting writer task, sending {} messages", num_messages);
    
    for i in 0..num_messages {
        let message_union = match message_type {
            MessageType::Simple => {
                let test_msg = TestMessage::new(i, message_size);
                (Some(test_msg), None)
            }
            MessageType::Arrow => {
                let record_batch = create_test_record_batch(i, message_size);
                let arrow_msg = Message::new(None, record_batch, None);
                (None, Some(arrow_msg))
            }
        };
        
        if let Err(e) = tx.send(message_union).await {
            eprintln!("[Writer] Failed to send message {}: {}", i, e);
            break;
        }
        
        // Record metrics
        metrics.record_message(0); // No processing latency for writer
    }
    
    println!("[Writer] Writer task finished");
}

async fn reader_task(
    mut rx: mpsc::Receiver<MessageUnion>,
    metrics: Arc<ProcessingMetrics>,
    expected_messages: u64,
) {
    println!("[Reader] Starting reader task, expecting {} messages", expected_messages);
    
    let mut received = 0u64;
    while let Some(message_union) = rx.recv().await {
        received += 1;
        
        // Simple validation
        if received % 1000 == 0 {
            match message_union {
                (Some(test_msg), None) => {
                    // println!("[Reader] Received simple message {}: id={}, timestamp={}", 
                    //          received, test_msg.id, test_msg.timestamp);
                }
                (None, Some(arrow_msg)) => {
                    let timestamp = arrow_msg.ingest_timestamp().unwrap_or(0);
                    // println!("[Reader] Received arrow message {}: timestamp={}", 
                    //          received, timestamp);
                }
                _ => panic!("Invalid message union state"),
            }
        }
        
        // Record metrics
        metrics.record_message(0); // No processing latency for reader
        
        if received >= expected_messages {
            break;
        }
    }
    
    println!("[Reader] Reader task finished, received {} messages", received);
}

async fn run_benchmark(message_type: MessageType) {
    let num_messages = 500_000;
    let message_size = 10;
    let processing_delay_ms = 0; // No artificial delay for max throughput
    let channel_buffer_size = 100;
    
    let message_type_str = match message_type {
        MessageType::Simple => "Simple",
        MessageType::Arrow => "Arrow",
    };
    
    println!("=== {} Message Flow Benchmark ===", message_type_str);
    println!("Messages: {}", num_messages);
    println!("Message size: {} bytes", message_size);
    println!("Processing delay: {}ms", processing_delay_ms);
    println!("Channel buffer: {}", channel_buffer_size);
    
    // Create channels for 2-task chain: Writer -> Reader
    let (tx, rx): (mpsc::Sender<MessageUnion>, mpsc::Receiver<MessageUnion>) = mpsc::channel(channel_buffer_size);
    
    // Create metrics for each task
    let writer_metrics = Arc::new(ProcessingMetrics::new());
    let reader_metrics = Arc::new(ProcessingMetrics::new());
    
    // Create separate runtimes for each task (simulating your worker runtimes)
    let writer_runtime = Runtime::new().unwrap();
    let reader_runtime = Runtime::new().unwrap();
    
    let start_time = Instant::now();
    
    // Spawn tasks in separate runtimes
    let writer_handle = writer_runtime.spawn(writer_task(
        num_messages,
        message_size,
        tx,
        writer_metrics.clone(),
        message_type,
    ));
    
    let reader_handle = reader_runtime.spawn(reader_task(
        rx,
        reader_metrics.clone(),
        num_messages,
    ));
    
    // Wait for both tasks to complete
    let _ = tokio::join!(writer_handle, reader_handle);
    
    let total_duration = start_time.elapsed();
    
    // Calculate and print results
    let (writer_processed, writer_throughput, writer_latency) = writer_metrics.get_stats();
    let (reader_processed, reader_throughput, reader_latency) = reader_metrics.get_stats();
    
    println!("\n=== {} Message Results ===", message_type_str);
    println!("Total duration: {:?}", total_duration);
    println!("Overall throughput: {:.2} messages/sec", 
             num_messages as f64 / total_duration.as_secs_f64());
    println!("Overall throughput: {:.2} MB/sec", 
             (num_messages * message_size as u64) as f64 / total_duration.as_secs_f64() / 1_048_576.0);
    
    println!("\nWriter Task:");
    println!("  Messages processed: {}", writer_processed);
    println!("  Throughput: {:.2} messages/sec", writer_throughput);
    println!("  Avg latency: {:.2} ms", writer_latency);
    
    println!("\nReader Task:");
    println!("  Messages processed: {}", reader_processed);
    println!("  Throughput: {:.2} messages/sec", reader_throughput);
    println!("  Avg latency: {:.2} ms", reader_latency);
    
    // Cleanup
    writer_runtime.shutdown_background();
    reader_runtime.shutdown_background();
}

#[tokio::test]
async fn benchmark_engine_flow() {
    // Run both message types for comparison
    // run_benchmark(MessageType::Simple).await;
    // println!("\n{}", "=".repeat(80));
    run_benchmark(MessageType::Arrow).await;
}

