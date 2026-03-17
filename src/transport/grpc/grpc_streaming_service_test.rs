use crate::common::message::Message;
use crate::common::key::Key;
use crate::transport::grpc::grpc_streaming_service::{
    MessageStreamClient, MessageStreamServiceImpl,
    message_stream::message_stream_service_server::MessageStreamServiceServer,
};
use arrow::array::{StringArray, Int64Array};
use arrow::datatypes::{Schema, Field, DataType};
use tonic::transport::Server;
use std::sync::Arc;
use tokio::sync::mpsc;
use std::collections::HashMap;

/// Start the gRPC server with custom shutdown signal
pub async fn start_server(
    addr: String,
    tx: mpsc::Sender<(Message, String)>,
    shutdown: tokio::sync::oneshot::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("[SERVER] Starting gRPC server on {}", addr);
    let addr = addr.parse()?;
    let service = MessageStreamServiceImpl::new(tx);
    let svc = MessageStreamServiceServer::new(service);

    println!("[SERVER] gRPC server listening on {}", addr);
    let router = Server::builder().add_service(svc);

    // Graceful shutdown on custom signal
    router
        .serve_with_shutdown(addr, async {
            let _ = shutdown.await;
            println!("[SERVER] Received shutdown signal");
        })
        .await?;

    println!("[SERVER] Server stopped");
    Ok(())
}

/// Start the gRPC client
pub async fn start_client(
    server_addr: String,
    rx: mpsc::Receiver<(Message, String)>,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("[CLIENT] Starting gRPC client");
    let mut client = MessageStreamClient::connect(server_addr).await?;
    client.stream_messages(rx).await?;
    println!("[CLIENT] Client completed");
    Ok(())
} 

pub async fn stream_grpc_many_clients_one_server() {
    println!("[TEST] Starting multi-client Arrow message streaming example");
    let server_addr = "127.0.0.1:50053";
    
    // Create channels for server communication
    let (server_tx, mut server_rx) = mpsc::channel::<(Message, String)>(100);
    
    // Create shutdown channel
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    
    println!("[TEST] Starting server task");
    // Start server with shutdown capability
    let server_handle = tokio::spawn(async move {
        start_server(
            server_addr.to_string(), 
            server_tx, 
            shutdown_rx
        ).await.unwrap();
    });
    
    println!("[TEST] Waiting for server to start");
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    // Create multiple clients with different channel IDs
    let client_configs = vec![
        ("client1".to_string(), "channel1".to_string(), 10),
        ("client2".to_string(), "channel2".to_string(), 10),
        ("client3".to_string(), "channel3".to_string(), 10),
        ("client4".to_string(), "channel4".to_string(), 10),
    ];
    
    let mut client_handles = Vec::new();
    let mut client_senders = Vec::new();
    let mut sent_messages = HashMap::new();
    
    // Start all clients first
    for (client_name, channel_id, message_count) in &client_configs {
        println!("[TEST] Starting client: {} on channel: {} with {} messages", 
                client_name, channel_id, message_count);
        
        let (client_tx, client_rx) = mpsc::channel::<(Message, String)>(100);
        
        // Start client task
        let client_handle = tokio::spawn(async move {
            start_client(format!("http://{}", server_addr), client_rx).await.unwrap();
        });
        client_handles.push(client_handle);
        
        // Store sender for later use
        client_senders.push((client_name.clone(), channel_id.clone(), *message_count, client_tx));
    }
    
    println!("[TEST] All clients started, now sending messages simultaneously");
    
    // Send messages from all clients simultaneously
    let mut send_tasks = Vec::new();
    for (client_name, channel_id, message_count, client_tx) in client_senders {
        let send_task = tokio::spawn(async move {
            let mut client_sent_messages = Vec::new();
            
            for i in 0..message_count {
                let message = create_test_message_for_client(&client_name, i);
                
                // Store sent message for verification
                client_sent_messages.push((i, message.clone()));
                
                println!("[TEST] Client {} sending message {} to channel {}", 
                        client_name, i + 1, channel_id);
                if let Err(e) = client_tx.send((message, channel_id.clone())).await {
                    eprintln!("[TEST] Failed to send message from {}: {}", client_name, e);
                }
            }
            
            // Store all sent messages for this channel
            (channel_id, client_sent_messages)
        });
        send_tasks.push(send_task);
    }
    
    // Wait for all send tasks to complete and collect sent messages
    for send_task in send_tasks {
        let (channel_id, client_sent_messages) = send_task.await.unwrap();
        sent_messages.insert(channel_id, client_sent_messages);
    }
    
    println!("[TEST] All clients started, waiting for messages on server side");
    
    // Receive and process messages on server
    let mut received_messages = HashMap::new();
    let mut total_received = 0;
    
    while let Some((message, channel_id)) = server_rx.recv().await {
        total_received += 1;
        
        println!("[TEST] Received message {} on channel: {}", total_received, channel_id);
        
        // Store received message for verification
        received_messages.entry(channel_id.clone()).or_insert_with(Vec::new).push(message.clone());
        
        match &message {
            Message::Regular(base_msg) => {
                println!("[TEST] Regular message with {} rows", base_msg.record_batch.num_rows());
            }
            Message::Keyed(keyed_msg) => {
                println!("[TEST] Keyed message with key hash: {}", keyed_msg.key.hash());
            }
            Message::Watermark(watermark_msg) => {
                println!("[TEST] Watermark message: {} -> {}", 
                        watermark_msg.metadata.upstream_vertex_id.as_ref().unwrap(), watermark_msg.watermark_value);
            }
            Message::CheckpointBarrier(barrier_msg) => {
                println!(
                    "[TEST] Checkpoint barrier: {} -> {}",
                    barrier_msg.metadata.upstream_vertex_id.as_ref().unwrap(),
                    barrier_msg.checkpoint_id
                );
            }
        }
        
        // Check if we've received all expected messages
        let total_expected: usize = sent_messages.values().map(|v| v.len()).sum();
        if total_received >= total_expected {
            println!("[TEST] Received all expected messages ({}), stopping", total_expected);
            break;
        }
    }
    
    println!("[TEST] Message reception completed");
    println!("[TEST] Total messages received: {}", total_received);
    
    // Verify message counts per channel
    println!("[TEST] Verifying message counts per channel:");
    for (channel_id, sent_messages_for_channel) in &sent_messages {
        let expected_count = sent_messages_for_channel.len();
        let received_count = received_messages.get(channel_id).map(|v| v.len()).unwrap_or(0);
        println!("[TEST] Channel {}: expected {}, received {}", channel_id, expected_count, received_count);
        assert_eq!(received_count, expected_count, 
                   "Message count mismatch for channel {}", channel_id);
    }
    
    // Verify actual message content matches
    println!("[TEST] Verifying message content matches:");
    for (channel_id, sent_messages_for_channel) in &sent_messages {
        let received_messages_for_channel = received_messages.get(channel_id)
            .expect(&format!("No received messages for channel {}", channel_id));
        
        println!("[TEST] Verifying channel {}: {} messages", channel_id, sent_messages_for_channel.len());
        
        // Verify message content matches
        for (i, (_, sent_message)) in sent_messages_for_channel.iter().enumerate() {
            let received_message = &received_messages_for_channel[i];
            
            println!("[TEST] Verifying channel {} message {}: {:?} type", channel_id, i + 1, 
                    get_message_type(sent_message));
            
            // Verify message types match
            assert_eq!(get_message_type(sent_message), get_message_type(received_message),
                       "Message type mismatch for channel {} message {}", channel_id, i + 1);
            
            // Verify specific content based on message type
            match (sent_message, received_message) {
                (Message::Regular(sent), Message::Regular(received)) => {
                    assert_eq!(sent.record_batch.num_rows(), received.record_batch.num_rows(),
                              "Row count mismatch for channel {} message {}", channel_id, i + 1);
                    assert_eq!(sent_message.upstream_vertex_id(), received_message.upstream_vertex_id(),
                              "Upstream vertex ID mismatch for channel {} message {}", channel_id, i + 1);
                }
                (Message::Keyed(sent), Message::Keyed(received)) => {
                    assert_eq!(sent.key.hash(), received.key.hash(),
                              "Key hash mismatch for channel {} message {}", channel_id, i + 1);
                    assert_eq!(sent.base.record_batch.num_rows(), received.base.record_batch.num_rows(),
                              "Row count mismatch for channel {} message {}", channel_id, i + 1);
                }
                (Message::Watermark(sent), Message::Watermark(received)) => {
                    assert_eq!(sent.metadata.upstream_vertex_id, received.metadata.upstream_vertex_id,
                              "Source vertex ID mismatch for channel {} message {}", channel_id, i + 1);
                    assert_eq!(sent.watermark_value, received.watermark_value,
                              "Watermark value mismatch for channel {} message {}", channel_id, i + 1);
                }
                _ => panic!("Message type mismatch for channel {} message {}", channel_id, i + 1),
            }
            
            println!("[TEST] Channel {} message {} verification passed", channel_id, i + 1);
        }
    }
    
    // Trigger server shutdown after all messages are received
    println!("[TEST] Triggering server shutdown");
    if let Err(_) = shutdown_tx.send(()) {
        eprintln!("[TEST] Failed to send shutdown signal");
    }
    
    println!("[TEST] Waiting for server and client tasks to complete");
    // Wait for all tasks to complete
    let mut all_handles = vec![server_handle];
    all_handles.extend(client_handles);
    
    // Wait for all handles to complete
    for handle in all_handles {
        let _ = handle.await;
    }
    println!("[TEST] Multi-client example completed successfully");
}

#[derive(Debug, PartialEq, Clone)]
enum MessageType {
    Regular,
    Keyed,
    Watermark,
    CheckpointBarrier,
}

fn get_message_type(message: &Message) -> MessageType {
    match message {
        Message::Regular(_) => MessageType::Regular,
        Message::Keyed(_) => MessageType::Keyed,
        Message::Watermark(_) => MessageType::Watermark,
        Message::CheckpointBarrier(_) => MessageType::CheckpointBarrier,
    }
}

fn create_test_message_for_client(client_name: &str, index: usize) -> Message {
    // Create schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    
    // Round-robin through message types based on index
    match index % 3 {
        0 => {
            // Regular message
            let record_batch = arrow::record_batch::RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from(vec![index as i64, (index + 1) as i64, (index + 2) as i64])),
                    Arc::new(StringArray::from(vec![
                        format!("{}_{}", client_name, index),
                        format!("{}_{}", client_name, index + 1),
                        format!("{}_{}", client_name, index + 2),
                    ])),
                ]
            ).unwrap();
            
            Message::new(
                Some(format!("upstream_{}", client_name)),
                record_batch,
                Some(1234567890 + index as u64),
                None
            )
        }
        1 => {
            // Keyed message
            let key_batch = arrow::record_batch::RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![index as i64])),
                    Arc::new(StringArray::from(vec![format!("{}_key_{}", client_name, index)])),
                ]
            ).unwrap();
            let key = Key::new(key_batch).unwrap();
            
            let data_batch = arrow::record_batch::RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from(vec![index as i64 * 10, (index + 1) as i64 * 10, (index + 2) as i64 * 10])),
                    Arc::new(StringArray::from(vec![
                        format!("{}_data_{}", client_name, index),
                        format!("{}_data_{}", client_name, index + 1),
                        format!("{}_data_{}", client_name, index + 2),
                    ])),
                ]
            ).unwrap();
            
            Message::new_keyed(
                Some(format!("upstream_{}", client_name)),
                data_batch,
                key,
                Some(1234567890 + index as u64),
                None
            )
        }
        2 => {
            // Watermark message
            Message::Watermark(
                crate::common::message::WatermarkMessage::new(
                    format!("source_{}", client_name),
                    9876543210 + index as u64,
                    Some(1234567890 + index as u64)
                )
            )
        }
        _ => unreachable!(),
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_stream_grpc() {
        stream_grpc_many_clients_one_server().await;
    }
} 