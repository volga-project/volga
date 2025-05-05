use crate::transport::{
    transport_backend::TransportBackend,
    transport_client::{TransportClient, DataWriter, DataReader},
    channel::Channel,
};
use crate::common::data_batch::DataBatch;
use anyhow::Result;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::runtime::Runtime;

#[test]
fn test_transport_exchange() -> Result<()> {
    // Create a runtime for transport operations
    let runtime = Runtime::new()?;

    // Create test data
    let test_batches = vec![
        DataBatch::new(None, vec!["test1".to_string()]),
        DataBatch::new(None, vec!["test2".to_string()]),
        DataBatch::new(None, vec!["test3".to_string()]),
    ];

    // Create transport backend
    let transport_backend = Arc::new(Mutex::new(crate::transport::InMemoryTransportBackend::new()));

    // Create source and sink clients (using vertex IDs)
    let mut source_client = TransportClient::new("0".to_string());
    let mut sink_client = TransportClient::new("1".to_string());

    // Create a channel for communication between source and sink
    let channel = Channel::Local {
        channel_id: "0_to_1".to_string(),
    };

    // Register clients and channel in the transport backend
    runtime.block_on(async {
        let mut backend = transport_backend.lock().await;
        
        // Register clients
        backend.register_client("0".to_string(), source_client.clone()).await?;
        backend.register_client("1".to_string(), sink_client.clone()).await?;
        
        // Register channel (source -> sink)
        backend.register_channel("0".to_string(), channel.clone(), false).await?; // source output
        backend.register_channel("1".to_string(), channel.clone(), true).await?;  // sink input
        
        // Start the backend
        backend.start().await?;
        
        Ok::<(), anyhow::Error>(())
    })?;

    // Get writer and reader
    let mut writer = source_client.writer().expect("Writer should be initialized");
    let reader = sink_client.reader().expect("Reader should be initialized");

    // Write test data
    runtime.block_on(async {
        for batch in test_batches {
            writer.write_batch("0_to_1", batch).await?;
        }
        Ok::<(), anyhow::Error>(())
    })?;

    // Read and verify data
    let received_batches = runtime.block_on(async {
        let mut batches = Vec::new();
        for _ in 0..3 {
            if let Some(batch) = reader.read_batch().await? {
                batches.push(batch);
            }
        }
        Ok::<Vec<DataBatch>, anyhow::Error>(batches)
    })?;

    // Verify received data
    assert_eq!(received_batches.len(), 3);
    assert_eq!(received_batches[0].record_batch()[0], "test1");
    assert_eq!(received_batches[1].record_batch()[0], "test2");
    assert_eq!(received_batches[2].record_batch()[0], "test3");

    // Close the transport backend
    runtime.block_on(async {
        let mut backend = transport_backend.lock().await;
        backend.close().await
    })?;

    Ok(())
} 