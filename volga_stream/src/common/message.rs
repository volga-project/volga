use arrow::record_batch::RecordBatch;
use anyhow::Result;
use serde::{Serialize, Deserialize};
use std::io::{Cursor, Read, Write};

use super::Key;

#[derive(Debug, Clone)]
pub struct BaseMessage {
    // pub upstream_vertex_id: Option<String>,
    pub record_batch: RecordBatch,
    // pub ingest_timestamp: Option<u64>,
    pub metadata: MessageMetadata
}

impl BaseMessage {
    pub fn new(upstream_vertex_id: Option<String>, record_batch: RecordBatch, ingest_timestamp: Option<u64>) -> Self {
        Self {
            record_batch,
            metadata: MessageMetadata { upstream_vertex_id, ingest_timestamp}
        }
    }

    pub fn set_ingest_timestamp(&mut self, ingest_timestamp: u64) {
        self.metadata.ingest_timestamp = Some(ingest_timestamp);
    }

    /// Serialize the BaseMessage to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buffer = Vec::new();
        
        // Serialize metadata (everything except record_batch)
        let metadata_bytes = bincode::serialize(&self.metadata).unwrap();
        
        // Write metadata length and metadata
        buffer.write_all(&(metadata_bytes.len() as u32).to_le_bytes()).unwrap();
        buffer.write_all(&metadata_bytes).unwrap();
        
        // Create a separate buffer for Arrow IPC serialization
        let mut arrow_buffer = Vec::new();
        let mut writer = arrow::ipc::writer::FileWriter::try_new(
            Cursor::new(&mut arrow_buffer),
            self.record_batch.schema().as_ref(),
        ).unwrap();
        
        writer.write(&self.record_batch).unwrap();
        writer.finish().unwrap();
        
        // Append the Arrow IPC data to the main buffer
        buffer.extend_from_slice(&arrow_buffer);
        
        buffer
    }

    /// Deserialize BaseMessage from bytes
    pub fn from_bytes(bytes: &[u8]) -> Self {
        let mut cursor = Cursor::new(bytes);
        
        // Read metadata length
        let mut metadata_len_bytes = [0u8; 4];
        cursor.read_exact(&mut metadata_len_bytes).unwrap();
        let metadata_len = u32::from_le_bytes(metadata_len_bytes) as usize;
        
        // Read metadata
        let mut metadata_bytes = vec![0u8; metadata_len];
        cursor.read_exact(&mut metadata_bytes).unwrap();
        let metadata: MessageMetadata = bincode::deserialize(&metadata_bytes).unwrap();
        
        // Get the remaining bytes for Arrow IPC
        let position = cursor.position() as usize;
        let remaining_bytes = &bytes[position..];
        
        // Read RecordBatch using Arrow IPC
        let mut reader = arrow::ipc::reader::FileReader::try_new(
            Cursor::new(remaining_bytes),
            None,
        ).unwrap();
        
        let record_batch = match reader.next() {
            Some(Ok(batch)) => batch,
            Some(Err(e)) => panic!("Failed to read record batch: {}", e),
            None => panic!("No record batch found in serialized data"),
        };
        
        Self {
            record_batch,
            metadata
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MessageMetadata {
    upstream_vertex_id: Option<String>,
    ingest_timestamp: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct KeyedMessage {
    pub base: BaseMessage,
    pub key: Key,
}

impl KeyedMessage {
    pub fn new(base: BaseMessage, key: Key) -> Self {
        Self {
            base,
            key,
        }
    }

    pub fn key(&self) -> &Key {
        &self.key
    }

    /// Serialize the KeyedMessage to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buffer = Vec::new();
        
        // Serialize the base message
        let base_bytes = self.base.to_bytes();
        buffer.write_all(&(base_bytes.len() as u32).to_le_bytes()).unwrap();
        buffer.write_all(&base_bytes).unwrap();
        
        // Serialize the key
        let key_bytes = self.key.to_bytes();
        buffer.write_all(&(key_bytes.len() as u32).to_le_bytes()).unwrap();
        buffer.write_all(&key_bytes).unwrap();
        
        buffer
    }

    /// Deserialize KeyedMessage from bytes
    pub fn from_bytes(bytes: &[u8]) -> Self {
        let mut cursor = Cursor::new(bytes);
        
        // Read base message length and deserialize
        let mut base_len_bytes = [0u8; 4];
        cursor.read_exact(&mut base_len_bytes).unwrap();
        let base_len = u32::from_le_bytes(base_len_bytes) as usize;
        
        let mut base_bytes = vec![0u8; base_len];
        cursor.read_exact(&mut base_bytes).unwrap();
        let base = BaseMessage::from_bytes(&base_bytes);
        
        // Read key length and deserialize
        let mut key_len_bytes = [0u8; 4];
        cursor.read_exact(&mut key_len_bytes).unwrap();
        let key_len = u32::from_le_bytes(key_len_bytes) as usize;
        
        let mut key_bytes = vec![0u8; key_len];
        cursor.read_exact(&mut key_bytes).unwrap();
        let key = Key::from_bytes(&key_bytes);
        
        Self { base, key }
    }
}

pub const MAX_WATERMARK_VALUE: u64 = u64::MAX;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatermarkMessage {
    pub source_vertex_id: String,
    pub watermark_value: u64,
    pub ingest_timestamp: Option<u64>,
}

impl WatermarkMessage {
    pub fn new(source_vertex_id: String, watermark_value: u64) -> Self {
        Self {
            source_vertex_id,
            watermark_value,
            ingest_timestamp: None,
        }
    }

    pub fn set_ingest_timestamp(&mut self, ingest_timestamp: u64) {
        self.ingest_timestamp = Some(ingest_timestamp);
    }

    /// Serialize the WatermarkMessage to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }

    /// Deserialize WatermarkMessage from bytes
    pub fn from_bytes(bytes: &[u8]) -> Self {
        bincode::deserialize(bytes).unwrap()
    }
}

#[derive(Debug, Clone)]
pub enum Message {
    Regular(BaseMessage),
    Keyed(KeyedMessage),
    Watermark(WatermarkMessage),
}

impl Message {
    pub fn new(upstream_vertex_id: Option<String>, record_batch: RecordBatch, ingest_timestamp: Option<u64>) -> Self {
        Message::Regular(BaseMessage::new(upstream_vertex_id, record_batch, ingest_timestamp))
    }

    pub fn new_keyed(upstream_vertex_id: Option<String>, record_batch: RecordBatch, key: Key, ingest_timestamp: Option<u64>) -> Self {
        Message::Keyed(KeyedMessage::new(
            BaseMessage::new(upstream_vertex_id, record_batch, ingest_timestamp),
            key,
        ))
    }

    pub fn upstream_vertex_id(&self) -> Option<String> {
        match self {
            Message::Regular(message) => message.metadata.upstream_vertex_id.clone(),
            Message::Keyed(message) => message.base.metadata.upstream_vertex_id.clone(),
            Message::Watermark(message) => Some(message.source_vertex_id.clone()),
        }
    }

    pub fn record_batch(&self) -> &RecordBatch {
        match self {
            Message::Regular(message) => &message.record_batch,
            Message::Keyed(message) => &message.base.record_batch,
            Message::Watermark(_) => unreachable!("Watermark message does not have a record batch"),
        }
    }

    pub fn key(&self) -> Result<&Key> {
        match self {
            Message::Regular(_) => Err(anyhow::anyhow!("Regular message does not have a key")),
            Message::Keyed(message) => Ok(&message.key),
            Message::Watermark(_) => Err(anyhow::anyhow!("Watermark message does not have a key")),
        }
    }

    pub fn ingest_timestamp(&self) -> Option<u64> {
        match self {
            Message::Regular(message) => message.metadata.ingest_timestamp,
            Message::Keyed(message) => message.base.metadata.ingest_timestamp,
            Message::Watermark(message) => message.ingest_timestamp,
        }
    }

    pub fn set_ingest_timestamp(&mut self, ingest_timestamp: u64) {
        match self {
            Message::Regular(message) => message.set_ingest_timestamp(ingest_timestamp),
            Message::Keyed(message) => message.base.set_ingest_timestamp(ingest_timestamp),
            Message::Watermark(message) => message.set_ingest_timestamp(ingest_timestamp),
        }
    }

    /// Serialize the Message to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buffer = Vec::new();
        
        // Write message type discriminator
        let message_type = match self {
            Message::Regular(_) => 0u8,
            Message::Keyed(_) => 1u8,
            Message::Watermark(_) => 2u8,
        };
        buffer.write_all(&[message_type]).unwrap();
        
        // Serialize the specific message type
        match self {
            Message::Regular(msg) => {
                let msg_bytes = msg.to_bytes();
                buffer.write_all(&(msg_bytes.len() as u32).to_le_bytes()).unwrap();
                buffer.write_all(&msg_bytes).unwrap();
            }
            Message::Keyed(msg) => {
                let msg_bytes = msg.to_bytes();
                buffer.write_all(&(msg_bytes.len() as u32).to_le_bytes()).unwrap();
                buffer.write_all(&msg_bytes).unwrap();
            }
            Message::Watermark(msg) => {
                let msg_bytes = msg.to_bytes();
                buffer.write_all(&(msg_bytes.len() as u32).to_le_bytes()).unwrap();
                buffer.write_all(&msg_bytes).unwrap();
            }
        }
        
        buffer
    }

    /// Deserialize Message from bytes
    pub fn from_bytes(bytes: &[u8]) -> Self {
        let mut cursor = Cursor::new(bytes);
        
        // Read message type discriminator
        let mut message_type_bytes = [0u8; 1];
        cursor.read_exact(&mut message_type_bytes).unwrap();
        let message_type = message_type_bytes[0];
        
        // Read message length
        let mut msg_len_bytes = [0u8; 4];
        cursor.read_exact(&mut msg_len_bytes).unwrap();
        let msg_len = u32::from_le_bytes(msg_len_bytes) as usize;
        
        // Read message data
        let mut msg_bytes = vec![0u8; msg_len];
        cursor.read_exact(&mut msg_bytes).unwrap();
        
        // Deserialize based on message type
        match message_type {
            0 => {
                let base_msg = BaseMessage::from_bytes(&msg_bytes);
                Message::Regular(base_msg)
            }
            1 => {
                let keyed_msg = KeyedMessage::from_bytes(&msg_bytes);
                Message::Keyed(keyed_msg)
            }
            2 => {
                let watermark_msg = WatermarkMessage::from_bytes(&msg_bytes);
                Message::Watermark(watermark_msg)
            }
            _ => panic!("Unknown message type: {}", message_type),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{StringArray, Int64Array, Float64Array};
    use arrow::datatypes::{Schema, Field, DataType};
    use std::sync::Arc;

    #[test]
    fn test_regular_message_serialization() {
        // Create a test record batch
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));
        
        let record_batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
                Arc::new(Float64Array::from(vec![1.1, 2.2, 3.3])),
            ]
        ).unwrap();

        // Create a regular message
        let original_message = Message::new(
            Some("upstream_vertex".to_string()),
            record_batch,
            Some(1234567890)
        );

        // Test serialization and deserialization
        let bytes = original_message.to_bytes();
        let deserialized_message = Message::from_bytes(&bytes);

        // Verify the message was correctly deserialized
        assert_eq!(original_message.upstream_vertex_id(), deserialized_message.upstream_vertex_id());
        assert_eq!(original_message.ingest_timestamp(), deserialized_message.ingest_timestamp());
        
        // Compare record batches
        let original_batch = original_message.record_batch();
        let deserialized_batch = deserialized_message.record_batch();
        assert_eq!(original_batch.num_rows(), deserialized_batch.num_rows());
        assert_eq!(original_batch.num_columns(), deserialized_batch.num_columns());
        assert_eq!(original_batch.schema(), deserialized_batch.schema());
    }

    #[test]
    fn test_keyed_message_serialization() {
        // Create a test record batch
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        
        let record_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ]
        ).unwrap();

        // Create a key
        let key_batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![42])),
                Arc::new(StringArray::from(vec!["key_value"])),
            ]
        ).unwrap();
        let key = Key::new(key_batch).unwrap();

        // Create a keyed message
        let original_message = Message::new_keyed(
            Some("upstream_vertex".to_string()),
            record_batch,
            key.clone(),
            Some(1234567890)
        );

        // Test serialization and deserialization
        let bytes = original_message.to_bytes();
        let deserialized_message = Message::from_bytes(&bytes);

        // Verify the message was correctly deserialized
        assert_eq!(original_message.upstream_vertex_id(), deserialized_message.upstream_vertex_id());
        assert_eq!(original_message.ingest_timestamp(), deserialized_message.ingest_timestamp());
        
        // Compare keys
        let original_key = original_message.key().unwrap();
        let deserialized_key = deserialized_message.key().unwrap();
        assert_eq!(original_key, deserialized_key);
        
        // Compare record batches
        let original_batch = original_message.record_batch();
        let deserialized_batch = deserialized_message.record_batch();
        assert_eq!(original_batch.num_rows(), deserialized_batch.num_rows());
        assert_eq!(original_batch.num_columns(), deserialized_batch.num_columns());
    }

    #[test]
    fn test_watermark_message_serialization() {
        // Create a watermark message
        let mut original_message = Message::Watermark(WatermarkMessage::new(
            "source_vertex".to_string(),
            9876543210
        ));

        // Set ingest timestamp
        if let Message::Watermark(ref mut msg) = original_message {
            msg.set_ingest_timestamp(1234567890);
        }

        // Test serialization and deserialization
        let bytes = original_message.to_bytes();
        let deserialized_message = Message::from_bytes(&bytes);

        // Verify the message was correctly deserialized
        assert_eq!(original_message.upstream_vertex_id(), deserialized_message.upstream_vertex_id());
        assert_eq!(original_message.ingest_timestamp(), deserialized_message.ingest_timestamp());
        
        // Compare watermark specific fields
        if let (Message::Watermark(original), Message::Watermark(deserialized)) = 
            (&original_message, &deserialized_message) {
            assert_eq!(original.source_vertex_id, deserialized.source_vertex_id);
            assert_eq!(original.watermark_value, deserialized.watermark_value);
            assert_eq!(original.ingest_timestamp, deserialized.ingest_timestamp);
        } else {
            panic!("Expected watermark messages");
        }
    }
} 