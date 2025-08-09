use ahash::RandomState;
use anyhow::Result;
use datafusion::common::hash_utils::create_hashes;
use std::hash::{Hash, Hasher};
use std::io::{Cursor, Read, Write};
use std::sync::Arc;
use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;

/// A Key implementation that directly uses a RecordBatch for storage and efficient hashing
#[derive(Debug, Clone)]
pub struct Key {
    /// The RecordBatch containing a single row of key data
    pub key_record_batch: RecordBatch,
    /// Precomputed hash
    pub hash: u64,
}

impl Key {
    /// Creates a new Key from a RecordBatch with a single row
    pub fn new(key_record_batch: RecordBatch) -> Result<Self> {
        // Check that the record batch has exactly one row
        if key_record_batch.num_rows() != 1 {
            return Err(anyhow::anyhow!(
                "Key must have exactly one row, found {}",
                key_record_batch.num_rows()
            ));
        }
        
        // Compute hash for the key row
        let arrays: Vec<ArrayRef> = key_record_batch.columns().iter().map(|c| Arc::clone(c)).collect();
        let random_state = RandomState::with_seeds(0, 0, 0, 0);
        let mut hashes_buffer = vec![0u64; 1];
        
        let hashes = create_hashes(&arrays, &random_state, &mut hashes_buffer)
            .map_err(|e| anyhow::anyhow!("Failed to create hashes: {}", e))?;
        
        // we know there is only one row
        let hash = hashes[0];
        Ok(Self { key_record_batch, hash })
    }
    
    pub fn hash(&self) -> u64 {
        self.hash
    }
    
    pub fn record_batch(&self) -> &RecordBatch {
        &self.key_record_batch
    }

    /// Serialize the Key to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buffer = Vec::new();
        
        // Write hash
        buffer.write_all(&self.hash.to_le_bytes()).unwrap();
        
        // Create a separate buffer for Arrow IPC serialization
        let mut arrow_buffer = Vec::new();
        let mut writer = arrow::ipc::writer::FileWriter::try_new(
            Cursor::new(&mut arrow_buffer),
            self.key_record_batch.schema().as_ref(),
        ).unwrap();
        
        writer.write(&self.key_record_batch).unwrap();
        writer.finish().unwrap();
        
        // Append the Arrow IPC data to the main buffer
        buffer.extend_from_slice(&arrow_buffer);
        
        buffer
    }

    /// Deserialize Key from bytes
    pub fn from_bytes(bytes: &[u8]) -> Self {
        let mut cursor = Cursor::new(bytes);
        
        // Read hash (u64 is always 8 bytes)
        let mut hash_bytes = [0u8; 8];
        cursor.read_exact(&mut hash_bytes).unwrap();
        let hash = u64::from_le_bytes(hash_bytes);
        
        let remaining_bytes = &bytes[8..];
        let mut reader = arrow::ipc::reader::FileReader::try_new(
            Cursor::new(remaining_bytes),
            None,
        ).unwrap();
        
        let record_batch = match reader.next() {
            Some(Ok(batch)) => batch,
            Some(Err(e)) => panic!("Failed to read record batch: {}", e),
            None => panic!("No record batch found in serialized data"),
        };
        
        Self { key_record_batch: record_batch, hash }
    }

    pub fn get_memory_size(&self) -> usize {
        self.key_record_batch.get_array_memory_size() + 8 // 8 bytes for u64 hash
    }
}

impl PartialEq for Key {
    fn eq(&self, other: &Self) -> bool {
        // First compare hash values directly
        if self.hash != other.hash {
            return false;
        }
        
        // If hashes are equal, compare record batches
        self.key_record_batch == other.key_record_batch
    }
}

impl Eq for Key {}

impl Hash for Key {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Use the precomputed hash
        self.hash.hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{StringArray, Int64Array, Int32Array, Float64Array, Float32Array};
    use arrow::datatypes::{Schema, Field, DataType};
    use std::sync::Arc;

    #[test]
    fn test_key_serialization() {

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
            Field::new("active", DataType::Int32, false),
            Field::new("score", DataType::Float32, false),
        ]));
        
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![42])),
                Arc::new(StringArray::from(vec!["test_key"])),
                Arc::new(Float64Array::from(vec![3.14159])),
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(Float32Array::from(vec![2.71828])),
            ]
        ).unwrap();
        
        let original_key = Key::new(batch).unwrap();
        
        // Test serialization and deserialization
        let bytes = original_key.to_bytes();
        let deserialized_key = Key::from_bytes(&bytes);
        
        // Verify the key was correctly deserialized
        assert_eq!(original_key.hash(), deserialized_key.hash());
        
        // Verify record batch properties
        let original_batch = original_key.record_batch();
        let deserialized_batch = deserialized_key.record_batch();
        assert_eq!(original_batch.num_rows(), deserialized_batch.num_rows());
        assert_eq!(original_batch.num_columns(), deserialized_batch.num_columns());
        assert_eq!(original_batch.schema(), deserialized_batch.schema());
    }
} 