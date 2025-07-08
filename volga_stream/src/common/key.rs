use anyhow::Result;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use std::io::{Cursor, Read, Write};
use arrow::array::{
    Array, StringArray, Int64Array, Int32Array, Int16Array, Int8Array,
    UInt64Array, UInt32Array, UInt16Array, UInt8Array,
    Float64Array, Float32Array, Date32Array, Date64Array, BinaryArray,
    TimestampSecondArray, TimestampMillisecondArray,
    TimestampMicrosecondArray, TimestampNanosecondArray,
    Decimal128Array, Decimal256Array, LargeBinaryArray,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

/// A Key implementation that directly uses a RecordBatch for storage and efficient hashing
#[derive(Debug, Clone)]
pub struct Key {
    /// The RecordBatch containing a single row of key data
    pub record_batch: RecordBatch,
    /// Precomputed hash
    pub hash: u64,
}

// TODO implement all arrow DataTypes
impl Key {
    /// Creates a new Key from a RecordBatch with a single row
    pub fn new(record_batch: RecordBatch) -> Result<Self> {
        // Check that the record batch has exactly one row
        if record_batch.num_rows() != 1 {
            return Err(anyhow::anyhow!(
                "Key must have exactly one row, found {}",
                record_batch.num_rows()
            ));
        }
        
        // Compute hash for the key row
        let hash = Self::compute_hash(&record_batch)?;
        
        Ok(Self { record_batch, hash })
    }
    
    pub fn hash(&self) -> u64 {
        self.hash
    }
    
    pub fn record_batch(&self) -> &RecordBatch {
        &self.record_batch
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
            self.record_batch.schema().as_ref(),
        ).unwrap();
        
        writer.write(&self.record_batch).unwrap();
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
        
        Self { record_batch, hash }
    }
    
    fn compute_hash(batch: &RecordBatch) -> Result<u64> {
        // We'll use a simple hash algorithm:
        // 1. Hash each column value
        // 2. Combine the hashes
        
        // Start with a seed value
        let mut hash = 0u64;
        
        // Hash each column
        for i in 0..batch.num_columns() {
            let array = batch.column(i);
            
            // Hash each column value - we know there's only one row (at index 0)
            // Use a different approach based on array type
            let column_hash = match array.data_type() {
                DataType::Utf8 => {
                    let str_array = array.as_any().downcast_ref::<StringArray>()
                        .ok_or_else(|| anyhow::anyhow!("Failed to downcast to StringArray"))?;
                    
                    if str_array.is_null(0) {
                        0u64 // Special value for NULL
                    } else {
                        let mut hasher = DefaultHasher::new();
                        str_array.value(0).hash(&mut hasher);
                        hasher.finish()
                    }
                },
                DataType::Int64 => {
                    let int_array = array.as_any().downcast_ref::<Int64Array>()
                        .ok_or_else(|| anyhow::anyhow!("Failed to downcast to Int64Array"))?;
                    
                    if int_array.is_null(0) {
                        0u64 // Special value for NULL
                    } else {
                        let mut hasher = DefaultHasher::new();
                        int_array.value(0).hash(&mut hasher);
                        hasher.finish()
                    }
                },
                DataType::Int32 => {
                    let int_array = array.as_any().downcast_ref::<Int32Array>()
                        .ok_or_else(|| anyhow::anyhow!("Failed to downcast to Int32Array"))?;
                    
                    if int_array.is_null(0) {
                        0u64 // Special value for NULL
                    } else {
                        let mut hasher = DefaultHasher::new();
                        int_array.value(0).hash(&mut hasher);
                        hasher.finish()
                    }
                },
                DataType::Float64 => {
                    let float_array = array.as_any().downcast_ref::<Float64Array>()
                        .ok_or_else(|| anyhow::anyhow!("Failed to downcast to Float64Array"))?;
                    
                    if float_array.is_null(0) {
                        0u64 // Special value for NULL
                    } else {
                        let mut hasher = DefaultHasher::new();
                        // Use to_bits() to handle NaN consistently
                        float_array.value(0).to_bits().hash(&mut hasher);
                        hasher.finish()
                    }
                },
                DataType::Float32 => {
                    let float_array = array.as_any().downcast_ref::<Float32Array>()
                        .ok_or_else(|| anyhow::anyhow!("Failed to downcast to Float32Array"))?;
                    
                    if float_array.is_null(0) {
                        0u64 // Special value for NULL
                    } else {
                        let mut hasher = DefaultHasher::new();
                        // Use to_bits() to handle NaN consistently
                        float_array.value(0).to_bits().hash(&mut hasher);
                        hasher.finish()
                    }
                },
                // Add other type-specific cases as needed
                _ => {
                    // For other types, we'll convert to a string representation and hash that
                    // This is not ideal for performance but ensures we can handle any type
                    if array.is_null(0) {
                        0u64 // Special value for NULL
                    } else {
                        // Create a simple string representation of the value
                        let value_str = format!("{:?}", array);
                        
                        let mut hasher = DefaultHasher::new();
                        value_str.hash(&mut hasher);
                        hasher.finish()
                    }
                }
            };
            
            // Combine with the running hash
            hash = hash.wrapping_mul(31).wrapping_add(column_hash);
        }
        
        Ok(hash)
    }

    pub fn get_memory_size(&self) -> usize {
        self.record_batch.get_array_memory_size() + 8 // 8 bytes for u64 hash
    }
}

impl PartialEq for Key {
    fn eq(&self, other: &Self) -> bool {
        // First compare hash values directly
        if self.hash != other.hash {
            return false;
        }
        
        // If hashes are equal, compare record batches
        self.record_batch == other.record_batch
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
        // assert_eq!(original_key, deserialized_key);
        assert_eq!(original_key.hash(), deserialized_key.hash());
        
        // Verify record batch properties
        let original_batch = original_key.record_batch();
        let deserialized_batch = deserialized_key.record_batch();
        assert_eq!(original_batch.num_rows(), deserialized_batch.num_rows());
        assert_eq!(original_batch.num_columns(), deserialized_batch.num_columns());
        assert_eq!(original_batch.schema(), deserialized_batch.schema());
        
        // Test equality and hashing consistency
        // let key2 = Key::new(deserialized_batch.clone()).unwrap();
        // assert_eq!(original_key, key2);
        // assert_eq!(original_key.hash(), key2.hash());
    }
} 