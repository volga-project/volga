use anyhow::Result;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
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
    record_batch: RecordBatch,
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
    use arrow::array::{StringArray, Int64Array};
    use arrow::datatypes::{Schema, Field, DataType};
    use std::sync::Arc;

    #[test]
    fn test_key_equality_and_hashing() {
        // Create record batches for testing
        let schema1 = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
        ]));
        
        let batch1 = RecordBatch::try_new(
            schema1.clone(),
            vec![Arc::new(Int64Array::from(vec![42]))]
        ).unwrap();
        
        let batch2 = RecordBatch::try_new(
            schema1.clone(),
            vec![Arc::new(Int64Array::from(vec![42]))]
        ).unwrap();
        
        let batch3 = RecordBatch::try_new(
            schema1,
            vec![Arc::new(Int64Array::from(vec![99]))]
        ).unwrap();
        
        // Create keys
        let key1 = Key::new(batch1).unwrap();
        let key2 = Key::new(batch2).unwrap();
        let key3 = Key::new(batch3).unwrap();
        
        // Test equality
        assert_eq!(key1, key2); // Same values should be equal
        assert_ne!(key1, key3); // Different values should not be equal
        
        // Test hashing works consistently
        let mut hasher1 = DefaultHasher::new();
        let mut hasher2 = DefaultHasher::new();
        hasher1.write_u64(key1.hash);
        hasher2.write_u64(key2.hash);
        assert_eq!(hasher1.finish(), hasher2.finish());
    }
    
    #[test]
    fn test_multi_column_key() {
        
        let key_batch1 = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["a"])),
            ]
        ).unwrap();
        
        let key_batch2 = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["a"])),
            ]
        ).unwrap();
        
        let key1 = Key::new(key_batch1).unwrap();
        let key2 = Key::new(key_batch2).unwrap();
        
        assert_eq!(key1, key2);
    }
} 