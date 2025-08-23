use std::collections::HashMap;
use arrow::record_batch::RecordBatch;
use foyer::{Cache, CacheBuilder};

fn batch_weighter(_key: &Vec<u8>, value: &Vec<RecordBatch>) -> usize {
    value.iter().map(|batch| {
        batch.get_array_memory_size()
    }).sum()
}


#[derive(Debug)]
pub struct InputBuffer {
    cache: Cache<Vec<u8>, Vec<RecordBatch>>,
}

impl InputBuffer {
    pub fn new() -> Self {
        let cache = CacheBuilder::new(1024 * 1024 * 1024) // 1GB capacity
            .with_weighter(batch_weighter)
            .build();
        
        Self { cache }
    }

    // TODO this should backpressure when memory is full
    pub fn add_batch(&mut self, key: Vec<u8>, batch: RecordBatch) {
        let mut batches = self.cache.get(&key)
            .map(|entry| entry.value().clone())
            .unwrap_or_default();
        
        batches.push(batch);
        self.cache.insert(key, batches);
    }

    // TODO we should have param indicating how much data to load at once
    pub fn load_batches(&mut self) -> HashMap<Vec<u8>, Vec<RecordBatch>> {
        let mut result = HashMap::new();
        // let keys_to_remove: Vec<Vec<u8>> = self.cache.iter()
        //     .map(|entry| entry.key().clone())
        //     .collect();
        
        // for key in keys_to_remove {
        //     if let Some(entry) = self.cache.remove(&key) {
        //         result.insert(key, entry);
        //     }
        // }
        
        result
    }

}