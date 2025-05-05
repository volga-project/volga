use anyhow::{Error, Result};
use std::sync::Mutex;
use lru::LruCache;
use std::hash::{Hash, Hasher};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::num::NonZeroUsize;
use crate::common::data_batch::DataBatch;
use std::fmt;

#[derive(Debug, Clone)]
pub enum PartitionType {
    Broadcast,
    Key,
    RoundRobin,
    Forward,
}

pub fn create_partition(partition_type: PartitionType) -> Box<dyn Partition> {
    match partition_type {
        PartitionType::Broadcast => Box::new(BroadcastPartition::new()),
        PartitionType::Key => Box::new(KeyPartition::new()),
        PartitionType::RoundRobin => Box::new(RoundRobinPartition::new()),
        PartitionType::Forward => Box::new(ForwardPartition::new()),
    }
}

pub trait Partition: Send + Sync + fmt::Debug {
    fn partition(&self, batch: &DataBatch, num_partition: usize) -> Result<Vec<usize>>;
}

// BroadcastPartition
#[derive(Debug)]
pub struct BroadcastPartition {
    partitions: Mutex<Vec<usize>>,
}

impl BroadcastPartition {
    pub fn new() -> Self {
        Self {
            partitions: Mutex::new(Vec::new()),
        }
    }
}

impl Partition for BroadcastPartition {
    fn partition(&self, _batch: &DataBatch, num_partition: usize) -> Result<Vec<usize>> {
        let mut partitions = self.partitions.lock().unwrap();
        if partitions.len() != num_partition {
            *partitions = (0..num_partition).collect();
        }
        Ok(partitions.clone())
    }
}

// KeyPartition
#[derive(Debug)]
pub struct KeyPartition {
    partitions: Mutex<Vec<usize>>,
    hash_cache: Mutex<LruCache<String, usize>>,
}

impl KeyPartition {
    pub fn new() -> Self {
        Self {
            partitions: Mutex::new(vec![0]),
            hash_cache: Mutex::new(LruCache::new(NonZeroUsize::new(1024).unwrap())),
        }
    }

    fn hash(&self, key: &str) -> usize {
        let mut cache = self.hash_cache.lock().unwrap();
        if let Some(&hash) = cache.get(key) {
            return hash;
        }

        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish() as usize;
        
        cache.put(key.to_string(), hash);
        hash
    }
}

impl Partition for KeyPartition {
    fn partition(&self, batch: &DataBatch, num_partition: usize) -> Result<Vec<usize>> {
        let key = batch.key()?;
        let hash = self.hash(&key);
        let mut partitions = self.partitions.lock().unwrap();
        partitions[0] = hash % num_partition;
        Ok(partitions.clone())
    }
}

// RoundRobinPartition
#[derive(Debug)]
pub struct RoundRobinPartition {
    partitions: Mutex<Vec<usize>>,
    seq: AtomicUsize,
}

impl RoundRobinPartition {
    pub fn new() -> Self {
        Self {
            partitions: Mutex::new(vec![0]),
            seq: AtomicUsize::new(0),
        }
    }
}

impl Partition for RoundRobinPartition {
    fn partition(&self, _batch: &DataBatch, num_partition: usize) -> Result<Vec<usize>> {
        let seq = self.seq.fetch_add(1, Ordering::Relaxed) % num_partition;
        let mut partitions = self.partitions.lock().unwrap();
        partitions[0] = seq;
        Ok(partitions.clone())
    }
}

// ForwardPartition
#[derive(Debug)]
pub struct ForwardPartition {
    partitions: Vec<usize>,
}

impl ForwardPartition {
    pub fn new() -> Self {
        Self {
            partitions: vec![0],
        }
    }
}

impl Partition for ForwardPartition {
    fn partition(&self, _batch: &DataBatch, _num_partition: usize) -> Result<Vec<usize>> {
        Ok(self.partitions.clone())
    }
}