use anyhow::{Error, Result};
use std::sync::atomic::{AtomicUsize, Ordering};
use crate::common::data_batch::DataBatch;
use std::fmt;
use std::hash::{Hash, Hasher};

pub trait Partition: Send + Sync + fmt::Debug {
    fn partition(&mut self, batch: &DataBatch, num_partitions: usize) -> Result<Vec<usize>>;
}

#[derive(Debug, Clone)]
pub enum PartitionType {
    Broadcast,
    Key,
    RoundRobin,
    Forward,
}

impl PartitionType {
    pub fn create(&self) -> Box<dyn Partition> {
        match self {
            PartitionType::Broadcast => Box::new(BroadcastPartition::new()),
            PartitionType::Key => Box::new(KeyPartition::new()),
            PartitionType::RoundRobin => Box::new(RoundRobinPartition::new()),
            PartitionType::Forward => Box::new(ForwardPartition::new()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct BroadcastPartition;

impl BroadcastPartition {
    pub fn new() -> Self {
        Self
    }
}

impl Partition for BroadcastPartition {
    fn partition(&mut self, _batch: &DataBatch, num_partitions: usize) -> Result<Vec<usize>> {
        Ok((0..num_partitions).collect())
    }
}

#[derive(Debug, Clone)]
pub struct KeyPartition;

impl KeyPartition {
    pub fn new() -> Self {
        Self
    }
}

impl Partition for KeyPartition {
    fn partition(&mut self, batch: &DataBatch, num_partitions: usize) -> Result<Vec<usize>> {
        let key = batch.key()?;
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish() as usize;
        Ok(vec![hash % num_partitions])
    }
}

#[derive(Debug, Clone)]
pub struct RoundRobinPartition {
    counter: usize,
}

impl RoundRobinPartition {
    pub fn new() -> Self {
        Self { counter: 0 }
    }
}

impl Partition for RoundRobinPartition {
    fn partition(&mut self, _batch: &DataBatch, num_partitions: usize) -> Result<Vec<usize>> {
        let seq = self.counter % num_partitions;
        self.counter += 1;
        Ok(vec![seq])
    }
}

#[derive(Debug, Clone)]
pub struct ForwardPartition;

impl ForwardPartition {
    pub fn new() -> Self {
        Self
    }
}

impl Partition for ForwardPartition {
    fn partition(&mut self, _batch: &DataBatch, num_partitions: usize) -> Result<Vec<usize>> {
        if num_partitions != 1 {
            anyhow::bail!("Forward partition requires exactly one partition");
        }
        Ok(vec![0])
    }
}