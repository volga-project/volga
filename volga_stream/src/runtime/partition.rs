use anyhow::{Error, Result};
use crate::common::message::Message;
use std::fmt;

pub trait PartitionTrait: Send + Sync + fmt::Debug {
    fn partition(&mut self, batch: &Message, num_partitions: usize) -> Result<Vec<usize>>;
}

#[derive(Debug, Clone)]
pub enum PartitionType {
    Broadcast,
    Hash,
    RoundRobin,
    Forward,
}

#[derive(Debug, Clone)]
pub enum Partition {
    Broadcast(BroadcastPartition),
    Hash(HashPartition),
    RoundRobin(RoundRobinPartition),
    Forward(ForwardPartition),
}

impl PartitionTrait for Partition {
    fn partition(&mut self, message: &Message, num_partitions: usize) -> Result<Vec<usize>> {
        match self {
            Partition::Broadcast(p) => p.partition(message, num_partitions),
            Partition::Hash(p) => p.partition(message, num_partitions),
            Partition::RoundRobin(p) => p.partition(message, num_partitions),
            Partition::Forward(p) => p.partition(message, num_partitions),
        }
    }
}

impl PartitionType {
    pub fn create(&self) -> Partition {
        match self {
            PartitionType::Broadcast => Partition::Broadcast(BroadcastPartition::new()),
            PartitionType::Hash => Partition::Hash(HashPartition::new()),
            PartitionType::RoundRobin => Partition::RoundRobin(RoundRobinPartition::new()),
            PartitionType::Forward => Partition::Forward(ForwardPartition::new()),
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

impl PartitionTrait for BroadcastPartition {
    fn partition(&mut self, _message: &Message, num_partitions: usize) -> Result<Vec<usize>> {
        Ok((0..num_partitions).collect())
    }
}

#[derive(Debug, Clone)]
pub struct HashPartition;

impl HashPartition {
    pub fn new() -> Self {
        Self
    }
}

impl PartitionTrait for HashPartition {
    fn partition(&mut self, message: &Message, num_partitions: usize) -> Result<Vec<usize>> {
        let key = message.key()?;
        let hash = key.hash();
        Ok(vec![(hash % num_partitions as u64) as usize])
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

impl PartitionTrait for RoundRobinPartition {
    fn partition(&mut self, _message: &Message, num_partitions: usize) -> Result<Vec<usize>> {
        let partition = self.counter % num_partitions;
        self.counter += 1;
        Ok(vec![partition])
    }
}

#[derive(Debug, Clone)]
pub struct ForwardPartition;

impl ForwardPartition {
    pub fn new() -> Self {
        Self
    }
}

impl PartitionTrait for ForwardPartition {
    fn partition(&mut self, _message: &Message, num_partitions: usize) -> Result<Vec<usize>> {
        if num_partitions != 1 {
            return Err(Error::msg("Forward partition requires exactly one partition"));
        }
        Ok(vec![0])
    }
}