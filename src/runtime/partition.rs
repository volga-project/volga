use crate::{common::message::Message, runtime::functions::source::request_source::SOURCE_TASK_INDEX_FIELD};
use std::fmt;

pub trait PartitionTrait: Send + Sync + fmt::Debug {
    fn partition(&mut self, batch: &Message, num_partitions: usize) -> Vec<usize>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionType {
    Broadcast,
    Hash,
    RoundRobin,
    Forward,
    RequestRoute,
}

impl fmt::Display for PartitionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PartitionType::Broadcast => write!(f, "Broadcast"),
            PartitionType::Hash => write!(f, "Hash"),
            PartitionType::RoundRobin => write!(f, "RoundRobin"),
            PartitionType::Forward => write!(f, "Forward"),
            PartitionType::RequestRoute => write!(f, "RequestRoute"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Partition {
    Broadcast(BroadcastPartition),
    Hash(HashPartition),
    RoundRobin(RoundRobinPartition),
    Forward(ForwardPartition),
    RequestRoute(RequestRoutePartition),
}

impl PartitionTrait for Partition {
    fn partition(&mut self, message: &Message, num_partitions: usize) -> Vec<usize> {
        match self {
            Partition::Broadcast(p) => p.partition(message, num_partitions),
            Partition::Hash(p) => p.partition(message, num_partitions),
            Partition::RoundRobin(p) => p.partition(message, num_partitions),
            Partition::Forward(p) => p.partition(message, num_partitions),
            Partition::RequestRoute(p) => p.partition(message, num_partitions),
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
            PartitionType::RequestRoute => Partition::RequestRoute(RequestRoutePartition::new()),
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
    fn partition(&mut self, _message: &Message, num_partitions: usize) -> Vec<usize> {
        (0..num_partitions).collect()
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
    fn partition(&mut self, message: &Message, num_partitions: usize) -> Vec<usize> {
        let key = message.key().unwrap();
        let hash = key.hash();
        let partition = (hash % num_partitions as u64) as usize;
        vec![partition]
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
    fn partition(&mut self, _message: &Message, num_partitions: usize) -> Vec<usize> {
        let partition = self.counter % num_partitions;
        self.counter += 1;
        vec![partition]
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
    fn partition(&mut self, _message: &Message, num_partitions: usize) -> Vec<usize> {
        if num_partitions != 1 {
            panic!("Forward partition requires exactly one partition");
        }
        vec![0]
    }
}

// passes request message from source task to sink task with the same task_index
#[derive(Debug, Clone)]
pub struct RequestRoutePartition;

impl RequestRoutePartition {
    pub fn new() -> Self {
        Self
    }
}

impl PartitionTrait for RequestRoutePartition {
    fn partition(&mut self, message: &Message, num_partitions: usize) -> Vec<usize> {
        let extras = message.get_extras().expect("RequestRoutePartition message should have extras");
        let task_index = extras.get(SOURCE_TASK_INDEX_FIELD)
            .expect("RequestRoutePartition message should have task_index extra")
            .parse::<i32>()
            .expect("task_index should be parsed as i32") as usize;

        if task_index >= num_partitions {
            panic!("task_index should be within 0..num_partitions")
        }

        return vec![task_index]
    }
}