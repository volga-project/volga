use std::cmp::PartialEq;
use std::hash::{Hash, Hasher};

impl PartialEq for Channel {
    fn eq(&self, other: &Self) -> bool {
        self.get_channel_id() == other.get_channel_id()
    }
}

impl Eq for Channel {}

impl Hash for Channel {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.get_channel_id().hash(state);
    }
}

#[derive(Clone, Debug)]
pub enum Channel {
    Local {
        channel_id: String,
        source_vertex_id: String,
        target_vertex_id: String,
        queue_size_records: u32,
    },
    Remote {
        channel_id: String,
        source_vertex_id: String,
        target_vertex_id: String,
        source_node_ip: String,
        source_node_id: String,
        target_node_ip: String,
        target_node_id: String,
        target_port: i32,
        queue_size_records: u32,
    }
}

impl Channel {
    pub fn new_local(source_vertex_id: String, target_vertex_id: String) -> Self {
        Self::new_local_with_queue(source_vertex_id, target_vertex_id, crate::transport::transport_spec::TransportSpec::DEFAULT_QUEUE_RECORDS)
    }

    pub fn new_local_with_queue(source_vertex_id: String, target_vertex_id: String, queue_size_records: u32) -> Self {
        let channel_id = gen_channel_id(source_vertex_id.clone(), target_vertex_id.clone());
        Channel::Local { channel_id, source_vertex_id, target_vertex_id, queue_size_records }
    }

    pub fn new_remote(
        source_vertex_id: String,
        target_vertex_id: String,
        source_node_ip: String,
        source_node_id: String,
        target_node_ip: String,
        target_node_id: String,
        target_port: i32,
    ) -> Self {
        Self::new_remote_with_queue(
            source_vertex_id,
            target_vertex_id,
            source_node_ip,
            source_node_id,
            target_node_ip,
            target_node_id,
            target_port,
            crate::transport::transport_spec::TransportSpec::DEFAULT_QUEUE_RECORDS,
        )
    }

    pub fn new_remote_with_queue(
        source_vertex_id: String,
        target_vertex_id: String,
        source_node_ip: String,
        source_node_id: String,
        target_node_ip: String,
        target_node_id: String,
        target_port: i32,
        queue_size_records: u32,
    ) -> Self {
        let channel_id = gen_channel_id(source_vertex_id.clone(), target_vertex_id.clone());
        Channel::Remote {
            channel_id,
            source_vertex_id,
            target_vertex_id,
            source_node_ip,
            source_node_id,
            target_node_ip,
            target_node_id,
            target_port,
            queue_size_records,
        }
    }
}

impl Channel {
    pub fn get_channel_id(&self) -> String {
        match &self {
            Channel::Local { channel_id, ..} => {
                channel_id.clone()
            },
            Channel::Remote { channel_id, ..} => {
                channel_id.clone()  
            }
        }
    }

    pub fn get_source_vertex_id(&self) -> String {
        match &self {
            Channel::Local { source_vertex_id, ..} => {
                source_vertex_id.clone()
            },
            Channel::Remote { source_vertex_id, ..} => {
                source_vertex_id.clone()
            }
        }
    }

    pub fn get_target_vertex_id(&self) -> String {
        match &self {
            Channel::Local { target_vertex_id, ..} => {
                target_vertex_id.clone()
            },
            Channel::Remote { target_vertex_id, ..} => {
                target_vertex_id.clone()
            }
        }
    }

    pub fn get_queue_size_records(&self) -> u32 {
        match &self {
            Channel::Local { queue_size_records, .. } => *queue_size_records,
            Channel::Remote { queue_size_records, .. } => *queue_size_records,
        }
    }
}

pub fn to_local_and_remote(channels: &Vec<Channel>) -> (Vec<Channel>, Vec<Channel>) {
    let mut local = Vec::new();
    let mut remote = Vec::new();

    for channel in channels {
        match channel {
            Channel::Local{..} => {
                local.push(channel.clone());
            }
            Channel::Remote {..} => {
                remote.push(channel.clone())
            }
        }
    }

    (local, remote)
} 

fn gen_channel_id(source_vertex_id: String, target_vertex_id: String) -> String {
    format!("{}_to_{}", source_vertex_id, target_vertex_id)
}