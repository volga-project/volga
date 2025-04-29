use serde::{Deserialize, Serialize};

#[derive(Clone, Debug)]
pub enum Channel {
    Local {
        channel_id: String,
    },
    Remote {
        channel_id: String,
        source_node_ip: String,
        source_node_id: String,
        target_node_ip: String,
        target_node_id: String,
        port: i32,
    }
}

impl Channel {
    pub fn get_channel_id(&self) -> &String {
        match &self {
            Channel::Local { channel_id, ..} => {
                channel_id
            },
            Channel::Remote { channel_id, ..} => {
                channel_id
            }
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