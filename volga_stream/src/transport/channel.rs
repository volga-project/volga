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
        target_port: i32,
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

pub fn gen_channel_id(source_vertex_id: &str, target_vertex_id: &str) -> String {
    format!("{}_to_{}", source_vertex_id, target_vertex_id)
}