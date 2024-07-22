use std::any::Any;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct ChannelMessage {
    key: String,
    value: String
}

pub enum Channel {
    Local {
        channel_id: String,
        ipc_addr: String
    },
    Remote {
        channel_id: String,
        source_local_ipc_addr: String,
        source_node_ip: String,
        source_node_id: String,
        target_local_ipc_addr: String,
        target_node_ip: String,
        target_node_id: String,
        port: i32,
    }
}
