use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct ChannelMessage {
    key: &str,
    value: &str
}