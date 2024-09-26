use serde::{Deserialize, Serialize};

use super::{buffer_utils::CHANNEL_ID_META_BYTES_LENGTH, io_loop::Bytes};

#[derive(Clone)]
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

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum DataReaderResponseMessageKind {
    Ack,
    QueueConsumed
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct DataReaderResponseMessage {
    pub kind: DataReaderResponseMessageKind,
    pub channel_id: String,
    pub buffer_id: u32
}

impl DataReaderResponseMessage {

    pub fn ser(&self) -> Box<Bytes>{
    
        let mut b = bincode::serialize(&self).unwrap();
       
        // append channel_id header
        let channel_id_bytes = self.channel_id.as_bytes().to_vec();
        if channel_id_bytes.len() > CHANNEL_ID_META_BYTES_LENGTH {
            panic!("channel_id is too long")
        }

        let mut res = Vec::new(); 
        for _ in 0..(CHANNEL_ID_META_BYTES_LENGTH - channel_id_bytes.len()) {
            res.push(0x00 as u8);
        }

        for v in channel_id_bytes {
            res.push(v);   
        }

        res.append(&mut b);
        Box::new(res)
    }

    pub fn de(b: Box<Bytes>) -> Self {
        let mut _b = b.clone();
        _b.drain(0..CHANNEL_ID_META_BYTES_LENGTH);
        let ack: DataReaderResponseMessage = bincode::deserialize(&_b).unwrap();
        ack
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ack_serde() {
        let ack = DataReaderResponseMessage{kind: DataReaderResponseMessageKind::Ack, channel_id:String::from("ch_0"), buffer_id: 1234};
        let b = ack.ser();
        let _ack = DataReaderResponseMessage::de(b);

        assert_eq!(ack, _ack);
    }
}