use serde::{Deserialize, Serialize};

use super::{buffer_utils::CHANNEL_ID_META_BYTES_LENGTH, buffer_utils::Bytes, utils::consecutive_slices};

// TODO class description
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
    pub buffer_ids_range: (u32, u32)
}

impl DataReaderResponseMessage {

    pub fn new_ack(channel_id: &String, buffer_id: u32) -> DataReaderResponseMessage {
        DataReaderResponseMessage{
            kind: DataReaderResponseMessageKind::Ack, 
            channel_id: channel_id.clone(), 
            buffer_ids_range: (buffer_id, buffer_id)
        }
    }

    // takes a list of acks and creates batched acks of sequential buffer_ids
    pub fn batch_acks(acks: &Vec<DataReaderResponseMessage>) -> Vec<DataReaderResponseMessage> {
        let mut buffer_ids = vec![];
        let channel_id = &acks[0].channel_id;

        for ack in acks {
            for buffer_id in ack.buffer_ids_range.0..(ack.buffer_ids_range.1 + 1) {
                buffer_ids.push(buffer_id);
            }
        }
        buffer_ids.sort();
        let grouped = consecutive_slices(&buffer_ids);
        let mut res = vec![];
        for group in grouped {
            res.push(DataReaderResponseMessage{
                kind: DataReaderResponseMessageKind::Ack, 
                channel_id: channel_id.clone(), 
                buffer_ids_range: (group[0], *group.last().unwrap())
            });
        }

        res
    }

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ack_serde() {
        let ack = DataReaderResponseMessage::new_ack(&String::from("ch_0"), 1234);
        let b = ack.ser();
        let _ack = DataReaderResponseMessage::de(b);

        assert_eq!(ack, _ack);
    }

    #[test]
    fn test_ack_batching() {
        let channeld_id = &String::from("ch_0");
        let mut acks = vec![];

        // 0-0
        let ack1 = DataReaderResponseMessage::new_ack(channeld_id, 0);
        acks.push(ack1);

        // 2-7
        for i in 2..8 {
            acks.push(DataReaderResponseMessage::new_ack(channeld_id, i));
        }

        // 9-12
        acks.push(DataReaderResponseMessage{
            kind: DataReaderResponseMessageKind::Ack,
            channel_id: channeld_id.clone(),
            buffer_ids_range: (9, 12)
        });

        // 14-23
        for i in 14..20 {
            acks.push(DataReaderResponseMessage::new_ack(channeld_id, i));
        }

        acks.push(DataReaderResponseMessage{
            kind: DataReaderResponseMessageKind::Ack,
            channel_id: channeld_id.clone(),
            buffer_ids_range: (20, 22)
        });
        acks.push(DataReaderResponseMessage::new_ack(channeld_id, 23));


        let bacthed = DataReaderResponseMessage::batch_acks(&acks);
        assert_eq!(bacthed.len(), 4);

        assert_eq!(bacthed[0].buffer_ids_range.0, 0);
        assert_eq!(bacthed[0].buffer_ids_range.1, 0);

        assert_eq!(bacthed[1].buffer_ids_range.0, 2);
        assert_eq!(bacthed[1].buffer_ids_range.1, 7);

        assert_eq!(bacthed[2].buffer_ids_range.0, 9);
        assert_eq!(bacthed[2].buffer_ids_range.1, 12);

        assert_eq!(bacthed[3].buffer_ids_range.0, 14);
        assert_eq!(bacthed[3].buffer_ids_range.1, 23);

        println!("asserts ok")
    }
}