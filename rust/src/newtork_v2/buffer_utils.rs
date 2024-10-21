use core::str;

extern crate varint;
use varint::{ VarintRead, VarintWrite };

use std::{collections::HashMap, io::Cursor};

use super::utils::random_string;

pub type Bytes = Vec<u8>;

pub const CHANNEL_ID_META_BYTES_LENGTH: usize = 16 * 4; // 16 chars

pub fn new_buffer_with_meta(b: Bytes, channel_id: String, buffer_id: u32) -> Bytes{
    // let channel_id_bytes = vec![0; CHANNEL_ID_META_BYTES_LENGTH];
    let channel_id_bytes = channel_id.as_bytes().to_vec();
    if channel_id_bytes.len() > CHANNEL_ID_META_BYTES_LENGTH {
        panic!("channel_id is too long")
    }

    // let mut res = b.to_vec();
    let mut res = Vec::new(); 
    for _ in 0..(CHANNEL_ID_META_BYTES_LENGTH - channel_id_bytes.len()) {
        res.push(0x00 as u8);
    }

    for v in channel_id_bytes {
        res.push(v);   
    }

    let buffer_id_bytes = Vec::new();
    let mut c = Cursor::new(buffer_id_bytes);
    VarintWrite::write_unsigned_varint_32(&mut c, buffer_id).expect("ok");

    for v in c.get_ref() {
        res.push(*v);   
    }

    res.append(&mut b.to_vec());

    res
}

pub fn new_buffer_drop_meta(b: Bytes) -> Bytes {
    let local_b = b.clone();
    let mut c = Cursor::new(b);
    c.set_position(CHANNEL_ID_META_BYTES_LENGTH as u64);
    VarintRead::read_unsigned_varint_32(&mut c).expect("ok");
    let pos = c.position();
    let res = local_b[pos as usize..].to_vec();
    res
}

pub fn get_channeld_id(b: &Bytes) -> String {
    let ch_id_bytes = &b[0..CHANNEL_ID_META_BYTES_LENGTH];

    str::from_utf8(ch_id_bytes).unwrap().trim_matches(char::from(0)).to_string()
}

pub fn get_buffer_id(b: &Bytes) -> u32 {
    let mut c = Cursor::new(b.to_vec());
    c.set_position(CHANNEL_ID_META_BYTES_LENGTH as u64);
    let buff_id = VarintRead::read_unsigned_varint_32(&mut c).expect("ok");
    buff_id
}

pub fn dummy_bytes(buffer_id: u32, channel_id: &String, payload_size: usize) -> Bytes {
    let mut msg = HashMap::new();
    msg.insert("key", buffer_id.to_string());
    msg.insert("value", random_string(payload_size));
    let bs = bincode::serialize(&msg).unwrap().to_vec();
    new_buffer_with_meta(bs, channel_id.clone(), buffer_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_utils_v2() {

        let s = String::from("a");
        let b = bincode::serialize(&s).unwrap();
        let ch_id = String::from("ch_0");
        let buffer_id = 12345;
        let _b = new_buffer_with_meta(b, ch_id.clone(), buffer_id);

        let _ch_id = get_channeld_id(&_b);
        let _buffer_id = get_buffer_id(&_b);

        let b_ = new_buffer_drop_meta(_b);
        let s_: String = bincode::deserialize(&b_).unwrap();
        assert_eq!(ch_id, _ch_id);
        assert_eq!(buffer_id, _buffer_id);
        assert_eq!(s_, s);
    }
}