use core::str;

extern crate varint;
use varint::{ VarintRead, VarintWrite };

use std::io::Cursor;

use super::io_loop::Bytes;

pub const CHANNEL_ID_META_BYTES_LENGTH: usize = 16 * 4; // 16 chars

pub fn new_buffer_with_meta(b: Box<Bytes>, channel_id: String, buffer_id: u32) -> Box<Bytes>{
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

    Box::new(res)
}

pub fn new_buffer_drop_meta(b: Box<Bytes>) -> Box<Bytes> {
    let local_b = b.clone();
    let mut c = Cursor::new(*b);
    c.set_position(CHANNEL_ID_META_BYTES_LENGTH as u64);
    VarintRead::read_unsigned_varint_32(&mut c).expect("ok");
    let pos = c.position();
    let res = local_b[pos as usize..].to_vec();
    Box::new(res)
}

pub fn get_channeld_id(b: Box<Bytes>) -> String {
    let ch_id_bytes = &b[0..CHANNEL_ID_META_BYTES_LENGTH];

    str::from_utf8(ch_id_bytes).unwrap().trim_matches(char::from(0)).to_string()
}

pub fn get_buffer_id(b: Box<Bytes>) -> u32 {
    let mut c = Cursor::new(*b);
    c.set_position(CHANNEL_ID_META_BYTES_LENGTH as u64);
    let buff_id = VarintRead::read_unsigned_varint_32(&mut c).expect("ok");
    buff_id
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_utils() {

        let s = String::from("a");
        let bytes = bincode::serialize(&s).unwrap();
        let b = Box::new(bytes);
        // let l = b.len();
        let ch_id = String::from("ch_0");
        let buffer_id = 12345;
        let _b = new_buffer_with_meta(b.clone(), ch_id.clone(), buffer_id);

        let _ch_id = get_channeld_id(_b.clone());
        let _buffer_id = get_buffer_id(_b.clone());

        let b_ = new_buffer_drop_meta(_b);
        let s_: String = bincode::deserialize(&b_).unwrap();
        assert_eq!(ch_id, _ch_id);
        assert_eq!(buffer_id, _buffer_id);
        assert_eq!(s_, s);
    }
}