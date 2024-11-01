use core::str;

extern crate varint;
use rand::Rng;
use varint::{ VarintRead, VarintWrite };

use std::{collections::{HashMap, VecDeque}, io::Cursor, sync::{atomic::{AtomicBool, Ordering}, Arc, Condvar, Mutex}, time::Duration};

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

// TODO we should address u32 overflow
pub fn get_buffer_id(b: &Bytes) -> u32 {
    let mut c = Cursor::new(b.to_vec());
    c.set_position(CHANNEL_ID_META_BYTES_LENGTH as u64);
    let buff_id = VarintRead::read_unsigned_varint_32(&mut c).expect("ok");
    buff_id
}

pub fn dummy_bytes(buffer_id: u32, channel_id: &String, payload_size: usize) -> Bytes {
    let mut rng = rand::thread_rng();
    let bs: Vec<u8> = (0..payload_size).map(|_| {
        rng.gen_range(0, 255)
    }).collect();
    new_buffer_with_meta(bs, channel_id.clone(), buffer_id)
}

struct MemoryBoundQueueInner {
    queue: VecDeque<Bytes>,
    max_capacity_bytes: usize,
    cur_occupied_bytes: usize
}

impl MemoryBoundQueueInner {
    pub fn new(max_capacity_bytes: usize) -> MemoryBoundQueueInner {
        MemoryBoundQueueInner{
            queue: VecDeque::new(),
            max_capacity_bytes,
            cur_occupied_bytes: 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub fn can_push(&self, b: &Bytes) -> bool {
        self.cur_occupied_bytes + b.len() <= self.max_capacity_bytes
    }

    pub fn push(&mut self, b: Bytes) {
        if !self.can_push(&b) {
            panic!("Can not push");
        }

        let size = b.len();
        self.queue.push_back(b);
        self.cur_occupied_bytes += size;
    }

    pub fn pop(&mut self) -> Bytes {
        if self.is_empty() {
            panic!("Can not pop");
        }
        let b = self.queue.pop_front().unwrap();
        self.cur_occupied_bytes -= b.len();
        b
    }
}

pub struct MemoryBoundQueue {
    queue: Mutex<MemoryBoundQueueInner>,
    condvar: Condvar,
    running: Arc<AtomicBool>
}

impl MemoryBoundQueue {
    pub fn new(max_capacity_bytes: usize, running: Arc<AtomicBool>) -> MemoryBoundQueue {
        MemoryBoundQueue{
            queue: Mutex::new(MemoryBoundQueueInner::new(max_capacity_bytes)),
            condvar: Condvar::new(),
            running
        }
    }

    pub fn can_push(&self, b: &Bytes) -> bool {
        self.queue.lock().unwrap().can_push(b)
    }

    pub fn push(&self, b: Bytes) {
        let mut locked_queue = self.queue.lock().unwrap();
        let condvar = &self.condvar;
        let running = &self.running;
        while running.load(Ordering::Relaxed) && !locked_queue.can_push(&b) {
            let res = condvar.wait_timeout(locked_queue, Duration::from_millis(100)).unwrap();
            locked_queue = res.0;
        }
        locked_queue.push(b);
        condvar.notify_one();
    }

    pub fn try_push(&self, b: Bytes) -> bool {
        if !self.can_push(&b) {
            return false;
        }

        self.push(b);
        true
    }

    pub fn pop(&self) -> Option<Bytes> {
        let mut locked_queue = self.queue.lock().unwrap();
        let condvar = &self.condvar;
        let running = &self.running;
        while running.load(Ordering::Relaxed) && locked_queue.is_empty() {
            let res = condvar.wait_timeout(locked_queue, Duration::from_millis(100)).unwrap();
            if res.1.timed_out() {
                return None
            }
            locked_queue = res.0;
        }

        let res = locked_queue.pop();
        condvar.notify_one();
        Some(res)
    }
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

    #[test]
    fn test_memory_bound_queue() {
        let running = Arc::new(AtomicBool::new(true));
        let capacity_bytes = 100;
        let payload_size_bytes = 40;
        let q = MemoryBoundQueue::new(capacity_bytes, running);

        let mut b: Vec<u8> = Vec::new();
        for _ in 0..payload_size_bytes {
            b.push(0);
        }

        assert!(q.pop() == None);
        assert!(q.can_push(&b));
        q.push(b.clone());
        assert!(q.can_push(&b));
        q.push(b.clone());
        assert!(!q.can_push(&b));
        assert!(q.pop() != None);
        assert!(q.pop() != None);
        assert!(q.pop() == None);
        assert!(q.can_push(&b));
    }
}