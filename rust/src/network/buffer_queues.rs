use std::{collections::{HashMap, HashSet, VecDeque}, sync::{atomic::{AtomicU32, AtomicU8, Ordering}, Arc, Mutex, RwLock}};

use super::{buffer_utils::{get_buffer_id, new_buffer_with_meta}, channel::{Channel}, io_loop::Bytes};


// pub const MAX_BUFFERS_PER_CHANNEL: usize = 10;

pub struct BufferQueue {
    v: VecDeque<Box<Bytes>>,
    index: u32,
    buffer_id_seq: u32,
    pop_requests: HashSet<u32>,
    max_buffers_per_channel: usize
}

impl BufferQueue {

    pub fn new(max_buffers_per_channel: usize) -> Self {
        BufferQueue{v: VecDeque::with_capacity(max_buffers_per_channel), index: 0, buffer_id_seq: 0, pop_requests: HashSet::new(), max_buffers_per_channel: max_buffers_per_channel}
    }

    pub fn try_push(&mut self, channel_id: String, b: Box<Bytes>) -> bool {
        if self.v.len() == self.max_buffers_per_channel {
            return false;
        }
        let buffer_id = self.buffer_id_seq;
        let new_b = new_buffer_with_meta(b, channel_id.clone(), buffer_id);
        self.v.push_back(new_b);
        self.buffer_id_seq = buffer_id + 1;
        return true
    }

    // returns value from queue at schedule index without popping
    pub fn schedule_next(&mut self) -> Option<Box<Bytes>> {
        let len = self.v.len();
        if len == 0 {
            return None;
        }

        let index = self.index;
        if index >= (len as u32) {
            return None;
        }
        let res = self.v.get(index as usize).unwrap();
        self.index += 1;
        Some(res.clone())
    }

    // submits pop request, performs pop only for in-order requests
    pub fn request_pop(&mut self, buffer_id: u32) {
        self.pop_requests.insert(buffer_id);
        while self.v.len() != 0 {
            let peek_buffer = self.v.get(0).unwrap();
            let peek_buffer_id = get_buffer_id(peek_buffer.clone());
            if self.pop_requests.contains(&peek_buffer_id) {
                self.v.pop_front();
                self.pop_requests.remove(&peek_buffer_id);
                self.index -= 1;
            } else {
                break;
            }
        }
    }
}

pub struct BufferQueues {
    in_queues: Arc<RwLock<HashMap<String, Arc<Mutex<BufferQueue>>>>>,
}

impl BufferQueues {
    pub fn new(channels: Vec<Channel>, max_buffers_per_channel: usize) -> BufferQueues {
        let n_channels = channels.len();
        let mut in_queues = HashMap::with_capacity(n_channels);
        for ch in channels {
            in_queues.insert(ch.get_channel_id().clone(), Arc::new(Mutex::new(BufferQueue::new(max_buffers_per_channel))));
        }

        BufferQueues{in_queues: Arc::new(RwLock::new(in_queues))}
    }

    pub fn try_push(&self, channel_id: &String, b: Box<Bytes>) -> bool {
        let locked_queues = self.in_queues.read().unwrap();
        let mut locked_queue = locked_queues.get(channel_id).unwrap().lock().unwrap();
        locked_queue.try_push(channel_id.clone(), b)
    }

    pub fn schedule_next(&self, channel_id: &String) -> Option<Box<Bytes>> {
        let locked_queues = self.in_queues.read().unwrap();
        let mut locked_queue = locked_queues.get(channel_id).unwrap().lock().unwrap();
        locked_queue.schedule_next()
    }
    pub fn request_pop(&self, channel_id: &String, buffer_id: u32) {
        let locked_queues = self.in_queues.read().unwrap();
        let mut locked_queue = locked_queues.get(channel_id).unwrap().lock().unwrap();
        locked_queue.request_pop(buffer_id)
    }
}