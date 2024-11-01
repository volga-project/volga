use std::{collections::{HashMap, HashSet, VecDeque}, sync::{atomic::{AtomicBool, Ordering}, Arc, Condvar, Mutex}, thread::{self, JoinHandle}, time::{Duration, SystemTime}};

use crossbeam::{channel::{bounded, Receiver, Sender}, queue::ArrayQueue};

use super::{buffer_utils::{get_buffer_id, new_buffer_with_meta, Bytes}, channel::{Channel, DataReaderResponseMessage}, io_loop::CROSSBEAM_DEFAULT_CHANNEL_SIZE};

// TODO test in-flight resend
// TODO class description

struct BufferQueueInner {
    v: VecDeque<Bytes>,
    index: usize,
    buffer_id_seq: u32,
    pop_requests: HashSet<u32>,
    max_capacity_bytes: usize,
    cur_occupied_bytes: usize,
    in_flights: HashMap<u32, (u128, Bytes)>,
    in_flight_timeout_s: usize
}

impl BufferQueueInner {

    pub fn can_push(&self, b: &Bytes) -> bool {
        self.cur_occupied_bytes + b.len() <= self.max_capacity_bytes
    }

    pub fn new(max_capacity_bytes: usize, in_flight_timeout_s: usize) -> Self {
        BufferQueueInner{
            v: VecDeque::new(), 
            index: 0, 
            buffer_id_seq: 0, 
            pop_requests: HashSet::new(),
            max_capacity_bytes,
            cur_occupied_bytes: 0,
            in_flights: HashMap::new(),
            in_flight_timeout_s
        }
    }

    pub fn push(&mut self, channel_id: String, b: Bytes) {
        if !self.can_push(&b) {
            panic!("Pushing when max capacity reached");
        }
        let buffer_id = self.buffer_id_seq;
        let new_b = new_buffer_with_meta(b, channel_id.clone(), buffer_id);
        let size = new_b.len();
        self.v.push_back(new_b);
        self.buffer_id_seq = buffer_id + 1;
        self.cur_occupied_bytes += size;
    }

    // returns value from queue at schedule index without popping
    pub fn schedule_next(&mut self) -> Option<Bytes> {
        let len = self.v.len();
        if len == 0 {
            return None;
        }

        let index = self.index;
        if index >= len {
            return None;
        }
        let res = self.v.get(index).unwrap();
        self.index += 1;
        Some(res.to_vec())
    }

    pub fn has_next_schedulable(&self) -> bool {
        self.v.len() != 0 && self.index < self.v.len()
    }

    // submits pop request, performs pop only for in-order requests
    pub fn request_pop(&mut self, buffer_id: u32) -> Vec<u32> {
        self.pop_requests.insert(buffer_id);
        let mut popped = vec![];
        while self.v.len() != 0 {
            let peek_buffer = self.v.get(0).unwrap();
            let peek_buffer_id = get_buffer_id(peek_buffer);
            if self.pop_requests.contains(&peek_buffer_id) {
                let b = self.v.pop_front().unwrap();
                self.cur_occupied_bytes -= b.len();
                self.pop_requests.remove(&peek_buffer_id);
                self.index -= 1;
                popped.push(peek_buffer_id);
            } else {
                break;
            }
        }
        popped
    }

    pub fn get_resendable_in_flight(&self) -> Option<Bytes> {
        for (_, ts_and_b) in &self.in_flights {
            let now_ts = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis();
            if now_ts - ts_and_b.0 > self.in_flight_timeout_s as u128 {
                return Some(ts_and_b.1.to_vec());
            }
        }
        None
    }

    pub fn has_resendable_in_flight(&self) -> bool {
        let r = self.get_resendable_in_flight();
        !r.is_none()
    }

    pub fn has_reached_max_in_flights(&self) -> bool {
        let mut in_flight_size_bytes = 0;
        for (_, ts_and_b) in &self.in_flights {
            in_flight_size_bytes += ts_and_b.1.len();
        }
        in_flight_size_bytes >= self.max_capacity_bytes
    }

    pub fn add_in_flight(&mut self, b: Bytes) {
        let buffer_id = get_buffer_id(&b);
        let now_ts = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis();
        self.in_flights.insert(buffer_id, (now_ts, b));
    }

    pub fn remove_in_flight(&mut self, buffer_id: u32) {
        self.in_flights.remove(&buffer_id);
    }
}

pub struct BufferQueuesInner {
    queues: HashMap<String, BufferQueueInner>
}

impl BufferQueuesInner {
    pub fn new(channels: &Vec<Channel>, max_capapcity_bytes_per_channel: usize, in_flight_timeout_s: usize) -> BufferQueuesInner {
        let n_channels = channels.len();
        let mut queues = HashMap::with_capacity(n_channels);
        for ch in channels {
            queues.insert(ch.get_channel_id().clone(), BufferQueueInner::new(max_capapcity_bytes_per_channel, in_flight_timeout_s));
        }

        BufferQueuesInner{queues}
    }

    pub fn has_at_least_one_resendable_in_flight(&self) -> bool {
        for (_, queue) in &self.queues {
            if queue.has_resendable_in_flight() {
                return true
            }
        }
        false
    }

    pub fn has_at_least_one_schedulable(&self) -> bool {
        for (_, queue) in &self.queues {
            if queue.has_next_schedulable() {
                return true
            }
        }
        false
    }

    pub fn schedule(&mut self) -> HashMap<&String, Bytes> {
        let mut res = HashMap::new();
        for (channel_id, queue) in self.queues.iter_mut() {
            // check in-flights first
            let b = queue.get_resendable_in_flight();
            if b.is_some() {
                let b = b.unwrap();
                res.insert(channel_id, b);
            } else {
                // if no in-flights, schedule only if in-flight limit is not reached
                if !queue.has_reached_max_in_flights() {
                    let b = queue.schedule_next();
                    if b.is_some() {
                        let b = b.unwrap();
                        res.insert(channel_id, b);
                    }
                }
            }
        }
        res
    }

    // pub fn is_full(&self, channel_id: &String) -> bool {
    //     let buffer_queue = self.queues.get(channel_id).unwrap();
    //     buffer_queue.is_full()
    // }
    pub fn can_push(&self, channel_id: &String, b: &Bytes) -> bool {
        let buffer_queue = self.queues.get(channel_id).unwrap();
        buffer_queue.can_push(b)
    }

    pub fn push(&mut self, channel_id: &String, b: Bytes) {
        let buffer_queue = self.queues.get_mut(channel_id).unwrap();
        buffer_queue.push(channel_id.clone(), b)
    }

    pub fn request_pop(&mut self, channel_id: &String, buffer_id: u32) -> Vec<u32> {
        let buffer_queue = self.queues.get_mut(channel_id).unwrap();
        buffer_queue.request_pop(buffer_id)
    }

    pub fn add_in_flight(&mut self, channel_id: &String, b: Bytes) {
        let buffer_queue = self.queues.get_mut(channel_id).unwrap();
        buffer_queue.add_in_flight(b);
    }

    pub fn remove_in_flight(&mut self, channel_id: &String, buffer_id: u32) {
        let buffer_queue = self.queues.get_mut(channel_id).unwrap();
        buffer_queue.remove_in_flight(buffer_id)
    }
}

pub struct BufferQueues {
    queues: Arc<Mutex<BufferQueuesInner>>,
    condvar: Arc<Condvar>,
    out_chan: Arc<(Sender<(String, Bytes)>, Receiver<(String, Bytes)>)>,
    running: Arc<AtomicBool>,
    thread_handles: Arc<ArrayQueue<JoinHandle<()>>>, // array queue so we do not mutate DataReader and keep ownership
}

impl BufferQueues {
    pub fn new(channels: &Vec<Channel>, max_capacity_bytes_per_channel: usize, in_flight_timeout_s: usize) -> BufferQueues {
        let bqs = BufferQueuesInner::new(channels, max_capacity_bytes_per_channel, in_flight_timeout_s);
        
        BufferQueues{
            queues: Arc::new(Mutex::new(bqs)), 
            condvar: Arc::new(Condvar::new()), 
            out_chan: Arc::new(bounded(CROSSBEAM_DEFAULT_CHANNEL_SIZE)),
            running: Arc::new(AtomicBool::new(false)),
            thread_handles: Arc::new(ArrayQueue::new(2))
        }
    }

    // TODO test this
    // TODO can we use crossbeam tick here instead of a separate thread?
    fn start_timer(&self) {
        let this_queues = self.queues.clone();
        let this_running = self.running.clone();
        let this_condvar = self.condvar.clone();
        let timer_loop = move || {

            while this_running.load(Ordering::Relaxed) {
                let locked_queues = this_queues.lock().unwrap();
                if locked_queues.has_at_least_one_resendable_in_flight() {
                    this_condvar.notify_one();
                }
                drop(locked_queues);
                thread::sleep(Duration::from_millis(100));
            }
        };
        self.thread_handles.push(thread::spawn(timer_loop)).unwrap();
    }

    fn start_scheduler(&self) {

        let this_queues = self.queues.clone();
        let this_running = self.running.clone();
        let this_out_chan = self.out_chan.clone();
        let this_condvar = self.condvar.clone();
        let scheduler_loop = move || {

            while this_running.load(Ordering::Relaxed) {
                let mut locked_queues = this_queues.lock().unwrap();
                let schedulable = locked_queues.schedule();
                for (channel_id, b) in schedulable {
                    let s = &this_out_chan.0;
                    // TODO update test to use 1 output channel
                    while this_running.load(Ordering::Relaxed) {
                        let res = s.send_timeout((channel_id.clone(), b.clone()), Duration::from_millis(100));
                        if res.is_ok() {
                            break;
                        }
                    }
                }

                while !locked_queues.has_at_least_one_schedulable() {
                    let res = this_condvar.wait_timeout(locked_queues, Duration::from_millis(100)).unwrap();
                    if res.1.timed_out() {
                        break;
                    }
                    locked_queues = res.0;
                }
            }
        };

        self.thread_handles.push(thread::spawn(scheduler_loop)).unwrap();

        println!("Scheduler started");
    }

    pub fn start(&self) {
        self.running.store(true, Ordering::Relaxed);
        self.start_timer();
        self.start_scheduler();
    }

    pub fn close(&self) {
        self.running.store(false, Ordering::Relaxed);
        while self.thread_handles.len() != 0 {
            let handle = self.thread_handles.pop();
            handle.unwrap().join().unwrap();
        }
    }

    pub fn try_push(&self, channel_id: &String, b: Bytes) -> bool {
        let timeout_ms = 100;
        let mut locked_bq = self.queues.lock().unwrap();
        while !locked_bq.can_push(channel_id, &b) {
            let res = self.condvar.wait_timeout(locked_bq, Duration::from_millis(timeout_ms)).unwrap();
            if res.1.timed_out() {
                return false;
            }
            locked_bq = res.0;
        }
        locked_bq.push(channel_id, b);
        self.condvar.notify_one();
        true
    }

    pub fn handle_ack(&self, ack: &DataReaderResponseMessage) -> Vec<u32> {
        let mut locked_bq = self.queues.lock().unwrap();
        let channel_id = &ack.channel_id;
        let buffer_ids_range = &ack.buffer_ids_range;
        let mut popped = vec![];
        for buffer_id in buffer_ids_range.0..(buffer_ids_range.1 + 1) {
            let mut _popped = locked_bq.request_pop(channel_id, buffer_id);
            popped.append(&mut _popped);
            locked_bq.remove_in_flight(channel_id, buffer_id);
        }
        self.condvar.notify_one();
        popped
    }

    pub fn get_out_chan(&self) -> &(Sender<(String, Bytes)>, Receiver<(String, Bytes)>) {
        &self.out_chan
    }
}

#[cfg(test)]
mod tests {

    use std::{thread, time};

    use crate::newtork::buffer_utils::dummy_bytes;

    use super::*;

    #[test]
    fn test_buffer_queues_v2() {
        let num_channels = 1;
        let mut channels = vec![];

        for i in 0..num_channels {
            let channel = Channel::Local { 
                channel_id: format!("ch_{i}"), 
                ipc_addr: format!(""),
            };
            channels.push(channel);
        }
        let qs = Arc::new(BufferQueues::new(&channels, 10, 1));

        let dummy_buffer_ids: Vec<u32> = (0..100000).collect();
        let dummy_buffer_ids = Arc::new(dummy_buffer_ids);

        let mut pushers = vec![];

        for channel in channels.clone() {
            let qs_p = qs.clone();
            let bids_p = dummy_buffer_ids.clone();
            let _channel_id = channel.get_channel_id().clone();
            let pusher = thread::spawn(move || {
                for buffer_id in bids_p.iter() {
                    thread::sleep(time::Duration::from_micros(50));
                    println!("[{_channel_id}] Pushing {buffer_id}...");
                    qs_p.try_push(&_channel_id, dummy_bytes(*buffer_id, &_channel_id, 1));
                    println!("[{_channel_id}] Pushed {buffer_id}")
                }
            });
            pushers.push(pusher);
        }

        let qs_c = qs.clone();
        let bids_c = dummy_buffer_ids.clone();
        let consumer = thread::spawn(move || {
            let mut i = 0;
            let bq_out_chan = qs_c.get_out_chan();
            while i < bids_c.len() {
                let (channel_id, b) = bq_out_chan.1.recv().unwrap();
                let buffer_id = get_buffer_id(&b);
                let ack = DataReaderResponseMessage::new_ack(&channel_id, buffer_id);
                let popped = qs_c.handle_ack(&ack);
                // thread::sleep(time::Duration::from_millis(50));
                thread::sleep(time::Duration::from_micros(100));
                let s = format!("[{channel_id}] Popped {:?}", popped);
                println!("{s}");
                if popped.len() == 0 {
                    continue;
                } else {
                    i += 1;
                }
            }
        });
        qs.start();

        while !pushers.is_empty() {
            let p = pushers.pop().unwrap();
            p.join().unwrap();
        }
        
        consumer.join().unwrap();
        qs.close();
    }
}