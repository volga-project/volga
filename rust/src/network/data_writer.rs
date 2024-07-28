use std::{collections::{HashMap, VecDeque}, sync::{atomic::{AtomicBool, Ordering}, Arc, Mutex, RwLock}, thread::{self, JoinHandle}, time::{Duration, SystemTime}};

use super::{channel::Channel, io_loop::{IOHandler, IOHandlerType}};
use super::io_loop::Bytes;
use crossbeam::{channel::{bounded, Receiver, Sender}, queue::ArrayQueue};

const MAX_BUFFERS_PER_CHANNEL: usize = 1000000;

pub struct DataWriter {
    name: String,
    channels: Vec<Channel>,
    send_chans: Arc<RwLock<HashMap<String, (Sender<Box<Bytes>>, Receiver<Box<Bytes>>)>>>,
    recv_chans: Arc<RwLock<HashMap<String, (Sender<Box<Bytes>>, Receiver<Box<Bytes>>)>>>,
    in_queues: Arc<RwLock<HashMap<String, Arc<Mutex<VecDeque<Box<Bytes>>>>>>>,

    running: Arc<AtomicBool>,
    dispatcher_thread_handle: Arc<ArrayQueue<JoinHandle<()>>> // array queue so we do not mutate DataReader and kepp ownership
}

impl DataWriter {

    pub fn new(name: String, channels: Vec<Channel>) -> DataWriter {
        let n_channels = channels.len();
        let mut send_chans = HashMap::with_capacity(n_channels);
        let mut recv_chans = HashMap::with_capacity(n_channels);
        let mut in_queues = HashMap::with_capacity(n_channels);

        for ch in &channels {
            send_chans.insert(ch.get_channel_id().clone(), bounded(MAX_BUFFERS_PER_CHANNEL));
            recv_chans.insert(ch.get_channel_id().clone(), bounded(MAX_BUFFERS_PER_CHANNEL));
            in_queues.insert(ch.get_channel_id().clone(), Arc::new(Mutex::new(VecDeque::with_capacity(MAX_BUFFERS_PER_CHANNEL))));
        }

        DataWriter{
            name,
            channels,
            send_chans: Arc::new(RwLock::new(send_chans)),
            recv_chans: Arc::new(RwLock::new(recv_chans)),
            in_queues: Arc::new(RwLock::new(in_queues)),
            running: Arc::new(AtomicBool::new(false)),
            dispatcher_thread_handle: Arc::new(ArrayQueue::new(1))
        }
    }

    pub fn write_bytes(&self, channel_id: &String, b: Box<Bytes>, timeout_ms: i32, retry_step_micros: u64) -> Option<u128> {
        let t = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_micros();
        let mut num_retries = 0;
        loop {
            let _t = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_micros();
            if _t - t > timeout_ms as u128 * 1000 {
                return None
            }
            let locked_in_queues = self.in_queues.read().unwrap();
            let in_queue = locked_in_queues.get(channel_id).unwrap();
            let mut locked_in_queue = in_queue.lock().unwrap();
            if locked_in_queue.len() == MAX_BUFFERS_PER_CHANNEL {
                drop(locked_in_queue);
                num_retries += 1;
                thread::sleep(Duration::from_micros(retry_step_micros));
                continue;
            }
            locked_in_queue.push_back(b.clone());
            drop(locked_in_queue);
            break;
        }
        let backpressured_time = if num_retries == 0 {0} else {SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_micros() - t};
        Some(backpressured_time)
    }

    // Returns backpressure in microseconds
    // pub fn write_message(&self, channel_id: &String, message: Arc<ChannelMessage>, timeout_ms: i32, retry_step_micros: i32) -> Option<u128> {
    //     let t = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_micros();
    //     let mut num_retries = 0;
    //     loop {
    //         let _t = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_micros();
    //         if _t - t > timeout_ms as u128 * 1000 {
    //             return None
    //         }
    //         if !self.write_message_no_block(channel_id, message.clone()) {
    //             if retry_step_micros > 0 {
    //                 std::thread::sleep(Duration::from_micros(retry_step_micros as u64));
    //             }
    //             num_retries += 1;
    //             continue;
    //         } else {
    //             break;
    //         }
    //     }
    //     let backpressured_time = if num_retries == 0 {0} else {SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_micros() - t};
    //     Some(backpressured_time)
    // }
  
    // pub fn write_message_no_block(&self, channel_id: &String, b: Box<ChannelMessage>) -> bool {
    //     // check send_queue size for backpressure
    //     let send_b_queues = &self.send_buffer_queues.read().unwrap();
    //     let locked_send_b_queue = send_b_queues.get(channel_id).unwrap().lock().unwrap();
    //     if locked_send_b_queue.len() >= MAX_BUFFERS_PER_CHANNEL {
    //         return false
    //     }
    //     drop(locked_send_b_queue);

    //     let in_queues = &self.in_message_queues.read().unwrap();
    //     let mut locked_queue = in_queues.get(channel_id).unwrap().lock().unwrap();
    //     locked_queue.push_back(message);
    //     true
    // }

    pub fn start(&self) {
        // start dispatcher thread: takes data from in_queue, passes to channel to IOLoop's threads
        self.running.store(true, Ordering::Relaxed);
        

        let this_send_chans = self.send_chans.clone();
        let this_in_queues = self.in_queues.clone();
        let this_runnning = self.running.clone();
        let f = move || {
            loop {
                let running = this_runnning.load(Ordering::Relaxed);
                if !running {
                    break;
                }
                let locked_in_queues = this_in_queues.read().unwrap();
                let locked_send_chans = this_send_chans.read().unwrap();
                for channel_id in  locked_in_queues.keys() {
                    let mut locked_in_queue = locked_in_queues.get(channel_id).unwrap().lock().unwrap();
                    if locked_in_queue.is_empty() {
                        drop(locked_in_queue);
                        continue;
                    }
                    
                    let send_chan = locked_send_chans.get(channel_id).unwrap();
                    let sender = send_chan.0.clone();
                    if !sender.is_full() {
                        let b = locked_in_queue.pop_front().unwrap();
                        sender.send(b).unwrap();
                    }
                    drop(locked_in_queue);
                }
                drop(locked_in_queues);
                drop(locked_send_chans);
            }
        };
        let name = &self.name;
        let thread_name = format!("volga_{name}_dispatcher_thread");
        self.dispatcher_thread_handle.push(std::thread::Builder::new().name(thread_name).spawn(f).unwrap()).unwrap();
    }

    pub fn close (&self) {
        self.running.store(false, Ordering::Relaxed);
        let handle = self.dispatcher_thread_handle.pop();
        handle.unwrap().join().unwrap();
    }
}

impl IOHandler for DataWriter {

    fn get_handler_type(&self) -> IOHandlerType {
        IOHandlerType::DataWriter
    }

    fn on_send_ready(&self, channel_id: &String) -> bool {
        true
    }

    fn on_recv_ready(&self, channel_id: &String) -> bool {
        true
    }

    fn get_channels(&self) -> &Vec<Channel> {
        &self.channels
    }

    fn get_send_chan(&self, key: &String) -> (Sender<Box<Bytes>>, Receiver<Box<Bytes>>) {
        let hm = &self.send_chans.read().unwrap();
        let v = hm.get(key).unwrap();
        v.clone()
    }

    fn get_recv_chan(&self, key: &String) -> (Sender<Box<Bytes>>, Receiver<Box<Bytes>>) {
        let hm = &self.recv_chans.read().unwrap();
        let v = hm.get(key).unwrap();
        v.clone()
    }

}