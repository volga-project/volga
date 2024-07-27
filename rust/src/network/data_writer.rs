use std::{collections::HashMap, sync::{Arc, RwLock}, time::{Duration, SystemTime}};

use super::{channel::Channel, io_loop::{IOHandler, IOHandlerType}};
use super::io_loop::Bytes;
use crossbeam::channel::{bounded, Sender, Receiver};

const MAX_BUFFERS_PER_CHANNEL: usize = 1000000;

pub struct DataWriter {
    name: String,
    channels: Vec<Channel>,

    // TODO maps are read-only, do we need locks here?
    send_chans: Arc<RwLock<HashMap<String, (Sender<Box<Bytes>>, Receiver<Box<Bytes>>)>>>,
    recv_chans: Arc<RwLock<HashMap<String, (Sender<Box<Bytes>>, Receiver<Box<Bytes>>)>>>,
    // in_message_queues: Arc<RwLock<HashMap<String, Arc<Mutex<VecDeque<Arc<ChannelMessage>>>>>>>,
}

impl DataWriter {

    pub fn new(name: String, channels: Vec<Channel>) -> DataWriter {
        let n_channels = channels.len();
        let mut send_chans = HashMap::with_capacity(n_channels);
        let mut recv_chans = HashMap::with_capacity(n_channels);

        for ch in &channels {
            send_chans.insert(ch.get_channel_id().clone(), bounded(MAX_BUFFERS_PER_CHANNEL));
            recv_chans.insert(ch.get_channel_id().clone(), bounded(MAX_BUFFERS_PER_CHANNEL));
        }

        DataWriter{
            name,
            channels,
            send_chans: Arc::new(RwLock::new(send_chans)),
            recv_chans: Arc::new(RwLock::new(recv_chans)),
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
            let locked_chans = self.send_chans.read().unwrap();
            let send_chan = locked_chans.get(channel_id).unwrap();
            let res = send_chan.0.send_timeout(b.clone(), Duration::from_micros(retry_step_micros));
            if !res.is_ok() {
                num_retries += 1;
                continue;
            } else {
                break;
            }
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
  
    // pub fn write_message_no_block(&self, channel_id: &String, message: Arc<ChannelMessage>) -> bool {
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

    // pub fn start(&mut self) {
    //     // start dispatcher thread: takes message from in_message_queue, passes to serializators pool, puts result in out_buffer_queue
    //     self.running.store(true, Ordering::Relaxed);

    //     let this_in_message_queues = self.in_message_queues.clone();
    //     let this_send_buffer_queues = self.send_buffer_queues.clone();
    //     let this_runnning = self.running.clone();
    //     let this_serde_pool = self.serde_pool.clone();
    //     let f = move || {
    //         loop {
    //             let running = this_runnning.load(Ordering::Relaxed);
    //             if !running {
    //                 break;
    //             }
    //             let msg_queues = this_in_message_queues.read().unwrap();
    //             let b_send_queues = this_send_buffer_queues.read().unwrap();
    //             for channel_id in  msg_queues.keys() {
    //                 let mut locked_msg_queue = msg_queues.get(channel_id).unwrap().lock().unwrap();
    //                 let msg = locked_msg_queue.pop_front();
    //                 drop(locked_msg_queue); // release lock
    //                 if !msg.is_none() {
    //                     this_serde_pool.submit_ser(msg.unwrap(), b_send_queues.get(channel_id).unwrap().clone());
    //                 }
    //             }
    //         }
    //     };
    //     let name = &self.name;
    //     let thread_name = format!("volga_{name}_dispatcher_thread");
    //     self.dispatcher_thread = Arc::new(Some(std::thread::Builder::new().name(thread_name).spawn(f).unwrap()));
    // }

    // pub fn close (&self) {
    //     self.running.store(false, Ordering::Relaxed);
    //     // TODO join
    // }
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