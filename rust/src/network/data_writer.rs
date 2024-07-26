use std::{collections::{HashMap, VecDeque}, sync::{atomic::{AtomicBool, Ordering}, Arc, Mutex, RwLock}, thread::JoinHandle, time::{Duration, SystemTime}};

use super::{channel::{Channel, ChannelMessage}, io_loop::{IOHandler, IOHandlerType}};
use super::io_loop::Bytes;

const MAX_BUFFERS_PER_CHANNEL: usize = 10000000;

pub struct DataWriter {
    name: String,
    channels: Vec<Channel>,
    send_buffer_queues: Arc<RwLock<HashMap<String, Arc<Mutex<VecDeque<Box<Bytes>>>>>>>,
    recv_buffer_queues: Arc<RwLock<HashMap<String, Arc<Mutex<VecDeque<Box<Bytes>>>>>>>,
    in_message_queues: Arc<RwLock<HashMap<String, Arc<Mutex<VecDeque<Arc<ChannelMessage>>>>>>>,

    running: Arc<AtomicBool>,
    dispatcher_thread: Arc<Option<JoinHandle<()>>>
}

impl DataWriter {

    pub fn new(name: String, channels: Vec<Channel>) -> DataWriter {
        let send_queues =  Arc::new(RwLock::new(HashMap::new()));
        let recv_queues =  Arc::new(RwLock::new(HashMap::new()));
        let in_message_queues =  Arc::new(RwLock::new(HashMap::new()));

        for ch in &channels {
            send_queues.write().unwrap().insert(ch.get_channel_id().clone(), Arc::new(Mutex::new(VecDeque::new())));
            recv_queues.write().unwrap().insert(ch.get_channel_id().clone(), Arc::new(Mutex::new(VecDeque::new())));
            in_message_queues.write().unwrap().insert(ch.get_channel_id().clone(), Arc::new(Mutex::new(VecDeque::new())));
        }

        DataWriter{
            name,
            channels,
            send_buffer_queues: send_queues,
            recv_buffer_queues: recv_queues,
            in_message_queues,
            running: Arc::new(AtomicBool::new(false)),
            dispatcher_thread: Arc::new(None)
        }
    }

    // Returns backpressure in microseconds
    pub fn write_message(&self, channel_id: &String, message: Arc<ChannelMessage>, timeout_ms: i32, retry_step_micros: i32) -> Option<u128> {
        let t = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_micros();
        let mut num_retries = 0;
        loop {
            let _t = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_micros();
            if _t - t > timeout_ms as u128 * 1000 {
                return None
            }
            if !self.write_message_no_block(channel_id, message.clone()) {
                if retry_step_micros > 0 {
                    std::thread::sleep(Duration::from_micros(retry_step_micros as u64));
                }
                num_retries += 1;
                continue;
            } else {
                break;
            }
        }
        let backpressured_time = if num_retries == 0 {0} else {SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_micros() - t};
        Some(backpressured_time)
    }
  
    pub fn write_message_no_block(&self, channel_id: &String, message: Arc<ChannelMessage>) -> bool {
        // check send_queue size for backpressure
        let send_b_queues = &self.send_buffer_queues.read().unwrap();
        let locked_send_b_queue = send_b_queues.get(channel_id).unwrap().lock().unwrap();
        if locked_send_b_queue.len() >= MAX_BUFFERS_PER_CHANNEL {
            return false
        }
        drop(locked_send_b_queue);

        let in_queues = &self.in_message_queues.read().unwrap();
        let mut locked_queue = in_queues.get(channel_id).unwrap().lock().unwrap();
        locked_queue.push_back(message);
        true
    }

    pub fn start(&mut self) {
        // start dispatcher thread: takes message from in_message_queue, passes to serializators pool, puts result in out_buffer_queue
        self.running.store(true, Ordering::Relaxed);

        let this_in_message_queues = self.in_message_queues.clone();
        let this_send_buffer_queues = self.send_buffer_queues.clone();
        let this_runnning = self.running.clone();
        let f = move || {
            loop {
                let running = this_runnning.load(Ordering::Relaxed);
                if !running {
                    break;
                }
                let msg_queues = this_in_message_queues.read().unwrap();
                let b_send_queues = this_send_buffer_queues.read().unwrap();
                for channel_id in  msg_queues.keys() {
                    let mut locked_msg_queue = msg_queues.get(channel_id).unwrap().lock().unwrap();
                    let msg = locked_msg_queue.pop_front();
                    drop(locked_msg_queue); // release lock
                    // https://stackoverflow.com/questions/36061560/can-i-take-a-byte-array-and-deserialize-it-into-a-struct 
                    if !msg.is_none() {
                        // TODO we should asynchronously dispatch to serde pool here, for simplicity we do sequential serde
                        let msg = msg.unwrap();
                        // let bytes = Box::new(serde_json::to_string(msg.as_ref()).unwrap().as_bytes().to_vec()); // move to heap
                        let bytes = Box::new(bincode::serialize(msg.as_ref()).unwrap());
                        
                        // put to buffer queue
                        let mut locked_b_send_queue = b_send_queues.get(channel_id).unwrap().lock().unwrap();
                        locked_b_send_queue.push_back(bytes);
                    }
                }
            }
        };
        let name = &self.name;
        let thread_name = format!("volga_{name}_dispatcher_thread");
        self.dispatcher_thread = Arc::new(Some(std::thread::Builder::new().name(thread_name).spawn(f).unwrap()));
    }

    pub fn close (&self) {
        self.running.store(false, Ordering::Relaxed);
        // TODO join
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

    fn get_send_buffer_queue(&self, key: &String) -> Arc<Mutex<VecDeque<Box<Bytes>>>> {
        let hm = &self.send_buffer_queues.read().unwrap();
        let v = hm.get(key).unwrap();
        v.clone()
    }

    fn get_recv_buffer_queue(&self, key: &String) -> Arc<Mutex<VecDeque<Box<Bytes>>>> {
        let hm = &self.recv_buffer_queues.read().unwrap();
        let v = hm.get(key).unwrap();
        v.clone()
    }

}