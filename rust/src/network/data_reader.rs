use std::{collections::{HashMap, VecDeque}, sync::{atomic::{AtomicBool, Ordering}, Arc, Mutex, RwLock}, thread::JoinHandle};

use super::{channel::{Channel, ChannelMessage}, io_loop::{Bytes, IOHandler, IOHandlerType}};

pub struct DataReader {
    name: String,
    channels: Vec<Channel>,
    send_buffer_queues: Arc<RwLock<HashMap<String, Arc<Mutex<VecDeque<Box<Bytes>>>>>>>,
    recv_buffer_queues: Arc<RwLock<HashMap<String, Arc<Mutex<VecDeque<Box<Bytes>>>>>>>,
    out_message_queue: Arc<Mutex<VecDeque<Box<ChannelMessage>>>>,

    running: Arc<AtomicBool>,
    dispatcher_thread: Option<JoinHandle<()>>
}

impl DataReader {

    pub fn new(name: String, channels: Vec<Channel>) -> DataReader {
        let send_queues =  Arc::new(RwLock::new(HashMap::new()));
        let recv_queues =  Arc::new(RwLock::new(HashMap::new()));

        for ch in &channels {
            send_queues.write().unwrap().insert(ch.get_channel_id().clone(), Arc::new(Mutex::new(VecDeque::new())));
            recv_queues.write().unwrap().insert(ch.get_channel_id().clone(), Arc::new(Mutex::new(VecDeque::new())));
        }

        DataReader{
            name,
            channels,
            send_buffer_queues: send_queues,
            recv_buffer_queues: recv_queues,
            out_message_queue: Arc::new(Mutex::new(VecDeque::new())),
            running: Arc::new(AtomicBool::new(false)),
            dispatcher_thread: None
        }
    }

    pub fn read_message(&self) -> Option<Box<ChannelMessage>> {
        // TODO set limit for backpressure
        let msg = self.out_message_queue.lock().unwrap().pop_front();
        if !msg.is_none() {
            let msg = msg.unwrap();
            Some(msg)
        } else {
            None
        }
    }

    pub fn start(&mut self) {
        // start dispatcher thread: takes message from out_buffer_queue, passes to deserializators pool, puts result in out_msg_queue
        self.running.store(true, Ordering::Relaxed);

        let this_out_message_queue = self.out_message_queue.clone();
        let this_recv_buffer_queues = self.recv_buffer_queues.clone();
        let this_runnning = self.running.clone();
        let f = move || {
            loop {
                let running = this_runnning.load(Ordering::Relaxed);
                if !running {
                    break;
                }

                let b_recv_queues = this_recv_buffer_queues.read().unwrap();
                for channel_id in  b_recv_queues.keys() {
                    let mut locked_b_recv_queue = b_recv_queues.get(channel_id).unwrap().lock().unwrap();
                    let b = locked_b_recv_queue.pop_front();
                    drop(locked_b_recv_queue); // release lock
                    if !b.is_none() {
                        // TODO we should asynchronously dispatch to serde pool here, for simplicity we do sequential serde
                        let b = b.unwrap();
                        // let msg: ChannelMessage = serde_json::from_str(&String::from_utf8(*b).unwrap()).unwrap();

                        let msg = bincode::deserialize(b.as_ref()).unwrap();
                        
                        // put to out msg queue
                        this_out_message_queue.lock().unwrap().push_back(Box::new(msg));
                    }
                }
            }
            
        };

        let name = &self.name;
        let thread_name = format!("volga_{name}_dispatcher_thread");
        self.dispatcher_thread = Some(std::thread::Builder::new().name(thread_name).spawn(f).unwrap());

    }

    pub fn close (&self) {
        self.running.store(false, Ordering::Relaxed);
        // TODO join thread
    }
}

impl IOHandler for DataReader {

    fn get_handler_type(&self) -> IOHandlerType {
        IOHandlerType::DataReader
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