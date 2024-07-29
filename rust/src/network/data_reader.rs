use std::{collections::{HashMap, VecDeque}, sync::{atomic::{AtomicBool, Ordering}, Arc, Mutex, RwLock}, thread::JoinHandle};

<<<<<<< HEAD
use super::{buffer_utils::{get_buffer_id}, channel::{AckMessage, Channel}, io_loop::{Bytes, IOHandler, IOHandlerType}};
=======
use super::{buffer_utils::{get_buffer_id, new_buffer_drop_meta}, channel::{AckMessage, Channel}, io_loop::{Bytes, IOHandler, IOHandlerType}};
>>>>>>> 85a48ff ([Rustify Network] Acks WIP)
use crossbeam::{channel::{bounded, unbounded, Receiver, Sender}, queue::ArrayQueue};

const OUTPUT_QUEUE_SIZE: usize = 10;
// const RECV_CHANNEL_SIZE: usize = 10;

pub struct DataReader {
    name: String,
    channels: Vec<Channel>,

    send_chans: Arc<RwLock<HashMap<String, (Sender<Box<Bytes>>, Receiver<Box<Bytes>>)>>>,
    recv_chans: Arc<RwLock<HashMap<String, (Sender<Box<Bytes>>, Receiver<Box<Bytes>>)>>>,
    out_queue: Arc<Mutex<VecDeque<Box<Bytes>>>>,

    running: Arc<AtomicBool>,
    dispatcher_thread_handle: Arc<ArrayQueue<JoinHandle<()>>> // array queue so we do not mutate DataReader and kepp ownership
}

impl DataReader {

    pub fn new(name: String, channels: Vec<Channel>) -> DataReader {
        let n_channels = channels.len();
        let mut send_chans = HashMap::with_capacity(n_channels);
        let mut recv_chans = HashMap::with_capacity(n_channels);

        for ch in &channels {
            send_chans.insert(ch.get_channel_id().clone(), unbounded()); // TODO should we use bounded channels for acks?
            recv_chans.insert(ch.get_channel_id().clone(), unbounded()); 
            // TODO making recv_chans bounded drops throughput 10x, why?
            // recv_chans.insert(ch.get_channel_id().clone(), bounded(RECV_CHANNEL_SIZE));
        }

        DataReader{
            name,
            channels,
            send_chans: Arc::new(RwLock::new(send_chans)),
            recv_chans: Arc::new(RwLock::new(recv_chans)),
            out_queue: Arc::new(Mutex::new(VecDeque::with_capacity(OUTPUT_QUEUE_SIZE))),
            running: Arc::new(AtomicBool::new(false)),
            dispatcher_thread_handle: Arc::new(ArrayQueue::new(1))
        }
    }

    pub fn read_bytes(&self) -> Option<Box<Bytes>> {
        // TODO set limit for backpressure
        let mut locked_out_queue = self.out_queue.lock().unwrap();
        let b = locked_out_queue.pop_front();
        if !b.is_none() {
            let b = b.unwrap();
            Some(b)
        } else {
            None
        }
    }

    pub fn start(&self) {
        // start dispatcher thread: takes message from channels, in shared out_queue
        self.running.store(true, Ordering::Relaxed);

        let this_runnning = self.running.clone();
        let this_recv_chans = self.recv_chans.clone();
        let this_send_chans = self.send_chans.clone();
        let this_out_queue = self.out_queue.clone();
        let f = move || {
            loop {
                let running = this_runnning.load(Ordering::Relaxed);
                if !running {
                    break;
                }

                let locked_recv_chans = this_recv_chans.read().unwrap();
                let locked_send_chans = this_send_chans.read().unwrap();
                for channel_id in locked_recv_chans.keys() {
                    let mut locked_out_queue = this_out_queue.lock().unwrap();
                    if locked_out_queue.len() == OUTPUT_QUEUE_SIZE {
                        // full
                        drop(locked_out_queue);
                        continue
                    }
                    let recv_chan = locked_recv_chans.get(channel_id).unwrap();
                    let receiver = recv_chan.1.clone();

                    let b = receiver.try_recv();
                    if b.is_ok() {
<<<<<<< HEAD
                        let b = b.unwrap();
                        let buffer_id = get_buffer_id(b.clone());
                        // let payload = new_buffer_drop_meta(b.clone());
                        // locked_out_queue.push_back(payload);
=======
                        // TODO set low watermark

                        let b = b.unwrap();
                        let buffer_id = get_buffer_id(b.clone());
                        let payload = new_buffer_drop_meta(b.clone());
                        locked_out_queue.push_back(payload);
>>>>>>> 85a48ff ([Rustify Network] Acks WIP)

                        // send ack
                        let ack = AckMessage{channel_id: channel_id.clone(), buffer_id};
                        let send_chan = locked_send_chans.get(channel_id).unwrap();
                        let sender = send_chan.0.clone();
                        sender.send(Box::new(bincode::serialize(&ack).unwrap())).expect("ok");
                    }
                }
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