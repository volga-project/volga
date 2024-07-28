use std::{collections::HashMap, sync::{atomic::{AtomicBool, Ordering}, Arc, RwLock}, thread::JoinHandle};

use super::{channel::Channel, io_loop::{Bytes, IOHandler, IOHandlerType}};
use crossbeam::{channel::{unbounded, Sender, Receiver}, queue::ArrayQueue};

const OUTPUT_QUEUE_SIZE: usize = 100000;

pub struct DataReader {
    name: String,
    channels: Vec<Channel>,

    // TODO maps are read-only, do we need locks here?
    send_chans: Arc<RwLock<HashMap<String, (Sender<Box<Bytes>>, Receiver<Box<Bytes>>)>>>,
    recv_chans: Arc<RwLock<HashMap<String, (Sender<Box<Bytes>>, Receiver<Box<Bytes>>)>>>,
    out_queue: Arc<ArrayQueue<Box<Bytes>>>,

    running: Arc<AtomicBool>,
    dispatcher_thread_handle: Arc<ArrayQueue<JoinHandle<()>>> // array queue so we do not mutate DataReader and kepp ownership
}

impl DataReader {

    pub fn new(name: String, channels: Vec<Channel>) -> DataReader {
        let n_channels = channels.len();
        let mut send_chans = HashMap::with_capacity(n_channels);
        let mut recv_chans = HashMap::with_capacity(n_channels);

        // TODO we should use bounded channels here?
        for ch in &channels {
            send_chans.insert(ch.get_channel_id().clone(), unbounded());
            recv_chans.insert(ch.get_channel_id().clone(), unbounded());
        }

        DataReader{
            name,
            channels,
            send_chans: Arc::new(RwLock::new(send_chans)),
            recv_chans: Arc::new(RwLock::new(recv_chans)),
            out_queue: Arc::new(ArrayQueue::new(OUTPUT_QUEUE_SIZE)),
            running: Arc::new(AtomicBool::new(false)),
            dispatcher_thread_handle: Arc::new(ArrayQueue::new(1))
        }
    }

    pub fn read_bytes(&self) -> Option<Box<Bytes>> {
        // TODO set limit for backpressure
        let b = self.out_queue.pop();
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
        let this_out_queue = self.out_queue.clone();
        let f = move || {
            loop {
                let running = this_runnning.load(Ordering::Relaxed);
                if !running {
                    break;
                }

                let locked_map = this_recv_chans.read().unwrap();
                for channel_id in locked_map.keys() {
                    let recv_chan = locked_map.get(channel_id).unwrap();
                    let receiver = recv_chan.1.clone();
                    let b = receiver.try_recv();
                    if b.is_ok() {
                        this_out_queue.push(b.unwrap()).unwrap();
                    }
                }
                drop(locked_map);
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