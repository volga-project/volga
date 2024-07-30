use std::{collections::{HashMap, VecDeque}, sync::{atomic::{AtomicBool, AtomicI32, Ordering}, Arc, Mutex, RwLock}, thread::JoinHandle};

use super::{buffer_utils::{get_buffer_id, new_buffer_drop_meta}, channel::{AckMessage, Channel}, io_loop::{Bytes, IOHandler, IOHandlerType}};
use crossbeam::{channel::{bounded, unbounded, Receiver, Sender}, queue::ArrayQueue};

const OUTPUT_QUEUE_SIZE: usize = 10;
// const RECV_CHANNEL_SIZE: usize = 10;

pub struct DataReader {
    name: String,
    channels: Vec<Channel>,

    send_chans: Arc<RwLock<HashMap<String, (Sender<Box<Bytes>>, Receiver<Box<Bytes>>)>>>,
    recv_chans: Arc<RwLock<HashMap<String, (Sender<Box<Bytes>>, Receiver<Box<Bytes>>)>>>,
    out_queue: Arc<Mutex<VecDeque<Box<Bytes>>>>,

    watermarks: Arc<RwLock<HashMap<String, Arc<AtomicI32>>>>,
    out_of_order_buffers: Arc<RwLock<HashMap<String, Arc<RwLock<HashMap<i32, Box<Bytes>>>>>>>,

    running: Arc<AtomicBool>,
    dispatcher_thread_handle: Arc<ArrayQueue<JoinHandle<()>>> // array queue so we do not mutate DataReader and kepp ownership
}

impl DataReader {

    pub fn new(name: String, channels: Vec<Channel>) -> DataReader {
        let n_channels = channels.len();
        let mut send_chans = HashMap::with_capacity(n_channels);
        let mut recv_chans = HashMap::with_capacity(n_channels);
        let mut watermarks = HashMap::with_capacity(n_channels);
        let mut out_of_order_buffers = HashMap::with_capacity(n_channels);

        for ch in &channels {
            // TODO making recv_chans bounded drops throughput 10x, why?
            send_chans.insert(ch.get_channel_id().clone(), unbounded());
            recv_chans.insert(ch.get_channel_id().clone(), unbounded()); 
            watermarks.insert(ch.get_channel_id().clone(), Arc::new(AtomicI32::new(-1)));
            out_of_order_buffers.insert(ch.get_channel_id().clone(), Arc::new(RwLock::new(HashMap::new())));   
        }

        DataReader{
            name,
            channels,
            send_chans: Arc::new(RwLock::new(send_chans)),
            recv_chans: Arc::new(RwLock::new(recv_chans)),
            out_queue: Arc::new(Mutex::new(VecDeque::with_capacity(OUTPUT_QUEUE_SIZE))),
            watermarks: Arc::new(RwLock::new(watermarks)),
            out_of_order_buffers: Arc::new(RwLock::new(out_of_order_buffers)),
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
        let this_watermarks = self.watermarks.clone();
        let this_out_of_order_buffers = self.out_of_order_buffers.clone();
        let f = move || {
            loop {
                let running = this_runnning.load(Ordering::Relaxed);
                if !running {
                    break;
                }

                let locked_recv_chans = this_recv_chans.read().unwrap();
                let locked_send_chans = this_send_chans.read().unwrap();
                let locked_watermarks = this_watermarks.read().unwrap();
                let locked_out_of_order_buffers = this_out_of_order_buffers.read().unwrap();
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
                        let b = b.unwrap();
                        let buffer_id = get_buffer_id(b.clone());

                        let wm = locked_watermarks.get(channel_id).unwrap().load(Ordering::Relaxed);
                        if buffer_id as i32 <= wm {
                            // drop and resend ack
                            let send_chan = locked_send_chans.get(channel_id).unwrap();
                            let sender = send_chan.0.clone();
                            Self::send_ack(channel_id, buffer_id, sender);
                        } else {
                            // We don't want out_of_order to grow infinitely and should put a limit on it,
                            // however in theory it should not happen - sender will ony send maximum of it's buffer queue size
                            // before receiving ack and sending more (which happens only after all _out_of_order is processed)
                            let locked_out_of_orders = locked_out_of_order_buffers.get(channel_id).unwrap();
                            let mut locked_out_of_order = locked_out_of_orders.write().unwrap(); 
                            
                            if locked_out_of_order.contains_key(&(buffer_id as i32)) {
                                // duplocate
                                let send_chan = locked_send_chans.get(channel_id).unwrap();
                                let sender = send_chan.0.clone();
                                Self::send_ack(channel_id, buffer_id, sender);
                            } else {
                                locked_out_of_order.insert(buffer_id as i32, b.clone());
                                let mut next_wm = wm + 1;
                                while locked_out_of_order.contains_key(&next_wm) {
                                    let stored_b = locked_out_of_order.get(&next_wm).unwrap();
                                    let stored_buffer_id = get_buffer_id(stored_b.clone());
                                    let payload = new_buffer_drop_meta(stored_b.clone());

                                    // TODO what if locked_out_queue is full?
                                    locked_out_queue.push_back(payload); 

                                    // send ack
                                    let send_chan = locked_send_chans.get(channel_id).unwrap();
                                    let sender = send_chan.0.clone();
                                    Self::send_ack(channel_id, stored_buffer_id, sender);
                                    locked_out_of_order.remove(&next_wm);
                                    next_wm += 1;
                                }
                                locked_watermarks.get(channel_id).unwrap().store(next_wm - 1, Ordering::Relaxed);
                            }
                        }
                    }
                }
            }
        };

        let name = &self.name;
        let thread_name = format!("volga_{name}_dispatcher_thread");
        self.dispatcher_thread_handle.push(std::thread::Builder::new().name(thread_name).spawn(f).unwrap()).unwrap();

    }

    fn send_ack(channel_id: &String, buffer_id: u32, sender: Sender<Box<Bytes>>) {
        let ack = AckMessage{channel_id: channel_id.clone(), buffer_id};
        sender.send(Box::new(bincode::serialize(&ack).unwrap())).expect("ok");
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