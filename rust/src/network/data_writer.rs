use std::{collections::{HashMap, VecDeque}, sync::{atomic::{AtomicBool, Ordering}, Arc, Mutex, RwLock}, thread::{self, JoinHandle}, time::{Duration, SystemTime}};

use super::{buffer_queue::{BufferQueue, MAX_BUFFERS_PER_CHANNEL}, buffer_utils::get_buffer_id, channel::{AckMessage, Channel}, io_loop::{IOHandler, IOHandlerType}, sockets::SocketMetadata};
use super::io_loop::Bytes;
use crossbeam::{channel::{bounded, Receiver, Sender}, queue::ArrayQueue};

const IN_FLIGHT_TIMEOUT_S: usize = 5; // how long to wait before re-sending un-acked buffers

pub struct DataWriter {
    name: String,
    channels: Vec<Channel>,
    send_chans: Arc<RwLock<HashMap<String, (Sender<Box<Bytes>>, Receiver<Box<Bytes>>)>>>,
    recv_chans: Arc<RwLock<HashMap<String, (Sender<Box<Bytes>>, Receiver<Box<Bytes>>)>>>,
    buffer_queue: Arc<BufferQueue>,

    in_flight: Arc<RwLock<HashMap<String, Arc<RwLock<HashMap<u32, (u128, Box<Bytes>)>>>>>>,

    running: Arc<AtomicBool>,
    io_thread_handles: Arc<ArrayQueue<JoinHandle<()>>> // array queue so we do not mutate DataReader and keep ownership
}

impl DataWriter {

    pub fn new(name: String, channels: Vec<Channel>) -> DataWriter {
        let n_channels = channels.len();
        let mut send_chans = HashMap::with_capacity(n_channels);
        let mut recv_chans = HashMap::with_capacity(n_channels);
        let mut in_flight = HashMap::with_capacity(n_channels);

        for ch in &channels {
            send_chans.insert(ch.get_channel_id().clone(), bounded(MAX_BUFFERS_PER_CHANNEL));
            recv_chans.insert(ch.get_channel_id().clone(), bounded(MAX_BUFFERS_PER_CHANNEL));
            in_flight.insert(ch.get_channel_id().clone(), Arc::new(RwLock::new(HashMap::new())));
        }

        DataWriter{
            name,
            channels: channels.to_vec(),
            send_chans: Arc::new(RwLock::new(send_chans)),
            recv_chans: Arc::new(RwLock::new(recv_chans)),
            buffer_queue: Arc::new(BufferQueue::new(channels.to_vec())),
            in_flight: Arc::new(RwLock::new(in_flight)),
            running: Arc::new(AtomicBool::new(false)),
            io_thread_handles: Arc::new(ArrayQueue::new(2))
        }
    }

    pub fn write_bytes(&self, channel_id: &String, b: Box<Bytes>, timeout_ms: i32, retry_step_micros: u64) -> Option<u128> {
        let t: u128 = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_micros();
        let mut num_retries = 0;
        loop {
            let _t = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_micros();
            if _t - t > timeout_ms as u128 * 1000 {
                return None
            }
            let succ = self.buffer_queue.try_push(channel_id, b.clone());
            if !succ {
                num_retries += 1;
                thread::sleep(Duration::from_micros(retry_step_micros));
                continue;
            }
            break;
        }
        let backpressured_time = if num_retries == 0 {0} else {SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_micros() - t};
        Some(backpressured_time)
    }

    
}

impl IOHandler for DataWriter {

    fn get_handler_type(&self) -> IOHandlerType {
        IOHandlerType::DataWriter
    }

    fn get_channels(&self) -> &Vec<Channel> {
        &self.channels
    }

    fn get_send_chan(&self, sm: &SocketMetadata) -> (Sender<Box<Bytes>>, Receiver<Box<Bytes>>) {
        let hm = &self.send_chans.read().unwrap();
        let v = hm.get(&sm.channel_id).unwrap();
        v.clone()
    }

    fn get_recv_chan(&self, sm: &SocketMetadata) -> (Sender<Box<Bytes>>, Receiver<Box<Bytes>>) {
        let hm = &self.recv_chans.read().unwrap();
        let v = hm.get(&sm.channel_id).unwrap();
        v.clone()
    }

    fn start(&self) {
        // start io threads to send buffers and receive acks
        self.running.store(true, Ordering::Relaxed);
        
        let this_send_chans = self.send_chans.clone();
        let this_buffer_queue = self.buffer_queue.clone();
        let this_in_flights = self.in_flight.clone();
        let this_runnning = self.running.clone();

        let output_loop = move || {

            while this_runnning.load(Ordering::Relaxed) {

                let locked_in_flights = this_in_flights.read().unwrap();
                let locked_send_chans = this_send_chans.read().unwrap();
                
                for channel_id in  locked_send_chans.keys() {

                    // check if in-flight buffers need to be resent first
                    let locked_in_flight = locked_in_flights.get(channel_id).unwrap().read().unwrap();
                    for in_flight_buffer_id in locked_in_flight.keys() {
                        let ts_and_b = locked_in_flight.get(in_flight_buffer_id).unwrap();
                        let now_ts = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis();
                        if now_ts - ts_and_b.0 > IN_FLIGHT_TIMEOUT_S as u128 {
                            let send_chan = locked_send_chans.get(channel_id).unwrap();
                            let sender = send_chan.0.clone();
                            if !sender.is_full() {
                                sender.send(ts_and_b.1.clone()).unwrap();
                                locked_in_flight.clone().insert(*in_flight_buffer_id, (now_ts, ts_and_b.1.clone()));
                            }
                        }
                    }

                    // stop sending new buffers if in-flight limit is reached
                    if locked_in_flight.len() == MAX_BUFFERS_PER_CHANNEL {
                        continue;
                    }
                    
                    let send_chan = locked_send_chans.get(channel_id).unwrap();
                    let sender = send_chan.0.clone();
                    if !sender.is_full() {

                        let b = this_buffer_queue.schedule_next(channel_id);
                        if b.is_some() {
                            let b = b.unwrap();
                            sender.send(b.clone()).unwrap();
                            let buffer_id = get_buffer_id(b.clone());
                            let now_ts = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis();
                            locked_in_flight.clone().insert(buffer_id, (now_ts, b.clone()));
                        }
                    }
                }
            }
        };

        let this_runnning = self.running.clone();
        let this_recv_chans = self.recv_chans.clone();
        let this_buffer_queue = self.buffer_queue.clone();
        let this_in_flights = self.in_flight.clone();
        let input_loop = move || {
            loop {
                let running = this_runnning.load(Ordering::Relaxed);
                if !running {
                    break;
                }
                let locked_in_flights = this_in_flights.write().unwrap();
                let locked_recv_chans = this_recv_chans.read().unwrap();
                for channel_id in  locked_recv_chans.keys() {
                    // poll for acks
                    let recv_chan = locked_recv_chans.get(channel_id).unwrap();
                    let receiver = recv_chan.1.clone();
                    let b = receiver.try_recv();
                    if b.is_ok() {
                        // let ack: AckMessage = bincode::deserialize(&b.unwrap()).unwrap();
                        let ack = AckMessage::de(b.unwrap());
                        let buffer_id = &ack.buffer_id;
                        // remove from in-flights
                        locked_in_flights.get(channel_id).unwrap().write().unwrap().remove(buffer_id);

                        // requets in-order pop
                        this_buffer_queue.request_pop(channel_id, *buffer_id);
                    }
                }
            }
        };

        let name = &self.name;
        let in_thread_name = format!("volga_{name}_in_thread");
        let out_thread_name = format!("volga_{name}_out_thread");
        self.io_thread_handles.push(std::thread::Builder::new().name(in_thread_name).spawn(input_loop).unwrap()).unwrap();
        self.io_thread_handles.push(std::thread::Builder::new().name(out_thread_name).spawn(output_loop).unwrap()).unwrap();
    }

    fn close (&self) {
        self.running.store(false, Ordering::Relaxed);
        while self.io_thread_handles.len() != 0 {
            let handle = self.io_thread_handles.pop();
            handle.unwrap().join().unwrap();
        }
    }
}