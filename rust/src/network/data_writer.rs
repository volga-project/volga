use core::time;
use std::{collections::{HashMap, VecDeque}, sync::{atomic::{AtomicBool, Ordering}, Arc, Mutex, RwLock}, thread::{self, JoinHandle}, time::{Duration, SystemTime}};

use super::{buffer_queues::BufferQueues, buffer_utils::get_buffer_id, channel::{AckMessage, Channel}, io_loop::{IOHandler, IOHandlerType}, metrics::{MetricsRecorder, NUM_BUFFERS_RECVD, NUM_BUFFERS_RESENT, NUM_BUFFERS_SENT, NUM_BYTES_RECVD, NUM_BYTES_SENT}, sockets::SocketMetadata, utils::sleep_thread};
use super::io_loop::Bytes;
use crossbeam::{channel::{bounded, Receiver, Sender}, queue::ArrayQueue};
use pyo3::{pyclass, pymethods};
use serde::{Deserialize, Serialize};

// const IN_FLIGHT_TIMEOUT_S: usize = 1; // how long to wait before re-sending un-acked buffers

#[derive(Serialize, Deserialize, Clone)]
#[pyclass(name="RustDataWriterConfig")]
pub struct DataWriterConfig {
    in_flight_timeout_s: usize,
    max_buffers_per_channel: usize
}

#[pymethods]
impl DataWriterConfig { 
    #[new]
    pub fn new(in_flight_timeout_s: usize, max_buffers_per_channel: usize) -> Self {
        DataWriterConfig{
            in_flight_timeout_s,
            max_buffers_per_channel
        }
    }
}

pub struct DataWriter {
    name: String,
    job_name: String,
    channels: Vec<Channel>,
    send_chans: Arc<RwLock<HashMap<String, (Sender<Box<Bytes>>, Receiver<Box<Bytes>>)>>>,
    recv_chans: Arc<RwLock<HashMap<String, (Sender<Box<Bytes>>, Receiver<Box<Bytes>>)>>>,
    buffer_queues: Arc<BufferQueues>,

    in_flight: Arc<RwLock<HashMap<String, Arc<RwLock<HashMap<u32, (u128, Box<Bytes>)>>>>>>,

    metrics_recorder: Arc<MetricsRecorder>,

    running: Arc<AtomicBool>,
    io_thread_handles: Arc<ArrayQueue<JoinHandle<()>>>, // array queue so we do not mutate DataReader and keep ownership

    // config options
    config: Arc<DataWriterConfig>
}

impl DataWriter {

    pub fn new(name: String, job_name: String, config: DataWriterConfig, channels: Vec<Channel>) -> DataWriter {
        let n_channels = channels.len();
        let mut send_chans = HashMap::with_capacity(n_channels);
        let mut recv_chans = HashMap::with_capacity(n_channels);
        let mut in_flight = HashMap::with_capacity(n_channels);

        for ch in &channels {
            send_chans.insert(ch.get_channel_id().clone(), bounded(config.max_buffers_per_channel));
            recv_chans.insert(ch.get_channel_id().clone(), bounded(config.max_buffers_per_channel));
            in_flight.insert(ch.get_channel_id().clone(), Arc::new(RwLock::new(HashMap::new())));
        }

        DataWriter{
            name: name.clone(),
            job_name: job_name.clone(),
            channels: channels.to_vec(),
            send_chans: Arc::new(RwLock::new(send_chans)),
            recv_chans: Arc::new(RwLock::new(recv_chans)),
            buffer_queues: Arc::new(BufferQueues::new(channels.to_vec(), config.max_buffers_per_channel)),
            in_flight: Arc::new(RwLock::new(in_flight)),
            metrics_recorder: Arc::new(MetricsRecorder::new(name.clone(), job_name.clone())),
            running: Arc::new(AtomicBool::new(false)),
            io_thread_handles: Arc::new(ArrayQueue::new(2)),
            config: Arc::new(config)
        }
    }

    pub fn write_bytes(&self, channel_id: &String, b: Box<Bytes>, block: bool, timeout_ms: i32, retry_step_micros: u64) -> Option<u128> {
        let t: u128 = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_micros();
        let mut num_retries = 0;
        loop {
            if !block {
                let succ = self.buffer_queues.try_push(channel_id, b.clone());
                if succ {
                    return Some(0);
                } else {
                    return None
                }
            }
            let _t = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_micros();
            if _t - t > timeout_ms as u128 * 1000 {
                return None
            }
            let succ = self.buffer_queues.try_push(channel_id, b.clone());
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

    fn get_name(&self) -> String {
        self.name.clone()
    }

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
        self.metrics_recorder.start();
        
        let this_send_chans = self.send_chans.clone();
        let this_buffer_queues = self.buffer_queues.clone();
        let this_in_flights = self.in_flight.clone();
        let this_runnning = self.running.clone();
        let this_metrics_recorder = self.metrics_recorder.clone();
        
        let this_config = self.config.clone();

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
                        if now_ts - ts_and_b.0 > this_config.in_flight_timeout_s as u128 {
                            let send_chan = locked_send_chans.get(channel_id).unwrap();
                            let sender = send_chan.0.clone();
                            if !sender.is_full() {
                                sender.send(ts_and_b.1.clone()).unwrap();
                                let size = ts_and_b.1.len();
                                locked_in_flight.clone().insert(*in_flight_buffer_id, (now_ts, ts_and_b.1.clone()));
                                this_metrics_recorder.inc(NUM_BUFFERS_RESENT, &channel_id, 1);
                                this_metrics_recorder.inc(NUM_BYTES_SENT, &channel_id, size as u64);
                            }
                        }
                    }

                    // stop sending new buffers if in-flight limit is reached
                    if locked_in_flight.len() == this_config.max_buffers_per_channel {
                        continue;
                    }
                    
                    let send_chan = locked_send_chans.get(channel_id).unwrap();
                    let sender = send_chan.0.clone();
                    if !sender.is_full() {

                        let b = this_buffer_queues.schedule_next(channel_id);
                        if b.is_some() {
                            let b = b.unwrap();
                            let size = b.len();
                            sender.send(b.clone()).unwrap();
                            let buffer_id = get_buffer_id(b.clone());
                            let now_ts = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis();
                            locked_in_flight.clone().insert(buffer_id, (now_ts, b.clone()));

                            this_metrics_recorder.inc(NUM_BUFFERS_SENT, &channel_id, 1);
                            this_metrics_recorder.inc(NUM_BYTES_SENT, &channel_id, size as u64);
                        }
                    }
                }

                sleep_thread();
            }
        };

        let this_runnning = self.running.clone();
        let this_recv_chans = self.recv_chans.clone();
        let this_buffer_queues = self.buffer_queues.clone();
        let this_in_flights = self.in_flight.clone();
        let this_metrics_recorder = self.metrics_recorder.clone();
        let input_loop = move || {
            while this_runnning.load(Ordering::Relaxed) {

                let locked_in_flights = this_in_flights.write().unwrap();
                let locked_recv_chans = this_recv_chans.read().unwrap();
                for channel_id in  locked_recv_chans.keys() {
                    // poll for acks
                    let recv_chan = locked_recv_chans.get(channel_id).unwrap();
                    let receiver = recv_chan.1.clone();
                    let b = receiver.try_recv();
                    if b.is_ok() {
                        let b = b.unwrap();
                        let size = b.len();
                        let ack = AckMessage::de(b);
                        let buffer_id = &ack.buffer_id;
                        // remove from in-flights
                        locked_in_flights.get(channel_id).unwrap().write().unwrap().remove(buffer_id);

                        // requets in-order pop
                        this_buffer_queues.request_pop(channel_id, *buffer_id);
                        this_metrics_recorder.inc(NUM_BUFFERS_RECVD, &channel_id, 1);
                        this_metrics_recorder.inc(NUM_BYTES_RECVD, &channel_id, size as u64);
                    }
                }
                
                sleep_thread();
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
        self.metrics_recorder.close();
    }
}