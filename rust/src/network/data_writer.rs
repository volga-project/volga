use core::time;
use std::{collections::{HashMap, HashSet, VecDeque}, sync::{atomic::{AtomicBool, Ordering}, Arc, Mutex, RwLock}, thread::{self, JoinHandle}, time::{Duration, SystemTime}};

use super::{buffer_queues::BufferQueues, buffer_utils::get_buffer_id, channel::{self, AckMessage, Channel}, io_loop::{IOHandler, IOHandlerType}, metrics::{MetricsRecorder, NUM_BUFFERS_RECVD, NUM_BUFFERS_RESENT, NUM_BUFFERS_SENT, NUM_BYTES_RECVD, NUM_BYTES_SENT}, sockets::SocketMetadata, utils::sleep_thread};
use super::io_loop::Bytes;
use crossbeam::{channel::{bounded, Receiver, Select, Sender}, queue::ArrayQueue};
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
    channels: Arc<Vec<Channel>>,
    send_chans: Arc<RwLock<HashMap<String, (Sender<Box<Bytes>>, Receiver<Box<Bytes>>)>>>,
    recv_chans: Arc<RwLock<HashMap<String, (Sender<Box<Bytes>>, Receiver<Box<Bytes>>)>>>,
    buffer_queues: Arc<BufferQueues>,

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

        for ch in &channels {
            send_chans.insert(ch.get_channel_id().clone(), bounded(config.max_buffers_per_channel));
            recv_chans.insert(ch.get_channel_id().clone(), bounded(config.max_buffers_per_channel));
        }

        let bqs = BufferQueues::new(&channels, config.max_buffers_per_channel, config.in_flight_timeout_s);

        DataWriter{
            name: name.clone(),
            job_name: job_name.clone(),
            channels: Arc::new(channels.to_vec()),
            send_chans: Arc::new(RwLock::new(send_chans)),
            recv_chans: Arc::new(RwLock::new(recv_chans)),
            buffer_queues: Arc::new(bqs),
            metrics_recorder: Arc::new(MetricsRecorder::new(name.clone(), job_name.clone())),
            running: Arc::new(AtomicBool::new(false)),
            io_thread_handles: Arc::new(ArrayQueue::new(2)),
            config: Arc::new(config)
        }
    }

    pub fn write_bytes(&self, channel_id: &String, b: Box<Bytes>, timeout_ms: u128) -> Option<u128> {
        let t = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis();
        loop {
            let succ = self.buffer_queues.try_push(channel_id, b.clone());
            if !succ {
                let _t = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis();
                if _t - t > timeout_ms {
                    return None
                }
                continue;
            }
            break;
        }
        let backpressured_time: u128 = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() - t;
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
        self.buffer_queues.start();
        
        let this_channels = self.channels.clone();
        let this_send_chans = self.send_chans.clone();
        let buffer_queues_chans = self.buffer_queues.get_chans().clone();
        let this_running = self.running.clone();
        let this_metrics_recorder = self.metrics_recorder.clone();

        let output_loop = move || {

            let mut sel = Select::new();
            let locked_send_chans = this_send_chans.read().unwrap();
            let mut indexes = HashMap::new();

            // order matters here - register recv first, send second
            for channel in this_channels.iter() {
                let (_, r) = buffer_queues_chans.get(channel.get_channel_id()).unwrap();
                let i = sel.recv(r);
                indexes.insert(i, (channel.get_channel_id(), false));
            }

            for channel in this_channels.iter() {
                let (s, _) = locked_send_chans.get(channel.get_channel_id()).unwrap();
                let i = sel.send(s);
                indexes.insert(i, (channel.get_channel_id(), true));
            }

            // since select returns only one sender/receiver we keep track of ready senders/recived buffers to match them
            let mut buffers_ready_to_send: HashMap<&String, Box<Vec<u8>>> = HashMap::new();
            let mut ready_senders: HashMap<&String, &Sender<Box<Vec<u8>>>> = HashMap::new();

            while this_running.load(Ordering::Relaxed) {

                let index = sel.ready_timeout(Duration::from_millis(100));
                if !index.is_ok() {
                    continue;
                }

                let index: usize = index.unwrap();
                let (channel_id, is_sender) = indexes.get(&index).copied().unwrap();

                let (s, _) = locked_send_chans.get(channel_id).unwrap();
                let (_, r) = buffer_queues_chans.get(channel_id).unwrap();

                let mut sent_size = None;

                if is_sender {
                    // sender
                    if buffers_ready_to_send.contains_key(channel_id) {
                        // we have a match, send
                        let b = buffers_ready_to_send.get(channel_id).unwrap();
                        let res = s.try_send(b.clone());
                        if !res.is_ok() {
                            println!("Unable to send");
                            continue;
                        }

                        sent_size = Some(b.len());

                        // remove stored data and re-register receiver
                        buffers_ready_to_send.remove(channel_id);
                        let i = sel.recv(r);
                        indexes.insert(i, (channel_id, false));
                    } else {
                        // mark as ready and remove from selector until we have a matching receiver
                        ready_senders.insert(channel_id, s);
                        sel.remove(index);
                        indexes.remove(&index);
                    }
                } else {
                    // receiver
                    // let b = r.try_recv().expect("Can not receive");
                    let b = r.try_recv();
                    if !b.is_ok() {
                        println!("Unable to rcv");
                        continue;
                    }
                    let b = b.unwrap();
                    if ready_senders.contains_key(channel_id) {
                        // we have a match, send
                        let s = ready_senders.get(channel_id).copied().unwrap();
                        s.send(b.clone()).expect("Unable to send");
                        sent_size = Some(b.len());

                        // re-register sender
                        ready_senders.remove(channel_id);
                        let i = sel.send(s);
                        indexes.insert(i, (channel_id, true));
                    } else {
                        // store received data and remove from selector until we have a matching sender
                        buffers_ready_to_send.insert(channel_id, b);
                        sel.remove(index);
                        indexes.remove(&index);
                    }
                }

                if sent_size.is_some() {
                    let size = sent_size.unwrap();

                    this_metrics_recorder.inc(NUM_BUFFERS_SENT, &channel_id, 1);
                    this_metrics_recorder.inc(NUM_BYTES_SENT, &channel_id, size as u64);
                }
            }
        };

        let this_channels = self.channels.clone();
        let this_runnning = self.running.clone();
        let this_recv_chans = self.recv_chans.clone();
        let this_buffer_queues = self.buffer_queues.clone();
        let this_metrics_recorder = self.metrics_recorder.clone();
        let input_loop = move || {

            let mut sel = Select::new();
            let locked_recv_chans = this_recv_chans.read().unwrap();
            for channel in this_channels.iter() {
                let (_, r) = locked_recv_chans.get(channel.get_channel_id()).unwrap();
                sel.recv(r);
            }

            while this_runnning.load(Ordering::Relaxed) {

                let oper = sel.select_timeout(time::Duration::from_millis(100));
                if !oper.is_ok() {
                    continue;
                }
                let oper = oper.unwrap();
                let index = oper.index();
                let channel_id = this_channels[index].get_channel_id();
                let recv = &locked_recv_chans.get(channel_id).unwrap().1;
                let b = oper.recv(recv).unwrap();
                let size = b.len();
                let ack = AckMessage::de(b);
                let buffer_id = ack.buffer_id;
                this_buffer_queues.handle_ack(channel_id, buffer_id);
                this_metrics_recorder.inc(NUM_BUFFERS_RECVD, &channel_id, 1);
                this_metrics_recorder.inc(NUM_BYTES_RECVD, &channel_id, size as u64);
            }
        };

        let name = &self.name;
        let in_thread_name = format!("volga_{name}_in_thread");
        let out_thread_name = format!("volga_{name}_out_thread");
        self.io_thread_handles.push(std::thread::Builder::new().name(in_thread_name).spawn(input_loop).unwrap()).unwrap();
        self.io_thread_handles.push(std::thread::Builder::new().name(out_thread_name).spawn(output_loop).unwrap()).unwrap();
    }

    fn close (&self) {
        self.buffer_queues.close();
        self.running.store(false, Ordering::Relaxed);
        while self.io_thread_handles.len() != 0 {
            let handle = self.io_thread_handles.pop();
            handle.unwrap().join().unwrap();
        }
        self.metrics_recorder.close();
    }
}