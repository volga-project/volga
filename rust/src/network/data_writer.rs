use core::time;
use std::{collections::{HashMap, HashSet, VecDeque}, sync::{atomic::{AtomicBool, Ordering}, Arc, Mutex, RwLock}, thread::{self, JoinHandle}, time::{Duration, SystemTime}};

use super::{buffer_queues::BufferQueues, buffer_utils::get_buffer_id, channel::{self, Channel, DataReaderResponseMessage}, channels_router::ChannelsRouter, io_loop::{IOHandler, IOHandlerType}, metrics::{MetricsRecorder, NUM_BUFFERS_RECVD, NUM_BUFFERS_RESENT, NUM_BUFFERS_SENT, NUM_BYTES_RECVD, NUM_BYTES_SENT}, sockets::SocketMetadata, utils::sleep_thread};
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
    io_thread_handles: Arc<ArrayQueue<JoinHandle<()>>>, // array queue so we do not mutate DataWriter and keep ownership

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

        // TODO this causes attempt to subtract with overflow sometimes, why?
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

            let locked_send_chans = this_send_chans.read().unwrap();

            let mut senders = HashMap::new();
            let mut receivers: HashMap<&String, &Receiver<Box<Vec<u8>>>> = HashMap::new();
            let mut receiver_to_sender_mapping = HashMap::new();
            for channel in this_channels.iter() {
                let (s, _) = locked_send_chans.get(channel.get_channel_id()).unwrap();
                let (_, r) = buffer_queues_chans.get(channel.get_channel_id()).unwrap();
                senders.insert(channel.get_channel_id(), s);
                receivers.insert(channel.get_channel_id(), r);
                receiver_to_sender_mapping.insert(channel.get_channel_id().clone(), vec![channel.get_channel_id().clone()]);
            }

            let mut s = ChannelsRouter::new(senders, receivers, &receiver_to_sender_mapping);
            
            while this_running.load(Ordering::Relaxed) {
                let result = s.iter();

                if result.is_some() {
                    let res = result.unwrap();
                    let size = res.0;
                    let channel_id = res.1;

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
                let ack = DataReaderResponseMessage::de(b);
                this_buffer_queues.handle_ack(&ack);
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