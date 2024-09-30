use core::time;
use std::{collections::HashMap, sync::{atomic::{AtomicBool, AtomicI32, Ordering}, Arc, RwLock}, thread::{self, JoinHandle}, time::Duration};

use super::{buffer_utils::{get_buffer_id, new_buffer_drop_meta}, channel::{self, Channel, DataReaderResponseMessage, DataReaderResponseMessageKind}, io_loop::{Bytes, IOHandler, IOHandlerType}, metrics::{MetricsRecorder, NUM_BUFFERS_RECVD, NUM_BYTES_RECVD, NUM_BYTES_SENT}, sockets::SocketMetadata, utils::sleep_thread};
use crossbeam::{channel::{bounded, unbounded, Receiver, Select, Sender}, queue::ArrayQueue};
use pyo3::{pyclass, pymethods};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone)]
#[pyclass(name="RustDataReaderConfig")]
pub struct DataReaderConfig {
    output_queue_size: usize
}

#[pymethods]
impl DataReaderConfig { 
    #[new]
    pub fn new(output_queue_size: usize) -> Self {
        DataReaderConfig{
            output_queue_size
        }
    }
}

pub struct DataReader {
    name: String,
    job_name: String,
    channels: Arc<Vec<Channel>>,

    send_chans: Arc<RwLock<HashMap<String, (Sender<Box<Bytes>>, Receiver<Box<Bytes>>)>>>,
    recv_chans: Arc<RwLock<HashMap<String, (Sender<Box<Bytes>>, Receiver<Box<Bytes>>)>>>,
    
    result_queue: Arc<(Sender<Box<Bytes>>, Receiver<Box<Bytes>>)>,

    response_queue: Arc<(Sender<DataReaderResponseMessage>, Receiver<DataReaderResponseMessage>)>,


    // TODO only one thread actually modifies this, can we simplify?
    watermarks: Arc<RwLock<HashMap<String, Arc<AtomicI32>>>>,
    out_of_order_buffers: Arc<RwLock<HashMap<String, Arc<RwLock<HashMap<i32, Box<Bytes>>>>>>>,

    metrics_recorder: Arc<MetricsRecorder>,

    running: Arc<AtomicBool>,
    io_thread_handles: Arc<ArrayQueue<JoinHandle<()>>>, // array queue so we do not mutate DataReader and keep ownership

    config: Arc<DataReaderConfig>
}

impl DataReader {

    pub fn new(name: String, job_name: String, data_reader_config: DataReaderConfig, channels: Vec<Channel>) -> DataReader {
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
            name: name.clone(),
            job_name: job_name.clone(),
            channels: Arc::new(channels),
            send_chans: Arc::new(RwLock::new(send_chans)),
            recv_chans: Arc::new(RwLock::new(recv_chans)),
            result_queue: Arc::new(bounded(data_reader_config.output_queue_size)),
            response_queue: Arc::new(unbounded()), // TODO bound this?
            watermarks: Arc::new(RwLock::new(watermarks)),
            out_of_order_buffers: Arc::new(RwLock::new(out_of_order_buffers)),
            metrics_recorder: Arc::new(MetricsRecorder::new(name.clone(), job_name.clone())),
            running: Arc::new(AtomicBool::new(false)),
            io_thread_handles: Arc::new(ArrayQueue::new(2)),
            config: Arc::new(data_reader_config),
        }
    }

    pub fn read_bytes(&self) -> Option<Box<Bytes>> {
        let out_queue_recvr = &self.result_queue.1;
        let b = out_queue_recvr.try_recv();
        if b.is_ok() {
            let b = b.unwrap();
            Some(b)
        } else {
            None
        }
    }

    fn schedule_ack(channel_id: &String, buffer_id: u32, sender: &Sender<DataReaderResponseMessage>) {
        // we assume ack channels are unbounded
        let resp = DataReaderResponseMessage::new_ack(channel_id, buffer_id);
        sender.send(resp).unwrap();
    }

    fn schedule_queue_update(channel_id: &String, buffer_id: u32, sender: &Sender<DataReaderResponseMessage>) {
        // we assume ack channels are unbounded
        // let resp = DataReaderResponseMessage{kind: DataReaderResponseMessageKind::QueueConsumed, channel_id: channel_id.clone(), buffer_id};
        // sender.send(resp);
    }
}

impl IOHandler for DataReader {
    
    fn get_name(&self) -> String {
        self.name.clone()
    }

    fn get_handler_type(&self) -> IOHandlerType {
        IOHandlerType::DataReader
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
        // start dispatcher thread: takes message from channels, in shared out_queue
        self.running.store(true, Ordering::Relaxed);
        self.metrics_recorder.start();

        let this_channels = self.channels.clone();
        let this_runnning = self.running.clone();
        let this_recv_chans = self.recv_chans.clone();
        let this_result_queue = self.result_queue.clone();
        let this_response_queue = self.response_queue.clone();
        let this_watermarks = self.watermarks.clone();
        let this_out_of_order_buffers = self.out_of_order_buffers.clone();
        let this_metrics_recorder = self.metrics_recorder.clone();
        let this_config = self.config.clone();

        let input_loop = move || {
            let locked_recv_chans = this_recv_chans.read().unwrap();
            let locked_watermarks = this_watermarks.read().unwrap();
            let locked_out_of_order_buffers = this_out_of_order_buffers.read().unwrap();
            
            let mut sel = Select::new();
            for channel in this_channels.iter() {
                let recv = &locked_recv_chans.get(channel.get_channel_id()).unwrap().1;
                sel.recv(recv);
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
                this_metrics_recorder.inc(NUM_BUFFERS_RECVD, channel_id, 1);
                this_metrics_recorder.inc(NUM_BYTES_RECVD, channel_id, size as u64);
                let buffer_id = get_buffer_id(b.clone());

                let wm = locked_watermarks.get(channel_id).unwrap().load(Ordering::Relaxed);
                if buffer_id as i32 <= wm {
                    Self::schedule_ack(channel_id, buffer_id, &this_response_queue.0);
                } else {
                    // We don't want out_of_order to grow infinitely and should put a limit on it,
                    // however in theory it should not happen - sender will ony send maximum of it's buffer queue size
                    // before receiving ack and sending more (which happens only after all _out_of_order is processed)
                    let locked_out_of_orders = locked_out_of_order_buffers.get(channel_id).unwrap();
                    let mut locked_out_of_order = locked_out_of_orders.write().unwrap(); 
                    
                    if locked_out_of_order.contains_key(&(buffer_id as i32)) {
                        // duplicate
                        Self::schedule_ack(channel_id, buffer_id, &this_response_queue.0);
                    } else {
                        locked_out_of_order.insert(buffer_id as i32, b.clone());
                        let mut next_wm = wm + 1;
                        while locked_out_of_order.contains_key(&next_wm) {
                            if this_result_queue.1.len() == this_config.output_queue_size {
                                // full
                                break;
                            }

                            let stored_b = locked_out_of_order.get(&next_wm).unwrap();
                            let stored_buffer_id = get_buffer_id(stored_b.clone());
                            let payload = new_buffer_drop_meta(stored_b.clone());

                            let result_queue_sender = &this_result_queue.0;
                            result_queue_sender.send(payload).unwrap();

                            // send ack
                            Self::schedule_ack(channel_id, stored_buffer_id, &this_response_queue.0);
                            locked_out_of_order.remove(&next_wm);
                            next_wm += 1;
                        }
                        locked_watermarks.get(channel_id).unwrap().store(next_wm - 1, Ordering::Relaxed);
                    }
                }
            }
        };


        let this_runnning = self.running.clone();
        let this_send_chans = self.send_chans.clone();
        let this_response_queue = self.response_queue.clone();
        let this_metrics_recorder = self.metrics_recorder.clone();

        let output_loop = move || {
            let timeout_ms = 100;

            // TODO unit tests for batching
            let max_batch_size = 500;
            let locked_send_chans = this_send_chans.read().unwrap();
            let mut last_resp = None;
            while this_runnning.load(Ordering::Relaxed) {
                let r = &this_response_queue.1;
                let mut batch = vec![];
                if last_resp.is_some() {
                    batch.push(last_resp.unwrap());
                }
                let mut resp = r.try_recv();
                while resp.is_ok() && batch.len() <= max_batch_size {
                    let _resp = resp.unwrap();
                    batch.push(_resp);
                    resp = r.try_recv();
                }
                if batch.len() != 0 {
                    let channel_id = &batch[0].channel_id;
                    let batched = DataReaderResponseMessage::batch_acks(&batch);
                    let (s, _) = locked_send_chans.get(channel_id).unwrap();
                    for batched_message in batched {
                        let b = &batched_message.ser();
                        let size = b.len();
                        s.send(b.clone()).expect("Unable to send scheduled response "); // TODO timeout?
                        this_metrics_recorder.inc(NUM_BYTES_SENT, &channel_id, size as u64);
                    }
                }

                if resp.is_ok() {
                    last_resp = Some(resp.unwrap());
                    continue;
                } else {
                    let _resp = r.recv_timeout(Duration::from_millis(timeout_ms));
                    if _resp.is_ok() {
                        last_resp = Some(_resp.unwrap());
                    } else {
                        last_resp = None;
                    }
                }
                
                thread::sleep(time::Duration::from_millis(100));
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