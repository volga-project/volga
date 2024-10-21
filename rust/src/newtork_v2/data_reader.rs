
use std::{collections::{HashMap, HashSet}, fs, sync::{atomic::{AtomicBool, AtomicI32, Ordering}, Arc, RwLock}, thread::{self, JoinHandle}, time::Duration};

use crossbeam::{channel::{bounded, unbounded, Receiver, Sender}, queue::ArrayQueue};
use pyo3::{pyclass, pymethods};
use serde::{Deserialize, Serialize};

use super::{buffer_utils::{get_buffer_id, get_channeld_id, new_buffer_drop_meta, Bytes}, channel::{Channel, DataReaderResponseMessage}, metrics::{MetricsRecorder, NUM_BUFFERS_RECVD, NUM_BYTES_RECVD, NUM_BYTES_SENT}, socket_service::{SocketMessage, SocketServiceSubscriber, CROSSBEAM_DEFAULT_CHANNEL_SIZE}, sockets::{channels_to_socket_identities, parse_ipc_path_from_addr, SocketIdentityGenerator, SocketKind, SocketMetadata}};

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
    id: String,
    name: String,
    job_name: String,
    channels: Arc<Vec<Channel>>,

    in_chan: Arc<(Sender<SocketMessage>, Receiver<SocketMessage>)>,
    out_chan: Arc<(Sender<SocketMessage>, Receiver<SocketMessage>)>,
    socket_metas: Vec<SocketMetadata>,
    
    result_queue: Arc<(Sender<Bytes>, Receiver<Bytes>)>,

    response_queue: Arc<(Sender<DataReaderResponseMessage>, Receiver<DataReaderResponseMessage>)>,


    // TODO only one thread actually modifies this, can we simplify?
    watermarks: Arc<RwLock<HashMap<String, Arc<AtomicI32>>>>,
    out_of_order_buffers: Arc<RwLock<HashMap<String, Arc<RwLock<HashMap<i32, Bytes>>>>>>,

    metrics_recorder: Arc<MetricsRecorder>,

    running: Arc<AtomicBool>,
    io_thread_handles: Arc<ArrayQueue<JoinHandle<()>>>, // array queue so we do not mutate DataReader and keep ownership

    config: Arc<DataReaderConfig>
}

impl DataReader {

    pub fn new(id: String, name: String, job_name: String, data_reader_config: DataReaderConfig, channels: Vec<Channel>) -> DataReader {
        let n_channels = channels.len();
        let socket_metas = DataReader::configure_sockets(&id, &name, &channels);

        let mut watermarks = HashMap::with_capacity(n_channels);
        let mut out_of_order_buffers = HashMap::with_capacity(n_channels);

        for ch in &channels {
            watermarks.insert(ch.get_channel_id().clone(), Arc::new(AtomicI32::new(-1)));
            out_of_order_buffers.insert(ch.get_channel_id().clone(), Arc::new(RwLock::new(HashMap::new())));   
        }

        DataReader{
            id,
            name: name.clone(),
            job_name: job_name.clone(),
            channels: Arc::new(channels),
            in_chan: Arc::new(bounded(CROSSBEAM_DEFAULT_CHANNEL_SIZE)),
            out_chan: Arc::new(bounded(CROSSBEAM_DEFAULT_CHANNEL_SIZE)),
            socket_metas,
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

    pub fn read_bytes(&self) -> Option<Bytes> {
        let out_queue_recvr = &self.result_queue.1;
        let b = out_queue_recvr.try_recv();
        if b.is_ok() {
            let b = b.unwrap();
            Some(b)
        } else {
            None
        }
    }
    
    fn configure_sockets(id: &String, name: &String, channels: &Vec<Channel>) -> Vec<SocketMetadata> {
        let mut socket_identity_generator = SocketIdentityGenerator::new(id.clone());
        let mut socket_metas = Vec::new();

        // one ROUTER for all channels (both local and remote)
        let mut ipc_addrs: HashSet<String> = HashSet::new();
        for channel in channels {
            match channel {
                Channel::Local{channel_id, ipc_addr} => {
                    ipc_addrs.insert(ipc_addr.clone());       
                },
                Channel::Remote { channel_id, source_local_ipc_addr, source_node_ip, source_node_id, target_local_ipc_addr, .. } => {
                    ipc_addrs.insert(target_local_ipc_addr.clone());       
                }
            }
        }

        if ipc_addrs.len() != 1 {
            panic!("Duplicate ipc addrs for {name}")
        }
        let ipc_addr = ipc_addrs.iter().next().unwrap().clone();
        let ipc_path = parse_ipc_path_from_addr(&ipc_addr);
        fs::create_dir_all(ipc_path).unwrap();

        let socket_identity = socket_identity_generator.gen();
        let socket_meta = SocketMetadata{
            identity: socket_identity.clone(),
            owner_id: id.clone(),
            kind: SocketKind::Router,
            channel_ids: channels.iter().map(|channel| channel.get_channel_id().clone()).collect(),
            addr: ipc_addr,
        };
        socket_metas.push(socket_meta);
        socket_metas
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

impl SocketServiceSubscriber for DataReader {

    fn get_id(&self) -> &String {
        &self.id
    }

    fn get_name(&self) -> &String {
        &self.name
    }

    fn get_channels(&self) -> &Vec<Channel> {
        &self.channels
    }

    fn get_sockets_metas(&self) -> &Vec<SocketMetadata> {
        &self.socket_metas
    }

    fn get_in_chan(&self, _: &SocketMetadata) -> (Sender<SocketMessage>, Receiver<SocketMessage>) {
        (self.in_chan.0.clone(), self.in_chan.1.clone())
    }

    fn get_out_chan(&self, _: &SocketMetadata) -> (Sender<SocketMessage>, Receiver<SocketMessage>) {
        (self.out_chan.0.clone(), self.out_chan.1.clone())
    }

    fn start(&self) {
        self.running.store(true, Ordering::Relaxed);
        self.metrics_recorder.start();

        let this_runnning = self.running.clone();
        let this_in_chan = self.in_chan.clone();
        let this_result_queue = self.result_queue.clone();
        let this_response_queue = self.response_queue.clone();
        let this_watermarks = self.watermarks.clone();
        let this_out_of_order_buffers = self.out_of_order_buffers.clone();
        let this_metrics_recorder = self.metrics_recorder.clone();
        let this_config = self.config.clone();

        let input_loop = move || {
            let locked_watermarks = this_watermarks.read().unwrap();
            let locked_out_of_order_buffers = this_out_of_order_buffers.read().unwrap();

            while this_runnning.load(Ordering::Relaxed) {

                let res = this_in_chan.1.recv_timeout(Duration::from_millis(100));
                if !res.is_ok() {
                    continue;
                }
                let (_, b) = res.unwrap();

                let size = b.len();
                let channel_id = &get_channeld_id(&b);
                this_metrics_recorder.inc(NUM_BUFFERS_RECVD, channel_id, 1);
                this_metrics_recorder.inc(NUM_BYTES_RECVD, channel_id, size as u64);
                let buffer_id = get_buffer_id(&b);

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
                            let stored_buffer_id = get_buffer_id(stored_b);
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
        let this_out_chan = self.out_chan.clone();
        let this_response_queue = self.response_queue.clone();
        let this_metrics_recorder = self.metrics_recorder.clone();
        let this_socket_metas = self.socket_metas.clone();

        let output_loop = move || {
            let channel_id_to_socket_id = channels_to_socket_identities(this_socket_metas);
            
            // TODO implement proper ack batching
            while this_runnning.load(Ordering::Relaxed) {
                let r = &this_response_queue.1;
                let resp = r.recv_timeout(Duration::from_millis(100));
                if !resp.is_ok() {
                    continue;
                }
                let resp = resp.unwrap();
                let channel_id = &resp.channel_id;
                let socket_identity = channel_id_to_socket_id.get(channel_id).unwrap();

                let s = &this_out_chan.0;
                let b = resp.ser();
                let size = b.len();
                let socket_msg = (Some(socket_identity.clone()), b);
                let res = s.try_send(socket_msg.clone());
                if !res.is_ok() {
                    s.send(socket_msg.clone()).unwrap();
                    println!("DataReader out chan should not backpressure");
                }
                this_metrics_recorder.inc(NUM_BYTES_SENT, &channel_id, size as u64);
            }
        };

        let name = &self.name;
        let in_thread_name = format!("volga_{name}_in_thread");
        let out_thread_name = format!("volga_{name}_out_thread");
        self.io_thread_handles.push(std::thread::Builder::new().name(in_thread_name).spawn(input_loop).unwrap()).unwrap();
        self.io_thread_handles.push(std::thread::Builder::new().name(out_thread_name).spawn(output_loop).unwrap()).unwrap();
    }

    fn stop (&self) {
        self.running.store(false, Ordering::Relaxed);
        while self.io_thread_handles.len() != 0 {
            let handle = self.io_thread_handles.pop();
            let r = handle.unwrap().join();
            if r.is_err() {
                println!("Panic joining DataReader thread: {:?}", r.err().unwrap().downcast_ref::<String>())
            }
        }
        self.metrics_recorder.close();
    }
}