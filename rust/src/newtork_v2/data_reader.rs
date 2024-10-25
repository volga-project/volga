
use std::{collections::{HashMap, HashSet}, fs, sync::{atomic::{AtomicBool, AtomicI32, Ordering}, Arc, RwLock}, thread::{self, JoinHandle}, time::Duration};

use crossbeam::{channel::{bounded, tick, unbounded, Receiver, Select, Sender}, queue::ArrayQueue};
use pyo3::{pyclass, pymethods};
use serde::{Deserialize, Serialize};

use super::{buffer_utils::{get_buffer_id, get_channeld_id, new_buffer_drop_meta, Bytes}, channel::{self, Channel, DataReaderResponseMessage}, metrics::{MetricsRecorder, NUM_BUFFERS_RECVD, NUM_BYTES_RECVD, NUM_BYTES_SENT}, socket_service::{SocketMessage, SocketServiceSubscriber, CROSSBEAM_DEFAULT_CHANNEL_SIZE}, sockets::{channels_to_socket_identities, parse_ipc_path_from_addr, SocketIdentityGenerator, SocketKind, SocketMetadata}};

#[derive(Serialize, Deserialize, Clone)]
#[pyclass(name="RustDataReaderConfig")]
pub struct DataReaderConfig {
    output_queue_size: usize,
    response_batch_period_ms: Option<usize>
}

#[pymethods]
impl DataReaderConfig { 
    #[new]
    pub fn new(output_queue_size: usize, response_batch_period_ms: Option<usize>) -> Self {
        DataReaderConfig{
            output_queue_size,
            response_batch_period_ms
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


    fn schedule_ack(socket_identity: &String, channel_id: &String, buffer_id: u32, sender: &Sender<DataReaderResponseMessage>) {
        // we assume ack channels are unbounded
        let resp = DataReaderResponseMessage::new_ack(socket_identity, channel_id, buffer_id);
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

    fn get_in_sender(&self, _: &SocketMetadata) -> Sender<SocketMessage> {
        self.in_chan.0.clone()
    }

    fn get_out_receiver(&self, _: &SocketMetadata) -> Receiver<SocketMessage> {
        self.out_chan.1.clone()
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
                let (socket_identity_opt, b) = res.unwrap();

                let socket_identity = socket_identity_opt.unwrap();
                let size = b.len();
                let channel_id = &get_channeld_id(&b);
                this_metrics_recorder.inc(NUM_BUFFERS_RECVD, channel_id, 1);
                this_metrics_recorder.inc(NUM_BYTES_RECVD, channel_id, size as u64);
                let buffer_id = get_buffer_id(&b);

                let wm = locked_watermarks.get(channel_id).unwrap().load(Ordering::Relaxed);
                if buffer_id as i32 <= wm {
                    Self::schedule_ack(&socket_identity, channel_id, buffer_id, &this_response_queue.0);
                } else {
                    // We don't want out_of_order to grow infinitely and should put a limit on it,
                    // however in theory it should not happen - sender will ony send maximum of it's buffer queue size
                    // before receiving ack and sending more (which happens only after all _out_of_order is processed)
                    let locked_out_of_orders = locked_out_of_order_buffers.get(channel_id).unwrap();
                    let mut locked_out_of_order = locked_out_of_orders.write().unwrap(); 
                    
                    // TODO
                    if locked_out_of_order.len() > 1 {
                        panic!("REMOVE THIS PANIC FROM PROD!!! - we have out of order data");
                    }

                    if locked_out_of_order.contains_key(&(buffer_id as i32)) {
                        // duplicate
                        Self::schedule_ack(&socket_identity, channel_id, buffer_id, &this_response_queue.0);
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

                            // TODO this blocks acks ?
                            result_queue_sender.send(payload).unwrap();

                            // send ack
                            Self::schedule_ack(&socket_identity, channel_id, stored_buffer_id, &this_response_queue.0);
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
        let this_config = self.config.clone();
        let this_channels = self.channels.clone();

        let output_loop = move || {
            // TODO implement proper ack batching
            
            let batch_period_ms = this_config.response_batch_period_ms;
            
            let mut responses_per_channel: HashMap<String, Vec<DataReaderResponseMessage>> = HashMap::new();
            for channel in this_channels.iter() {
                responses_per_channel.insert(channel.get_channel_id().clone(), Vec::new());
            }

            let r = &this_response_queue.1;

            let mut sel = Select::new();
            sel.recv(r);

            let mut tick_size_ms = 100;
            if batch_period_ms.is_some() {
                tick_size_ms = batch_period_ms.unwrap();
            }
            let ticker = tick(Duration::from_millis(tick_size_ms as u64));
            if batch_period_ms.is_some() {
                sel.recv(&ticker);
            }

            while this_runnning.load(Ordering::Relaxed) {

                let sel_res = sel.select_timeout(Duration::from_millis(100));
                if !sel_res.is_ok() {
                    continue;
                }
                let oper = sel_res.unwrap();
                let index = oper.index();
                if index == 0 {
                    let resp = oper.recv(r).unwrap();
                    let channel_id = &resp.channel_id;
                    responses_per_channel.get_mut(channel_id).unwrap().push(resp);
                } else {
                    oper.recv(&ticker).unwrap();
                }

                if batch_period_ms.is_some() && index != 1 {
                    // batching enabled, wait for ticker to fire
                    continue;
                }

                // batch responses and send to appropriate channels
                for channel in this_channels.iter() {
                    let channel_id = channel.get_channel_id();
                    let resps = responses_per_channel.get(channel_id).unwrap();
                    if resps.len() == 0 {
                        continue;
                    }
                    let batched_resp = DataReaderResponseMessage::batch_acks(resps);
                    for resp in batched_resp {
                        let socket_identity = &resp.socket_identity;
                        let s = &this_out_chan.0;
                        let b = resp.ser();
                        let size = b.len();
                        let socket_msg = (Some(socket_identity.clone()), b);
                        while this_runnning.load(Ordering::Relaxed) {
                            let _res = s.send_timeout(socket_msg.clone(), Duration::from_millis(100));
                            if _res.is_ok() {
                                break;
                            }
                        }
                        this_metrics_recorder.inc(NUM_BYTES_SENT, &channel_id, size as u64);
                    }

                    // reset temporary batch store
                    responses_per_channel.insert(channel_id.clone(), Vec::new());
                }
                
            }
        };

        let name = &self.name;
        let in_thread_name = format!("volga_{name}_in_thread");
        let out_thread_name = format!("volga_{name}_out_thread");
        self.io_thread_handles.push(std::thread::Builder::new().name(in_thread_name).spawn(input_loop).unwrap()).unwrap();
        self.io_thread_handles.push(std::thread::Builder::new().name(out_thread_name).spawn(output_loop).unwrap()).unwrap();
    }

    fn stop (&self) {
        // TODO wait for acks to be sent, wait for in/out chans to get empty
        self.running.store(false, Ordering::Relaxed);
        while self.io_thread_handles.len() != 0 {
            let handle = self.io_thread_handles.pop();
            let r = handle.unwrap().join();
            if r.is_err() {
                println!("Panic joining DataReader thread: {:?}", r.err().unwrap().downcast_ref::<String>())
            }
        }
        self.metrics_recorder.stop();
    }
}