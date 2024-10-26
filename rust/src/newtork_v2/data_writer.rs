
use std::{collections::{HashMap, HashSet}, fs, sync::{atomic::{AtomicBool, Ordering}, Arc, RwLock}, thread::JoinHandle, time::{Duration, SystemTime}};

use crate::newtork_v2::buffer_utils::get_buffer_id;

use super::{buffer_queues::BufferQueues, buffer_utils::Bytes, channel::{to_local_and_remote, Channel, DataReaderResponseMessage}, metrics::{MetricsRecorder, NUM_BUFFERS_RECVD, NUM_BUFFERS_RESENT, NUM_BUFFERS_SENT, NUM_BYTES_RECVD, NUM_BYTES_SENT}, socket_service::{SocketServiceSubscriber, CROSSBEAM_DEFAULT_CHANNEL_SIZE}, sockets::{channels_to_socket_identities, parse_ipc_path_from_addr, SocketIdentityGenerator, SocketKind, SocketMetadata}};
use crossbeam::{channel::{bounded, Receiver, Select, Sender}, queue::ArrayQueue};
use pyo3::{pyclass, pymethods};
use serde::{Deserialize, Serialize};


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
    id: String,
    name: String,
    job_name: String,
    channels: Arc<Vec<Channel>>,
    socket_metas: Vec<SocketMetadata>,
    in_chans: Arc<RwLock<HashMap<String, (Sender<Bytes>, Receiver<Bytes>)>>>,
    out_chans: Arc<RwLock<HashMap<String, (Sender<Bytes>, Receiver<Bytes>)>>>,
    buffer_queues: Arc<BufferQueues>,

    metrics_recorder: Arc<MetricsRecorder>,

    running: Arc<AtomicBool>,
    io_thread_handles: Arc<ArrayQueue<JoinHandle<()>>>, // array queue so we do not mutate DataWriter and keep ownership

    // config options
    config: Arc<DataWriterConfig>
}

impl DataWriter {

    pub fn new(id: String, name: String, job_name: String, config: DataWriterConfig, channels: Vec<Channel>) -> DataWriter {
        let (socket_metas, in_chans, out_chans) = DataWriter::configure_sockets_and_io_chans(&id, &name, &channels);

        let bqs = BufferQueues::new(&channels, config.max_buffers_per_channel, config.in_flight_timeout_s);

        DataWriter{
            id: id.clone(),
            name: name.clone(),
            job_name: job_name.clone(),
            channels: Arc::new(channels.to_vec()),
            socket_metas,
            in_chans: Arc::new(RwLock::new(in_chans)),
            out_chans: Arc::new(RwLock::new(out_chans)),
            buffer_queues: Arc::new(bqs),
            metrics_recorder: Arc::new(MetricsRecorder::new(name.clone(), job_name.clone())),
            running: Arc::new(AtomicBool::new(false)),
            io_thread_handles: Arc::new(ArrayQueue::new(2)),
            config: Arc::new(config)
        }
    }

    pub fn write_bytes(&self, channel_id: &String, b: Bytes, timeout_ms: u128) -> Option<u128> {
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

    fn configure_sockets_and_io_chans(id: &String, name: &String, channels: &Vec<Channel>) 
    -> (
        Vec<SocketMetadata>, 
        HashMap<String, (Sender<Bytes>, Receiver<Bytes>)>, 
        HashMap<String, (Sender<Bytes>, Receiver<Bytes>)>
    ) {
        let mut socket_identity_generator = SocketIdentityGenerator::new(id.clone());
        let mut socket_metas = Vec::new();
        let mut in_chans = HashMap::new();
        let mut out_chans = HashMap::new();

        let (local_channels, remote_channels) = to_local_and_remote(&channels);
        // one DEALER per local channel, one DEALER for all remote channels
        for channel in local_channels {
            match channel {
                Channel::Local{channel_id, ipc_addr} => {
                    let ipc_path = parse_ipc_path_from_addr(&ipc_addr);
                    fs::create_dir_all(ipc_path).unwrap();
                    let socket_identity = socket_identity_generator.gen();
                    let local_socket_meta = SocketMetadata{
                        identity: socket_identity.clone(),
                        owner_id: id.clone(),
                        kind: SocketKind::Dealer,
                        channel_ids: vec![channel_id.clone()],
                        addr: ipc_addr.clone(),
                    };
                    socket_metas.push(local_socket_meta);
                    if in_chans.contains_key(&socket_identity) {
                        panic!("Duplicate socket identity {socket_identity}")
                    }
                    in_chans.insert(socket_identity.clone(), bounded(CROSSBEAM_DEFAULT_CHANNEL_SIZE));
                    out_chans.insert(socket_identity.clone(), bounded(CROSSBEAM_DEFAULT_CHANNEL_SIZE));
                },
                Channel::Remote{..} => {panic!("only local")}
            }
        }

        if remote_channels.len() != 0 {
            // assert all remote channel source ipc_addr are the same
            let mut ipc_addrs: HashSet<String> = HashSet::new();
            for channel in &remote_channels {
                match channel {
                    Channel::Local{..} => {panic!("only remote")},
                    Channel::Remote { channel_id, source_local_ipc_addr, ..} => {
                        ipc_addrs.insert(source_local_ipc_addr.clone());       
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
            let remote_socket_meta = SocketMetadata{
                identity: socket_identity.clone(),
                owner_id: id.clone(),
                kind: SocketKind::Dealer,
                channel_ids: remote_channels.iter().map(|channel| channel.get_channel_id().clone()).collect(),
                addr: ipc_addr,
            };
            socket_metas.push(remote_socket_meta);
            in_chans.insert(socket_identity.clone(), bounded(CROSSBEAM_DEFAULT_CHANNEL_SIZE));
            out_chans.insert(socket_identity.clone(), bounded(CROSSBEAM_DEFAULT_CHANNEL_SIZE));
        }
        (socket_metas, in_chans, out_chans)
    }
}

impl SocketServiceSubscriber for DataWriter {

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

    fn get_in_sender(&self, sm: &SocketMetadata) -> Sender<Bytes> {
        self.in_chans.read().unwrap().get(&sm.identity).unwrap().clone().0
    }

    fn get_out_receiver(&self, sm: &SocketMetadata) -> Receiver<Bytes> {
        self.out_chans.read().unwrap().get(&sm.identity).unwrap().clone().1
    }

    fn start(&self) {
        // start io threads to send buffers and receive acks
        self.running.store(true, Ordering::Relaxed);
        self.metrics_recorder.start();
        self.buffer_queues.start();
        
        let this_out_chans = self.out_chans.clone();
        let buffer_queues_out_chan = self.buffer_queues.get_out_chan().clone();
        let this_running = self.running.clone();
        let this_metrics_recorder = self.metrics_recorder.clone();
        let this_socket_metas = self.socket_metas.clone();

        // moves data from buffer queues to output channel
        let output_loop = move || {

            let channel_id_to_socket_id = channels_to_socket_identities(this_socket_metas);
            let mut out_chans = HashMap::new();
            let locked_out_chans = this_out_chans.read().unwrap();
            for (socket_identity, (s, r)) in locked_out_chans.iter() {
                out_chans.insert(socket_identity.clone(), (s.clone(), r.clone()));
            }
            drop(locked_out_chans);

            while this_running.load(Ordering::Relaxed) {
                let ch_and_b = buffer_queues_out_chan.1.recv_timeout(Duration::from_millis(100));
                if !ch_and_b.is_ok() {
                    continue;
                }
                let (channel_id, b) = ch_and_b.unwrap();
                let socket_identity = channel_id_to_socket_id.get(&channel_id).unwrap();

                let out_chan = out_chans.get(socket_identity).unwrap();
                let sender = &out_chan.0.clone();

                // Data Writer writes to DEALERs - no need to send socket_identity
                let res = sender.try_send(b.clone());
                if !res.is_ok() {
                    // we have a backpressure here which blocks all other channels. Ideally this should not happen
                    // since we rely on higher level credit-based flow control to handle backpressure. 
                    // TODO we should log blocking here and see in which cases this happens
                    while this_running.load(Ordering::Relaxed) {
                        let bid = get_buffer_id(&b);
                        println!("Wasteful backpressure channel_id: {channel_id}, socket_identity: {socket_identity}, buffer_id: {bid}");
                        
                        let res = sender.send_timeout(b.clone(), Duration::from_millis(1000));
                        if res.is_ok() {
                            break;
                        }
                    }
                }
                let size = b.len();

                this_metrics_recorder.inc(NUM_BUFFERS_SENT, &channel_id, 1);
                this_metrics_recorder.inc(NUM_BYTES_SENT, &channel_id, size as u64);
            }
        };

        let this_runnning = self.running.clone();
        let this_in_chans = self.in_chans.clone();
        let this_buffer_queues = self.buffer_queues.clone();
        let this_metrics_recorder = self.metrics_recorder.clone();

        // reads data from in chans (acks, credit annoucments, etc.) and updates buffer queues accordingly
        let input_loop = move || {

            let mut sel = Select::new();
            let mut in_chans = HashMap::new();
            let locked_in_chans = this_in_chans.read().unwrap();
            for (socket_identity, (s, r)) in locked_in_chans.iter() {
                in_chans.insert(socket_identity.clone(), (s.clone(), r.clone()));
            }
            drop(locked_in_chans);

            let socket_identities = in_chans.keys().cloned().collect::<Vec<String>>();
            for socket_identity in &socket_identities {
                let (_, r) = in_chans.get(socket_identity).unwrap();
                sel.recv(r);
            }
            while this_runnning.load(Ordering::Relaxed) {

                let oper = sel.select_timeout(Duration::from_millis(100));
                if !oper.is_ok() {
                    continue;
                }
                let oper = oper.unwrap();
                let index = oper.index();
                let socket_identity = &socket_identities[index];
                let recv = &in_chans.get(socket_identity).unwrap().1;

                let b = oper.recv(recv).unwrap();
                let size = b.len();
                let ack = DataReaderResponseMessage::de(b);
                let channel_id = &ack.channel_id;
                this_buffer_queues.handle_ack(&ack);
                this_metrics_recorder.inc(NUM_BUFFERS_RECVD, channel_id, 1);
                this_metrics_recorder.inc(NUM_BYTES_RECVD, channel_id, size as u64);
            }
        };

        let name = &self.name;
        let in_thread_name = format!("volga_{name}_in_thread");
        let out_thread_name = format!("volga_{name}_out_thread");
        self.io_thread_handles.push(std::thread::Builder::new().name(in_thread_name).spawn(input_loop).unwrap()).unwrap();
        self.io_thread_handles.push(std::thread::Builder::new().name(out_thread_name).spawn(output_loop).unwrap()).unwrap();
    }

    fn stop (&self) {
        // TODO wait for un-acked messages, wait for in/out chans to get empty
        self.buffer_queues.close();
        self.running.store(false, Ordering::Relaxed);
        while self.io_thread_handles.len() != 0 {
            let handle = self.io_thread_handles.pop();
            handle.unwrap().join().unwrap();
        }
        self.metrics_recorder.stop();
    }
}