
use std::{collections::{HashMap, HashSet}, fs, sync::{atomic::{AtomicBool, Ordering}, Arc, RwLock}, thread::{self, JoinHandle}, time::{Duration, SystemTime}};

use super::{buffer_queues::BufferQueues, buffer_utils::Bytes, channel::{to_local_and_remote, Channel, DataReaderResponseMessage}, metrics::{MetricsRecorder, NUM_BUFFERS_RECVD, NUM_BUFFERS_RESENT, NUM_BUFFERS_SENT, NUM_BYTES_RECVD, NUM_BYTES_SENT}, socket_service::{SocketMessage, SocketServiceSubscriber, SocketServiceSubscriberType, CROSSBEAM_DEFAULT_CHANNEL_SIZE}, sockets::{parse_ipc_path_from_addr, SocketIdentityGenerator, SocketKind, SocketMetadata}};
use crossbeam::{channel::{bounded, Receiver, Sender}, queue::ArrayQueue};
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
    in_chan: Arc<(Sender<SocketMessage>, Receiver<SocketMessage>)>,
    out_chan: Arc<(Sender<SocketMessage>, Receiver<SocketMessage>)>,
    // in_chans: Arc<RwLock<HashMap<String, (Sender<SocketMessage>, Receiver<SocketMessage>)>>>,
    // out_chans: Arc<RwLock<HashMap<String, (Sender<SocketMessage>, Receiver<SocketMessage>)>>>,
    buffer_queues: Arc<BufferQueues>,

    metrics_recorder: Arc<MetricsRecorder>,

    running: Arc<AtomicBool>,
    io_thread_handles: Arc<ArrayQueue<JoinHandle<()>>>, // array queue so we do not mutate DataWriter and keep ownership

    // config options
    config: Arc<DataWriterConfig>
}

impl DataWriter {

    pub fn new(id: String, name: String, job_name: String, config: DataWriterConfig, channels: Vec<Channel>) -> DataWriter {
        let socket_metas = DataWriter::configure_sockets(&id, &name, &channels);

        let bqs = BufferQueues::new(&channels, config.max_buffers_per_channel, config.in_flight_timeout_s);

        DataWriter{
            id: id.clone(),
            name: name.clone(),
            job_name: job_name.clone(),
            channels: Arc::new(channels.to_vec()),
            socket_metas,
            in_chan: Arc::new(bounded(CROSSBEAM_DEFAULT_CHANNEL_SIZE)),
            out_chan: Arc::new(bounded(CROSSBEAM_DEFAULT_CHANNEL_SIZE)),
            // in_chans: Arc::new(RwLock::new(in_chans)),
            // out_chans: Arc::new(RwLock::new(out_chans)),
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

    fn configure_sockets(id: &String, name: &String, channels: &Vec<Channel>) -> Vec<SocketMetadata> {
        let mut socket_identity_generator = SocketIdentityGenerator::new(id.clone());
        let mut socket_metas = Vec::new();

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
        }
        socket_metas
    }

    
}

impl SocketServiceSubscriber for DataWriter {

    fn get_id(&self) -> String {
        self.id.clone()
    }

    fn get_name(&self) -> String {
        self.name.clone()
    }

    fn get_type(&self) -> SocketServiceSubscriberType {
        SocketServiceSubscriberType::DataWriter
    }

    fn get_channels(&self) -> &Vec<Channel> {
        &self.channels
    }

    fn get_sockets_metas(&self) -> &Vec<SocketMetadata> {
        &self.socket_metas
    }

    fn get_in_chan(&self) -> &(Sender<SocketMessage>, Receiver<SocketMessage>) {
        &self.in_chan
    }

    fn get_out_chan(&self) -> &(Sender<SocketMessage>, Receiver<SocketMessage>) {
        &self.out_chan
    }

    fn start(&self) {
        // start io threads to send buffers and receive acks
        self.running.store(true, Ordering::Relaxed);
        self.metrics_recorder.start();
        self.buffer_queues.start();
        
        // let this_channels = self.channels.clone();
        let this_out_chan = self.out_chan.clone();
        let buffer_queues_chan = self.buffer_queues.get_out_chan().clone();
        let this_running = self.running.clone();
        let this_metrics_recorder = self.metrics_recorder.clone();

        // move data from buffer queues to output channel
        let output_loop = move || {

            // let locked_out_chans = this_send_chans.read().unwrap();

            // let mut senders = HashMap::new();
            // let mut receivers: HashMap<&String, &Receiver<Box<Vec<u8>>>> = HashMap::new();
            // let mut receiver_to_sender_mapping = HashMap::new();
            // for channel in this_channels.iter() {
            //     let (s, _) = locked_send_chans.get(channel.get_channel_id()).unwrap();
            //     let (_, r) = buffer_queues_chans.get(channel.get_channel_id()).unwrap();
            //     senders.insert(channel.get_channel_id(), s);
            //     receivers.insert(channel.get_channel_id(), r);
            //     receiver_to_sender_mapping.insert(channel.get_channel_id().clone(), vec![channel.get_channel_id().clone()]);
            // }

            // let mut s = ChannelsRouter::new(senders, receivers, &receiver_to_sender_mapping);
            
            while this_running.load(Ordering::Relaxed) {
                // let result = s.iter(false);

                // if result.is_some() {
                //     let res = result.unwrap();
                //     let size = res.0;
                //     let channel_id = res.1;

                //     this_metrics_recorder.inc(NUM_BUFFERS_SENT, &channel_id, 1);
                //     this_metrics_recorder.inc(NUM_BYTES_SENT, &channel_id, size as u64);
                // }
            }
        };

        let this_channels = self.channels.clone();
        let this_runnning = self.running.clone();
        // let this_recv_chans = self.recv_chans.clone();
        let this_buffer_queues = self.buffer_queues.clone();
        let this_metrics_recorder = self.metrics_recorder.clone();
        let input_loop = move || {

            // let mut sel = Select::new();
            // let locked_recv_chans = this_recv_chans.read().unwrap();
            // for channel in this_channels.iter() {
            //     let (_, r) = locked_recv_chans.get(channel.get_channel_id()).unwrap();
            //     sel.recv(r);
            // }

            while this_runnning.load(Ordering::Relaxed) {

                // let oper = sel.select_timeout(time::Duration::from_millis(100));
                // if !oper.is_ok() {
                //     continue;
                // }
                // let oper = oper.unwrap();
                // let index = oper.index();
                // let channel_id = this_channels[index].get_channel_id();
                // let recv = &locked_recv_chans.get(channel_id).unwrap().1;
                // let b = oper.recv(recv).unwrap();
                // let size = b.len();
                // let ack = DataReaderResponseMessage::de(b);
                // this_buffer_queues.handle_ack(&ack);
                // this_metrics_recorder.inc(NUM_BUFFERS_RECVD, &channel_id, 1);
                // this_metrics_recorder.inc(NUM_BYTES_RECVD, &channel_id, size as u64);
            }
        };

        let name = &self.name;
        let in_thread_name = format!("volga_{name}_in_thread");
        let out_thread_name = format!("volga_{name}_out_thread");
        self.io_thread_handles.push(std::thread::Builder::new().name(in_thread_name).spawn(input_loop).unwrap()).unwrap();
        self.io_thread_handles.push(std::thread::Builder::new().name(out_thread_name).spawn(output_loop).unwrap()).unwrap();
    }

    fn stop (&self) {
        self.buffer_queues.close();
        self.running.store(false, Ordering::Relaxed);
        while self.io_thread_handles.len() != 0 {
            let handle = self.io_thread_handles.pop();
            handle.unwrap().join().unwrap();
        }
        self.metrics_recorder.close();
    }
}