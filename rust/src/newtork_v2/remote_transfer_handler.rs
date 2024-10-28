
use std::{collections::{HashMap, HashSet}, fs, sync::{atomic::{AtomicBool, Ordering}, Arc, RwLock}, thread::JoinHandle, time::Duration};

use crossbeam::{channel::{bounded, Receiver, Select, Sender}, queue::ArrayQueue};
use pyo3::{pyclass, pymethods};
use serde::{Deserialize, Serialize};

use crate::network::metrics::NUM_BYTES_RECVD;

use super::{buffer_utils::{get_channeld_id, Bytes}, channel::{to_local_and_remote, Channel}, metrics::{MetricsRecorder, NUM_BUFFERS_RECVD, NUM_BUFFERS_SENT, NUM_BYTES_SENT}, io_loop::{IOHandler, CROSSBEAM_DEFAULT_CHANNEL_SIZE}, sockets::{channels_to_socket_identities, parse_ipc_path_from_addr, SocketIdentityGenerator, SocketKind, SocketMetadata}};

#[derive(Serialize, Deserialize, Clone)]
#[pyclass(name="RustTransferConfig")]
pub struct TransferConfig {
    pub transfer_queue_size: usize // TODO this is no-op
}

#[pymethods]
impl TransferConfig { 
    #[new]
    pub fn new(transfer_queue_size: usize) -> Self {
        TransferConfig{
            transfer_queue_size
        }
    }
}

pub struct RemoteTransferHandler {
    id: String,
    name: String,
    job_name: String,
    is_sender: bool,
    channels: Vec<Channel>,
    socket_metas: Vec<SocketMetadata>,

    local_in_chans: Arc<RwLock<HashMap<String, (Sender<Bytes>, Receiver<Bytes>)>>>,
    local_out_chans: Arc<RwLock<HashMap<String, (Sender<Bytes>, Receiver<Bytes>)>>>,

    remote_in_chans: Arc<RwLock<HashMap<String, (Sender<Bytes>, Receiver<Bytes>)>>>,
    remote_out_chans: Arc<RwLock<HashMap<String, (Sender<Bytes>, Receiver<Bytes>)>>>,

    node_ip: String,

    metrics_recorder: Arc<MetricsRecorder>,

    running: Arc<AtomicBool>,
    io_thread_handles: Arc<ArrayQueue<JoinHandle<()>>>, // array queue so we do not mutate DataReader and keep ownership

    config: Arc<TransferConfig>
}

impl RemoteTransferHandler {

    pub fn new(id: String, name: String, job_name: String, channels: Vec<Channel>, config: TransferConfig, is_sender: bool) -> Self {

        let (
            socket_metas, 
            local_in_chans, 
            local_out_chans, 
            remote_in_chans, 
            remote_out_chans,
            node_id, 
            node_ip
        ) = RemoteTransferHandler::configure_sockets_and_io_chans(&id, &name, &channels, is_sender);


        RemoteTransferHandler{
            id,
            name: name.clone(), 
            job_name: job_name.clone(),
            is_sender,
            channels,
            socket_metas,
            local_in_chans: Arc::new(RwLock::new(local_in_chans)),
            local_out_chans: Arc::new(RwLock::new(local_out_chans)),
            remote_in_chans: Arc::new(RwLock::new(remote_in_chans)),
            remote_out_chans: Arc::new(RwLock::new(remote_out_chans)),
            node_ip,
            metrics_recorder: Arc::new(MetricsRecorder::new(name.clone(), job_name.clone())),
            running: Arc::new(AtomicBool::new(false)),
            io_thread_handles: Arc::new(ArrayQueue::new(2)),
            config: Arc::new(config)
        }
    }

    fn configure_sockets_and_io_chans(id: &String, name: &String, channels: &Vec<Channel>, is_sender: bool) 
    -> (
        Vec<SocketMetadata>, 
        HashMap<String, (Sender<Bytes>, Receiver<Bytes>)>, // local in
        HashMap<String, (Sender<Bytes>, Receiver<Bytes>)>, // local out
        HashMap<String, (Sender<Bytes>, Receiver<Bytes>)>, // remote in
        HashMap<String, (Sender<Bytes>, Receiver<Bytes>)>, // remote out
        String, // this node_id
        String // this node_ip
    ) {
        
        let mut socket_identity_generator = SocketIdentityGenerator::new(id.clone());
        let mut socket_metas = Vec::new();
        let mut local_in_chans = HashMap::new();
        let mut local_out_chans = HashMap::new();
        let mut remote_in_chans = HashMap::new();
        let mut remote_out_chans = HashMap::new();

        let (local_channels, remote_channels) = to_local_and_remote(&channels);
        if remote_channels.len() != channels.len() {
            panic!("{name} should have only remote channels")
        }

        let mut node_id: Option<String> = None; 
        let mut node_ip: Option<String> = None;

        if is_sender {
            // one ROUTER to receive from all local inputs, one DEALER  per remote node to send, 
            let mut remote_channels_per_node_id: HashMap<String, Vec<Channel>> = HashMap::new();
            let mut ipc_addrs: HashSet<String> = HashSet::new();
            for channel in &remote_channels {
                match channel {
                    Channel::Local{..} => {panic!("only remote")},
                    Channel::Remote { channel_id, source_local_ipc_addr, source_node_ip, source_node_id, target_local_ipc_addr, target_node_ip, target_node_id, port } => {
                        if node_id.is_none() {
                            node_id = Some(source_node_id.clone());
                            node_ip = Some(source_node_ip.clone());
                        } else {
                            if &node_id.clone().unwrap() != source_node_id {
                                panic!("Duplicate source_node_id");
                            }
                            if &node_ip.clone().unwrap() != source_node_ip {
                                panic!("Duplicate source_node_ip");
                            }
                        }
                        
                        if remote_channels_per_node_id.contains_key(target_node_id) {
                            remote_channels_per_node_id.get_mut(target_node_id).unwrap().push(channel.clone());
                        } else {
                            remote_channels_per_node_id.insert(target_node_id.clone(), vec![channel.clone()]);
                        }
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
            let router_socket_meta = SocketMetadata{
                identity: socket_identity.clone(),
                owner_id: id.clone(),
                kind: SocketKind::Router,
                channel_ids: remote_channels.iter().map(|channel| channel.get_channel_id().clone()).collect(),
                addr: ipc_addr,
            };
            local_in_chans.insert(socket_identity.clone(), bounded(CROSSBEAM_DEFAULT_CHANNEL_SIZE));
            local_out_chans.insert(socket_identity.clone(), bounded(CROSSBEAM_DEFAULT_CHANNEL_SIZE));
            socket_metas.push(router_socket_meta);

            for (_, node_channels) in remote_channels_per_node_id {
                let mut ports = HashSet::new();
                let mut target_node_ips = HashSet::new();
                let mut channel_ids = vec![];
                for channel in node_channels {
                    match channel {
                        Channel::Local{..} => {panic!("only remote")},
                        Channel::Remote { channel_id, source_local_ipc_addr, source_node_ip, source_node_id, target_local_ipc_addr, target_node_ip, target_node_id, port } => {
                            ports.insert(port);
                            channel_ids.push(channel_id);   
                            target_node_ips.insert(target_node_ip);
                        }
                    }
                }

                if ports.len() != 1 {
                    panic!("Duplicate ports for {name}")
                }
                let port = ports.iter().next().unwrap();

                if target_node_ips.len() != 1 {
                    panic!("Duplicate target_node_ip for {name}")
                }
                let target_node_ip = target_node_ips.iter().next().unwrap();

                let socket_identity = socket_identity_generator.gen();
                let addr = format!("tcp://{target_node_ip}:{port}");
                let dealer_socket_meta = SocketMetadata{
                    identity: socket_identity.clone(),
                    owner_id: id.clone(),
                    kind: SocketKind::Dealer,
                    channel_ids,
                    addr,
                };    

                remote_in_chans.insert(socket_identity.clone(), bounded(CROSSBEAM_DEFAULT_CHANNEL_SIZE));
                remote_out_chans.insert(socket_identity.clone(), bounded(CROSSBEAM_DEFAULT_CHANNEL_SIZE));
                socket_metas.push(dealer_socket_meta);
            }
        } else {
            // one ROUTER to receive from all remote connection, one DEALER per remote connection to send to local reader, 
            let mut ports = HashSet::new();

            // group channels by target ipc addrs (maps to data readers on this node)
            let mut target_local_ipc_addrs: HashMap<String, Vec<String>> = HashMap::new();
            
            for channel in &remote_channels {
                match channel {
                    Channel::Local{..} => {panic!("only remote")},
                    Channel::Remote { channel_id, source_local_ipc_addr, source_node_ip, source_node_id, target_local_ipc_addr, target_node_ip, target_node_id, port } => {
                        if node_id.is_none() {
                            node_id = Some(target_node_id.clone());
                            node_ip = Some(target_node_ip.clone());
                        } else {
                            if &node_id.clone().unwrap() != target_node_id {
                                panic!("Duplicate target_node_id");
                            }
                            if &node_ip.clone().unwrap() != target_node_ip {
                                panic!("Duplicate target_node_ip");
                            }
                        }
                        
                        ports.insert(port);
                        if target_local_ipc_addrs.contains_key(target_local_ipc_addr) {
                            target_local_ipc_addrs.get_mut(target_local_ipc_addr).unwrap().push(channel_id.clone());
                        } else {
                            target_local_ipc_addrs.insert(target_local_ipc_addr.clone(), vec![channel_id.clone()]);
                        }
                    }
                }
            }

            for (target_local_ipc_addr, channel_ids) in target_local_ipc_addrs {
                let socket_identity = socket_identity_generator.gen();
                let dealer_socket_meta = SocketMetadata{
                    identity: socket_identity.clone(),
                    owner_id: id.clone(),
                    kind: SocketKind::Dealer,
                    channel_ids,
                    addr: target_local_ipc_addr.clone(),
                };
                socket_metas.push(dealer_socket_meta);
                local_in_chans.insert(socket_identity.clone(), bounded(CROSSBEAM_DEFAULT_CHANNEL_SIZE));
                local_out_chans.insert(socket_identity.clone(), bounded(CROSSBEAM_DEFAULT_CHANNEL_SIZE));
            }

            if ports.len() != 1 {
                panic!("Duplicate ports for {name}")
            }
            let port = ports.iter().next().unwrap();
            let addr = format!("tcp://0.0.0.0:{port}");

            let socket_identity = socket_identity_generator.gen();
            let router_socket_meta = SocketMetadata{
                identity: socket_identity.clone(),
                owner_id: id.clone(),
                kind: SocketKind::Router,
                channel_ids: remote_channels.iter().map(|channel| channel.get_channel_id().clone()).collect(),
                addr,
            };
            socket_metas.push(router_socket_meta);
            remote_in_chans.insert(socket_identity.clone(), bounded(CROSSBEAM_DEFAULT_CHANNEL_SIZE));
            remote_out_chans.insert(socket_identity.clone(), bounded(CROSSBEAM_DEFAULT_CHANNEL_SIZE));
        }

        (socket_metas, local_in_chans, local_out_chans, remote_in_chans, remote_out_chans, node_id.unwrap(), node_ip.unwrap())
    }
}

fn run_one_to_many_loop(
    from_chan: &(Sender<Bytes>, Receiver<Bytes>),
    to_chans: HashMap<String, (Sender<Bytes>, Receiver<Bytes>)>,
    socket_metas: Vec<SocketMetadata>,
    running: Arc<AtomicBool>,
    metrics_recorder: Arc<MetricsRecorder>,
    is_sender: bool,
    node_ip: &String
) {
    let to_socket_identities = to_chans.keys().cloned().collect::<Vec<String>>();
    let to_socket_metas = socket_metas.into_iter().filter(|sm| to_socket_identities.contains(&sm.identity)).collect();
    let channel_id_to_to_socket = channels_to_socket_identities(to_socket_metas);

    while running.load(Ordering::Relaxed) {
        let rcvd_res = from_chan.1.recv_timeout(Duration::from_millis(100));
        if !rcvd_res.is_ok() {
            continue;
        }

        let b = rcvd_res.unwrap();
        let channel_id = get_channeld_id(&b);
        let size = b.len();

        let to_socket_identity = channel_id_to_to_socket.get(&channel_id).unwrap();   
        let to_chan = to_chans.get(to_socket_identity).unwrap();
        
        // TODO this will block whole shared channel, log? We count on higher level credit-based flow control to handle backpressure,
        // so idealy this should not happen
        while running.load(Ordering::Relaxed) {
            let res = to_chan.0.send_timeout(b.clone(), Duration::from_millis(100));
            if res.is_ok() {
                break;
            }
        }
        metrics_recorder.inc(if is_sender {NUM_BUFFERS_SENT} else {NUM_BUFFERS_RECVD}, node_ip, 1);
        metrics_recorder.inc(if is_sender {NUM_BYTES_SENT} else {NUM_BYTES_RECVD}, node_ip, size as u64);
    }
}

fn run_many_to_one_loop(
    from_chans: HashMap<String, (Sender<Bytes>, Receiver<Bytes>)>,
    to_chan: &(Sender<Bytes>, Receiver<Bytes>),
    running: Arc<AtomicBool>,
    metrics_recorder: Arc<MetricsRecorder>,
    is_sender: bool,
    node_ip: &String
) {
    let mut sel = Select::new();
    let mut from_receivers = Vec::new();

    for (_, (_, r)) in &from_chans {
        from_receivers.push(r);
        sel.recv(r);
    }

    while running.load(Ordering::Relaxed) {
        let sel_res = sel.select_timeout(Duration::from_millis(100));
        if !sel_res.is_ok() {
            continue;
        }

        let oper = sel_res.unwrap();
        let index = oper.index();

        let b = oper.recv(&from_receivers[index]).unwrap();
        let size = b.len();

        while running.load(Ordering::Relaxed) {
            let res = to_chan.0.send_timeout(b.clone(), Duration::from_millis(100));
            if res.is_ok() {
                break;
            }
        }
        metrics_recorder.inc(if is_sender {NUM_BUFFERS_SENT} else {NUM_BUFFERS_RECVD}, node_ip, 1);
        metrics_recorder.inc(if is_sender {NUM_BYTES_SENT} else {NUM_BYTES_RECVD}, node_ip, size as u64);
    }
}

impl IOHandler for RemoteTransferHandler {

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
        let local_in_chans = self.local_in_chans.read().unwrap();
        if local_in_chans.contains_key(&sm.identity) {
            return local_in_chans.get(&sm.identity).unwrap().0.clone();
        }
        let remote_in_chans = self.remote_in_chans.read().unwrap();
        if remote_in_chans.contains_key(&sm.identity) {
            return remote_in_chans.get(&sm.identity).unwrap().0.clone();
        }

        panic!("Can not find in chan")
    }

    fn get_out_receiver(&self, sm: &SocketMetadata) -> Receiver<Bytes> {
        let local_out_chans = self.local_out_chans.read().unwrap();
        if local_out_chans.contains_key(&sm.identity) {
            return local_out_chans.get(&sm.identity).unwrap().1.clone();
        }
        let remote_out_chans = self.remote_out_chans.read().unwrap();
        if remote_out_chans.contains_key(&sm.identity) {
            return remote_out_chans.get(&sm.identity).unwrap().1.clone();
        }

        panic!("Can not find out chan")
    }

    fn start(&self) {

        self.running.store(true, Ordering::Relaxed);
        self.metrics_recorder.start();
        
        let this_local_in_chans = self.local_in_chans.clone();
        let this_local_out_chans = self.local_out_chans.clone();
        let this_remote_in_chans = self.remote_in_chans.clone();
        let this_remote_out_chans = self.remote_out_chans.clone();
        let this_running = self.running.clone();
        let this_socket_metas = self.socket_metas.clone();

        let this_metrics_recorder = self.metrics_recorder.clone();
        let is_sender = self.is_sender.clone();
        let this_node_ip = self.node_ip.clone();

        let forward_loop = move || {

            let local_in_chans = this_local_in_chans.read().unwrap().to_owned();
            let local_out_chans = this_local_out_chans.read().unwrap().to_owned();
            let remote_in_chans = this_remote_in_chans.read().unwrap().to_owned();
            let remote_out_chans = this_remote_out_chans.read().unwrap().to_owned();

            let from_chan;
            let to_chans;
            if is_sender {
                // one local in chan -> many remote out chans    
                if local_in_chans.len() != 1 {
                    panic!("Misconfigured local_in_chans");
                }    
                from_chan = local_in_chans.values().next().unwrap(); 
                to_chans = remote_out_chans;      
            } else {
                // one remote in chan -> many local out chans
                if remote_in_chans.len() != 1 {
                    panic!("Misconfigured remote_in_chans");
                }    
                from_chan = remote_in_chans.values().next().unwrap(); 
                to_chans = local_out_chans; 
            }
            run_one_to_many_loop(
                from_chan,
                to_chans,
                this_socket_metas,
                this_running,
                this_metrics_recorder,
                is_sender,
                &this_node_ip
            );
        };

        let this_local_in_chans = self.local_in_chans.clone();
        let this_local_out_chans = self.local_out_chans.clone();
        let this_remote_in_chans = self.remote_in_chans.clone();
        let this_remote_out_chans = self.remote_out_chans.clone();
        let this_running = self.running.clone();
        let this_metrics_recorder = self.metrics_recorder.clone();
        let is_sender = self.is_sender.clone();
        let this_node_ip = self.node_ip.clone();

        let backward_loop = move || {

            let local_in_chans = this_local_in_chans.read().unwrap().to_owned();
            let local_out_chans = this_local_out_chans.read().unwrap().to_owned();
            let remote_in_chans = this_remote_in_chans.read().unwrap().to_owned();
            let remote_out_chans = this_remote_out_chans.read().unwrap().to_owned();
            
            let from_chans;
            let to_chan;

            if is_sender {
                // many remote in chans -> one local out chan
                from_chans = remote_in_chans;
                if local_out_chans.len() != 1 {
                    panic!("Misconfigured local_out_chans");
                }
                to_chan = local_out_chans.values().next().unwrap();
            } else {
                // many local in chans -> one remote out chan
                from_chans = local_in_chans;
                if remote_out_chans.len() != 1 {
                    panic!("Misconfigured remote_out_chans");
                }
                to_chan = remote_out_chans.values().next().unwrap();
            }
            run_many_to_one_loop(
                from_chans,
                to_chan,
                this_running,
                this_metrics_recorder,
                is_sender,
                &this_node_ip
            );
        };

        let name = &self.name;
        let forward_thread_name = format!("volga_{name}_forward_thread");
        let backward_thread_name = format!("volga_{name}_backward_thread");
        self.io_thread_handles.push(std::thread::Builder::new().name(forward_thread_name).spawn(forward_loop).unwrap()).unwrap();
        self.io_thread_handles.push(std::thread::Builder::new().name(backward_thread_name).spawn(backward_loop).unwrap()).unwrap();
    }
    

    fn stop(&self) {
        // TODO wait for all chans to get empty
        self.running.store(false, Ordering::Relaxed);
        while self.io_thread_handles.len() != 0 {
            let handle = self.io_thread_handles.pop();
            handle.unwrap().join().unwrap();
        }
        self.metrics_recorder.stop();
    }
}