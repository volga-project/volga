use std::{collections::HashMap, hash::Hash, sync::{atomic::{AtomicBool, Ordering}, Arc, RwLock}, thread::JoinHandle};

use crossbeam::{channel::{unbounded, bounded, Receiver, Sender}, queue::ArrayQueue};
use pyo3::{pyclass, pymethods};
use serde::{Deserialize, Serialize};

use super::{buffer_utils::{get_buffer_id, get_channeld_id}, channel::{self, Channel}, io_loop::{Bytes, Direction, IOHandler, IOHandlerType}, metrics::{MetricsRecorder, NUM_BUFFERS_RECVD, NUM_BUFFERS_SENT, NUM_BYTES_RECVD, NUM_BYTES_SENT}, sockets::{SocketMetadata, SocketOwner}};

// const TRANSFER_QUEUE_SIZE: usize = 10; // TODO should we separate local and remote channel sizes?

#[derive(Serialize, Deserialize, Clone)]
#[pyclass(name="RustTransferConfig")]
pub struct TransferConfig {
    transfer_queue_size: usize
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
    name: String,
    job_name: String,
    channels: Vec<Channel>,
    direction: Direction,

    local_send_chans: Arc<RwLock<HashMap<String, (Sender<Box<Bytes>>, Receiver<Box<Bytes>>)>>>,
    local_recv_chans: Arc<RwLock<HashMap<String, (Sender<Box<Bytes>>, Receiver<Box<Bytes>>)>>>,

    remote_send_chans: Arc<RwLock<HashMap<String, (Sender<Box<Bytes>>, Receiver<Box<Bytes>>)>>>,
    remote_recv_chans: Arc<RwLock<HashMap<String, (Sender<Box<Bytes>>, Receiver<Box<Bytes>>)>>>,

    channel_id_to_node_id: Arc<RwLock<HashMap<String, String>>>,

    metrics_recorder: Arc<MetricsRecorder>,

    running: Arc<AtomicBool>,
    io_thread_handles: Arc<ArrayQueue<JoinHandle<()>>>, // array queue so we do not mutate DataReader and keep ownership

    config: Arc<TransferConfig>
}

impl RemoteTransferHandler {

    pub fn new(name: String, job_name: String, channels: Vec<Channel>, config: TransferConfig, direction: Direction) -> Self {

        let is_sender = direction == Direction::Sender;

        let mut channel_id_to_node_id = HashMap::new();
        let n_channels = channels.len();
        let mut local_send_chans = HashMap::with_capacity(n_channels);
        let mut local_recv_chans = HashMap::with_capacity(n_channels);

        let mut remote_send_chans = HashMap::new();
        let mut remote_recv_chans = HashMap::new();

        for channel in &channels {
            match channel {
                Channel::Local{..} => {panic!("RemoteTransferHandler does not use Local Channels")}
                Channel::Remote {
                    channel_id, 
                    target_node_id, 
                    source_node_id, 
                    ..
                } => {
                    let peer_node_id =  if is_sender {target_node_id} else {source_node_id};
                    channel_id_to_node_id.insert(channel_id.clone(), peer_node_id.clone());
                    
                    local_send_chans.insert(channel_id.clone(), bounded(config.transfer_queue_size));
                    local_recv_chans.insert(channel_id.clone(), bounded(config.transfer_queue_size));
                    if !remote_send_chans.contains_key(peer_node_id) {
                        remote_send_chans.insert(peer_node_id.clone(), bounded(config.transfer_queue_size));
                    }
                    if !remote_recv_chans.contains_key(peer_node_id) {
                        remote_recv_chans.insert(peer_node_id.clone(), bounded(config.transfer_queue_size));
                    }
                }
            }
        }

        RemoteTransferHandler{
            name: name.clone(), 
            job_name: job_name.clone(),
            channels,
            direction,
            local_send_chans: Arc::new(RwLock::new(local_send_chans)),
            local_recv_chans: Arc::new(RwLock::new(local_recv_chans)),
            remote_send_chans: Arc::new(RwLock::new(remote_send_chans)),
            remote_recv_chans: Arc::new(RwLock::new(remote_recv_chans)),
            channel_id_to_node_id: Arc::new(RwLock::new(channel_id_to_node_id)),
            metrics_recorder: Arc::new(MetricsRecorder::new(name.clone(), job_name.clone())),
            running: Arc::new(AtomicBool::new(false)),
            io_thread_handles: Arc::new(ArrayQueue::new(2)),
            config: Arc::new(config)
        }
    }
}

impl IOHandler for RemoteTransferHandler {

    fn get_name(&self) -> String {
        self.name.clone()
    }

    fn get_handler_type(&self) -> IOHandlerType {
        if self.direction == Direction::Sender {
            IOHandlerType::TransferSender
        } else {
            IOHandlerType::TransferReceiver
        }
    }

    fn get_channels(&self) -> &Vec<Channel> {
        &self.channels
    }

    fn get_send_chan(&self, sm: &SocketMetadata) -> (Sender<Box<Bytes>>, Receiver<Box<Bytes>>) {
        if sm.owner == SocketOwner::TransferLocal {
            let l = &self.local_send_chans.read().unwrap();
            let v = l.get(&sm.channel_id).unwrap();
            v.clone()
        } else if sm.owner == SocketOwner::TransferRemote {
            let hm = &self.remote_send_chans.read().unwrap();
            let peers = &self.channel_id_to_node_id.read().unwrap();
            let peer_node_id = peers.get(&sm.channel_id).unwrap();
            let v = hm.get(peer_node_id).unwrap();
            v.clone()
        } else {
            panic!("RemoteTransferHandler only deals with remote socket owners");
        }
    }

    fn get_recv_chan(&self, sm: &SocketMetadata) -> (Sender<Box<Bytes>>, Receiver<Box<Bytes>>) {
        if sm.owner == SocketOwner::TransferLocal {
            let l = &self.local_recv_chans.read().unwrap();
            let v = l.get(&sm.channel_id).unwrap();
            v.clone()
        } else if sm.owner == SocketOwner::TransferRemote {
            let hm = &self.remote_recv_chans.read().unwrap();
            let peers = &self.channel_id_to_node_id.read().unwrap();
            let peer_node_id = peers.get(&sm.channel_id).unwrap();
            let v = hm.get(peer_node_id).unwrap();
            v.clone()
        } else {
            panic!("RemoteTransferHandler only deals with remote socket owners");
        }
    }

    fn start(&self) {

        self.running.store(true, Ordering::Relaxed);
        self.metrics_recorder.start();
        
        let this_local_recv_chans = self.local_recv_chans.clone();
        let this_remote_send_chans = self.remote_send_chans.clone();
        let this_runnning = self.running.clone();
        let this_peers = self.channel_id_to_node_id.clone();
        let this_metrics_recorder = self.metrics_recorder.clone();

        // we put stuff fromm all local recv chans into corresponding remote out chans
        let output_loop = move || {

            while this_runnning.load(Ordering::Relaxed) {

                let locked_local_recv_chans = this_local_recv_chans.read().unwrap();
                let locked_remote_send_chans = this_remote_send_chans.read().unwrap();
                let locked_peers = this_peers.read().unwrap();

                for channel_id in locked_local_recv_chans.keys() {
                    let peer_node_id = locked_peers.get(channel_id).unwrap();
                    let send_chan = locked_remote_send_chans.get(peer_node_id).unwrap();
                    let sender = send_chan.0.clone();
                    let recv_chan = locked_local_recv_chans.get(channel_id).unwrap();
                    let receiver = recv_chan.1.clone();
                    if !sender.is_full() & !receiver.is_empty() {
                        let b = receiver.recv().unwrap();
                        let size = b.len();
                        this_metrics_recorder.inc(NUM_BUFFERS_SENT, peer_node_id, 1);
                        this_metrics_recorder.inc(NUM_BYTES_SENT, peer_node_id, size as u64);
                        sender.send(b).unwrap();
                    }
                }
            }
        };

        let this_local_send_chans = self.local_send_chans.clone();
        let this_remote_recv_chans = self.remote_recv_chans.clone();
        let this_metrics_recorder = self.metrics_recorder.clone();
        let this_runnning = self.running.clone();

        let input_loop = move || {

            while this_runnning.load(Ordering::Relaxed) {

                let locked_local_send_chans = this_local_send_chans.read().unwrap();
                let locked_remote_recv_chans = this_remote_recv_chans.read().unwrap();

                for peer_node_id in locked_remote_recv_chans.keys() {
                    let recv_chan = locked_remote_recv_chans.get(peer_node_id).unwrap();
                    let receiver = recv_chan.1.clone();
                    if !receiver.is_empty() {
                        let b = receiver.recv().unwrap();
                        let size = b.len();
                        let channel_id = get_channeld_id(b.clone());
                        let send_chan = locked_local_send_chans.get(&channel_id).unwrap();
                        let sender = send_chan.0.clone();

                        // this will cause backpressure for all local channels sharing this remote channel
                        // TODO we should implement credit-based flow control to avoid this
                        sender.send(b).unwrap();
                        this_metrics_recorder.inc(NUM_BUFFERS_RECVD, peer_node_id, 1);
                        this_metrics_recorder.inc(NUM_BYTES_RECVD, peer_node_id, size as u64);
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

    fn close(&self) {
        self.running.store(false, Ordering::Relaxed);
        while self.io_thread_handles.len() != 0 {
            let handle = self.io_thread_handles.pop();
            handle.unwrap().join().unwrap();
        }
        self.metrics_recorder.close();
    }
}