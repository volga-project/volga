
use std::{collections::HashMap, sync::{atomic::{AtomicBool, Ordering}, Arc, RwLock}, thread::JoinHandle};

use crossbeam::{channel::{bounded, Receiver, Sender}, queue::ArrayQueue};
use pyo3::{pyclass, pymethods};
use serde::{Deserialize, Serialize};

use super::{buffer_utils::get_channeld_id, channel::Channel, channels_router::ChannelsRouter, io_loop::{Bytes, Direction, IOHandler, IOHandlerType}, metrics::{MetricsRecorder, NUM_BUFFERS_RECVD, NUM_BUFFERS_SENT, NUM_BYTES_RECVD, NUM_BYTES_SENT}, sockets::{SocketMetadata, SocketOwner}, utils::sleep_thread};

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
        let this_channels = self.channels.clone();
        let this_metrics_recorder = self.metrics_recorder.clone();

        // we put stuff fromm all local recv chans into corresponding remote out chans
        let output_loop = move || {

            let locked_local_recv_chans = this_local_recv_chans.read().unwrap();
            let locked_remote_send_chans = this_remote_send_chans.read().unwrap();
            let locked_peers = this_peers.read().unwrap();

            let mut senders = HashMap::new();
            let mut receivers: HashMap<&String, &Receiver<Box<Vec<u8>>>> = HashMap::new();
            let mut receiver_to_sender_mapping = HashMap::new();
            for channel in this_channels.iter() {
                let peer_node_id = locked_peers.get(channel.get_channel_id()).unwrap();

                let (s, _) = locked_remote_send_chans.get(peer_node_id).unwrap();
                if !senders.contains_key(peer_node_id) {
                    senders.insert(peer_node_id, s);
                }
                let (_, r) = locked_local_recv_chans.get(channel.get_channel_id()).unwrap();
                receivers.insert(channel.get_channel_id(), r);

                receiver_to_sender_mapping.insert(channel.get_channel_id().clone(), vec![peer_node_id.clone()]);
            }

            let mut s = ChannelsRouter::new(senders, receivers, &receiver_to_sender_mapping);

            while this_runnning.load(Ordering::Relaxed) {
                let result = s.iter(false);
                if result.is_some() {
                    let res = result.unwrap();
                    let size = res.0;
                    let peer_node_id = res.1;
                    this_metrics_recorder.inc(NUM_BUFFERS_SENT, peer_node_id, 1);
                    this_metrics_recorder.inc(NUM_BYTES_SENT, peer_node_id, size as u64);
                }
            }
        };

        let this_local_send_chans = self.local_send_chans.clone();
        let this_remote_recv_chans = self.remote_recv_chans.clone();
        let this_metrics_recorder = self.metrics_recorder.clone();
        let this_runnning = self.running.clone();
        let this_channels = self.channels.clone();
        let this_peers = self.channel_id_to_node_id.clone();

        let input_loop = move || {

            let locked_local_send_chans = this_local_send_chans.read().unwrap();
            let locked_remote_recv_chans = this_remote_recv_chans.read().unwrap();
            let locked_peers = this_peers.read().unwrap();

            let mut senders = HashMap::new();
            let mut receivers: HashMap<&String, &Receiver<Box<Vec<u8>>>> = HashMap::new();
            let mut receiver_to_sender_mapping: HashMap<String, Vec<String>> = HashMap::new();
            for channel in this_channels.iter() {
                let peer_node_id = locked_peers.get(channel.get_channel_id()).unwrap();

                let (s, _) = locked_local_send_chans.get(channel.get_channel_id()).unwrap();
                senders.insert(channel.get_channel_id(), s);
                let (_, r) = locked_remote_recv_chans.get(peer_node_id).unwrap();
                if !receivers.contains_key(peer_node_id) {
                    receivers.insert(peer_node_id, r);
                }

                if receiver_to_sender_mapping.contains_key(peer_node_id) {
                    receiver_to_sender_mapping.get_mut(peer_node_id).unwrap().push(channel.get_channel_id().clone());
                } else {
                    receiver_to_sender_mapping.insert(peer_node_id.clone(), vec![channel.get_channel_id().clone()]);
                }
            }

            let mut s = ChannelsRouter::new(senders, receivers, &receiver_to_sender_mapping);

            while this_runnning.load(Ordering::Relaxed) {
                // this may cause backpressure for all local channels sharing this remote channel
                // TODO we should implement credit-based flow control to avoid this

                let result = s.iter(true);                
                if result.is_some() {
                    let res = result.unwrap();
                    let size = res.0;
                    let peer_node_id = res.1;
                    this_metrics_recorder.inc(NUM_BUFFERS_SENT, peer_node_id, 1);
                    this_metrics_recorder.inc(NUM_BYTES_SENT, peer_node_id, size as u64);
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