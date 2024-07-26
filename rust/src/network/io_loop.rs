use std::{cmp::min, collections::{HashMap, VecDeque}, fs, sync::{atomic::{AtomicBool, Ordering}, Arc, Mutex, RwLock}, thread::JoinHandle};

use super::{channel::{Channel, ChannelMessage}, socket_meta::{SocketKind, SocketMetadata, SocketOwner}};

use super::socket_meta::ipc_path_from_addr;

pub type Bytes = Vec<u8>;

#[derive(PartialEq, Eq)]
enum Direction {
    Sender,
    Receiver
}

#[derive(PartialEq, Eq)]
pub enum IOHandlerType {
    DataReader,
    DataWriter,
    TransferSender,
    TransferReceiver
}

pub trait IOHandler {

    fn get_handler_type(&self) -> IOHandlerType;

    fn on_send_ready(&self, channel_id: &String) -> bool;

    fn on_recv_ready(&self, channel_id: &String) -> bool;

    fn get_channels(&self) -> &Vec<Channel>;

    fn get_send_buffer_queue(&self, key: &String) -> Arc<Mutex<VecDeque<Box<Bytes>>>>;

    fn get_recv_buffer_queue(&self, key: &String) -> Arc<Mutex<VecDeque<Box<Bytes>>>>;
}

struct SocketManager {
    sockets_and_metas: Vec<(zmq::Socket, SocketMetadata)>
}

impl SocketManager {

    fn new() -> SocketManager {
        SocketManager{sockets_and_metas: Vec::new()}
    }

    fn create_sockets(&mut self, zmq_context: &zmq::Context, socket_metas: &Vec<SocketMetadata>) {
        for sm in socket_metas {
            let socket = zmq_context.socket(zmq::PAIR).unwrap();
            self.sockets_and_metas.push((socket, sm.clone()));
        }
    }

    fn bind_and_connect(&mut self) {
        for (socket, sm) in &self.sockets_and_metas {
            if sm.kind == SocketKind::Bind {
                socket.bind(&sm.addr).unwrap();
            } else {
                socket.connect(&sm.addr).unwrap();
            }
        }
    }

    fn close_sockets(&mut self) {
        for (socket, _) in &self.sockets_and_metas {
            // TODO unbind/disconnect?
        }
    }
}

fn create_local_sockets_meta(channels: &Vec<Channel>, direction: Direction) -> Vec<SocketMetadata> {
    let mut v: Vec<SocketMetadata> = Vec::new();
    let is_reader = direction == Direction::Receiver;
    for channel in channels {
        match channel {
            Channel::Local{channel_id, ipc_addr} => {
                let ipc_path = ipc_path_from_addr(ipc_addr);
                fs::create_dir_all(ipc_path).unwrap();
                let socket_meta = SocketMetadata{
                    owner: SocketOwner::Client,
                    kind: if is_reader {SocketKind::Connect} else {SocketKind::Bind},
                    channel_id: channel_id.clone(),
                    addr: ipc_addr.clone(),
                };
                v.push(socket_meta);
            }
            Channel::Remote {..} => {
                panic!("Remote Not supported")
            }
        }
    }
    v
}

pub struct IOLoop {
    handlers: Arc<Mutex<Vec<Arc<dyn IOHandler + Send + Sync>>>>,
    running: Arc<AtomicBool>,
    zmq_context: Arc<zmq::Context>,
    io_threads: Vec<JoinHandle<()>>,
    socket_meta_to_handler: Arc<RwLock<HashMap<SocketMetadata, Arc<dyn IOHandler + Send + Sync>>>>
}

impl IOLoop {

    pub fn new() -> IOLoop {
        let io_loop = IOLoop{
            handlers: Arc::new(Mutex::new(Vec::new())),
            running: Arc::new(AtomicBool::new(false)), 
            zmq_context: Arc::new(zmq::Context::new()),
            io_threads: Vec::new(),
            socket_meta_to_handler: Arc::new(RwLock::new(HashMap::new()))
        };
        io_loop
    }

    pub fn register_handler(&mut self, handler: Arc<dyn IOHandler + Send + Sync>) {
        self.handlers.lock().unwrap().push(handler);
    }

    pub fn start_io_threads(&mut self, num_threads: i16) {
        // since zmq::Sockets are not thread safe we will have a model where each socket can be polled by only 1 IO thread
        // each IO thread can have multiple sockets associated with it
        // let mut num_sockets = 0;
        let mut sockets_metadata = Vec::new();
        // let this_handlers = self.handlers.clone();
        let locked_handlers = self.handlers.lock().unwrap();
        for handler in locked_handlers.iter() {
            let handler_type = handler.get_handler_type();
            let channels = handler.get_channels();
            let dir;
            if (handler_type == IOHandlerType::DataWriter) | (handler_type == IOHandlerType::TransferSender) {
                dir = Direction::Sender;
            } else {
                dir = Direction::Receiver;
            }

            if (handler_type == IOHandlerType::DataWriter) | (handler_type == IOHandlerType::DataReader) {
                let sockets_meta = create_local_sockets_meta(channels, dir);
                // sockets_metadata.append(&mut sockets_meta);
                for sm in sockets_meta {
                    sockets_metadata.push(sm.clone());
                    self.socket_meta_to_handler.write().unwrap().insert(sm.clone(), handler.clone());
                }
            } else {
                panic!("Transfer Handlers are not implemented yet");
            }
        }
        drop(locked_handlers);

        assert_eq!(self.socket_meta_to_handler.read().unwrap().len(), sockets_metadata.len());

        let num_threads = min(num_threads, sockets_metadata.len() as i16);
        let mut cur_thread_id = 0;
        let mut sockets_meta_per_thread = HashMap::new();

        // round-robin distribution
        for sm in sockets_metadata {
            cur_thread_id = cur_thread_id%num_threads;
            if !sockets_meta_per_thread.contains_key(&cur_thread_id) {
                sockets_meta_per_thread.insert(cur_thread_id, vec![sm]);
            } else {
                sockets_meta_per_thread.get_mut(&cur_thread_id).unwrap().push(sm);
            }
        }

        self.running.store(true, Ordering::Relaxed);

        for (thread_id, sms) in sockets_meta_per_thread.iter() {

            let this_runnning = self.running.clone();
            let this_zmqctx = self.zmq_context.clone();
            let this_meta_to_handlers = self.socket_meta_to_handler.clone();
            let new_sms = sms.to_vec();

            let f = move |metas: &Vec<SocketMetadata>| {
                let mut socket_manager = SocketManager::new();
                socket_manager.create_sockets(&this_zmqctx, metas);
                socket_manager.bind_and_connect();
                let mut handlers = Vec::new();
                let locked_meta_to_handlers = this_meta_to_handlers.read().unwrap();
                for i in 0..socket_manager.sockets_and_metas.len() {
                    let sm = socket_manager.sockets_and_metas[i].1.clone(); 
                    let handler = locked_meta_to_handlers.get(&sm).unwrap();
                    handlers.push(handler);
                }

                // run loop
                loop  {
                    let running = this_runnning.load(Ordering::Relaxed);
                    if !running {
                        break;
                    }

                    let mut poll_list = Vec::new();
                    for i in 0..socket_manager.sockets_and_metas.len() {
                        let socket = &socket_manager.sockets_and_metas[i].0;
                        poll_list.push(socket.as_poll_item(zmq::POLLIN|zmq::POLLOUT));
                    }

                    zmq::poll(&mut poll_list, 1).unwrap();

                    for i in 0..poll_list.len() {
                        let channel_id = &socket_manager.sockets_and_metas[i].1.channel_id;
                        let handler = handlers[i];
                        let socket = &socket_manager.sockets_and_metas[i].0;
                        if poll_list[i].is_readable() {
                            let ready = handler.on_recv_ready(channel_id);
                            if ready {
                                // this goes on heap
                                let bytes = socket.recv_bytes(zmq::DONTWAIT).unwrap();

                                // TODO for transfer handlers we should use peer ndoe id as key
                                let recv_queue = handler.get_recv_buffer_queue(channel_id);
                                let mut l = recv_queue.lock().unwrap();
                                l.push_back(Box::new(bytes));
                                drop(l); // release lock
                            }
                        }

                        if poll_list[i].is_writable() {
                            let ready = handler.on_send_ready(channel_id);
                            if ready {
                                // TODO for transfer handlers we should use peer ndoe id as key
                                let send_queue = handler.get_send_buffer_queue(channel_id);
                                
                                let mut l = send_queue.lock().unwrap();
                                let bytes = l.pop_front();
                                drop(l); // release lock
                                if !bytes.is_none() {
                                    socket.send(bytes.unwrap().as_ref(), zmq::DONTWAIT).unwrap();
                                }
                            }
                        }
                    }
                }
            };
            let thread_name = format!("volga_io_thread_{thread_id}");
            self.io_threads.push(
                std::thread::Builder::new().name(thread_name).spawn(
                    move || {f(&new_sms)}
                ).unwrap()
            );
        }
    }

    pub fn close(&self) {
        self.running.store(false, Ordering::Relaxed);
    }
}
