use core::time;
use std::{cmp::min, collections::HashMap, sync::{atomic::{AtomicBool, Ordering}, Arc, Mutex, RwLock}, thread::{self, sleep, JoinHandle}};

use crossbeam::{channel::{Sender, Receiver}, queue::SegQueue};
use pyo3::{pyclass, pymethods};
use serde::{Deserialize, Serialize};

use super::{channel::Channel, sockets::{SocketMetadata, SocketsManager, SocketsMeatadataManager}, sockets_monitor::SocketsMonitor};

pub type Bytes = Vec<u8>;


#[derive(Serialize, Deserialize, Clone)]
#[pyclass(name="RustZmqConfig")]
pub struct ZmqConfig {
    pub sndhwm: i32,
    pub rcvhwm: i32,
    pub sndbuf: i32,
    pub rcvbuf: i32,
    pub linger: i32
}

#[pymethods]
impl ZmqConfig { 
    #[new]
    pub fn new(sndhwm: i32, rcvhwm: i32, sndbuf: i32, rcvbuf: i32, linger: i32) -> Self {
        ZmqConfig{sndhwm, rcvhwm, sndbuf, rcvbuf, linger}
    }
}

#[derive(PartialEq, Eq)]
pub enum Direction {
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

    fn get_name(&self) -> String;

    fn get_handler_type(&self) -> IOHandlerType;

    fn get_channels(&self) -> &Vec<Channel>;

    fn get_send_chan(&self, sm: &SocketMetadata) -> (Sender<Box<Bytes>>, Receiver<Box<Bytes>>);

    fn get_recv_chan(&self, sm: &SocketMetadata) -> (Sender<Box<Bytes>>, Receiver<Box<Bytes>>);

    fn start(&self);

    fn close(&self);
}

pub struct IOLoop {
    name: String,
    handlers: Arc<Mutex<Vec<Arc<dyn IOHandler + Send + Sync>>>>,
    running: Arc<AtomicBool>,
    zmq_context: Arc<zmq::Context>,
    io_threads: Arc<SegQueue<JoinHandle<()>>>,
    sockets_metadata_manager: Arc<SocketsMeatadataManager>,
    zmq_config: ZmqConfig,
    sockets_monitor: Arc<SocketsMonitor>,
}

impl IOLoop {

    pub fn new(name: String, zmq_config: ZmqConfig) -> IOLoop {
        let zmq_ctx = Arc::new(zmq::Context::new());
        IOLoop{
            name,
            handlers: Arc::new(Mutex::new(Vec::new())),
            running: Arc::new(AtomicBool::new(false)), 
            zmq_context: zmq_ctx.clone(),
            io_threads: Arc::new(SegQueue::new()),
            sockets_metadata_manager: Arc::new(SocketsMeatadataManager::new()),
            zmq_config: zmq_config,

            sockets_monitor: Arc::new(SocketsMonitor::new(zmq_ctx.clone())),
        }
    }

    pub fn register_handler(&self, handler: Arc<dyn IOHandler + Send + Sync>) {
        self.handlers.lock().unwrap().push(handler);
    }

    pub fn start_io_threads(&self, num_threads: usize) -> Option<String> {
        self.sockets_monitor.start(num_threads);
        
        // since zmq::Sockets are not thread safe we will have a model where each socket can be polled by only 1 IO thread
        // each IO thread can have multiple sockets associated with it
        let name = self.name.clone();
        println!("Started loop {name}");
        let locked_handlers = self.handlers.lock().unwrap();

        if locked_handlers.len() == 0 {
            panic!("{name} loop started with no registered handlers");
        }

        let sockets_metadata = self.sockets_metadata_manager.create_for_handlers(&locked_handlers);

        let num_threads = min(num_threads, sockets_metadata.len());
        let mut cur_thread_id = 0;
        let mut sockets_meta_per_thread: HashMap<usize, Vec<SocketMetadata>> = HashMap::new();

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
            let this_thread_id = thread_id.clone();
            let this_sockets_monitor = self.sockets_monitor.clone();
            let this_running = self.running.clone();
            let this_zmqctx = self.zmq_context.clone();
            let this_socket_metadata_manager = self.sockets_metadata_manager.clone();
            
            let new_sms = sms.to_vec();
            let this_zmq_config = self.zmq_config.clone();

            let f = move |metas: &Vec<SocketMetadata>| {
                let mut sockets_manager = SocketsManager::new();
                sockets_manager.create_sockets(&this_zmqctx, metas, this_zmq_config);
                this_sockets_monitor.register_sockets(this_thread_id, sockets_manager.get_sockets_and_metas());
                this_sockets_monitor.wait_for_monitor_ready();
                thread::sleep(time::Duration::from_millis(1000));
                sockets_manager.bind_and_connect();
                let err = this_sockets_monitor.wait_for_all_connected();
                if err.is_some() {
                    return
                }

                let mut handlers = Vec::new();
                for i in 0..sockets_manager.get_sockets_and_metas().len() {
                    let sm = sockets_manager.get_sockets_and_metas()[i].1.clone(); 
                    let handler = this_socket_metadata_manager.get_handler_for_meta(&sm);
                    handlers.push(handler);
                }

                // run loop
                while this_running.load(Ordering::Relaxed) {
                    let mut poll_list = Vec::new();
                    for i in 0..sockets_manager.get_sockets_and_metas().len() {
                        let socket = &sockets_manager.get_sockets_and_metas()[i].0;
                        poll_list.push(socket.as_poll_item(zmq::POLLIN|zmq::POLLOUT));
                    }

                    zmq::poll(&mut poll_list, 1).unwrap();

                    for i in 0..poll_list.len() {
                        let handler = handlers[i].clone();
                        let (socket, sm)  = &sockets_manager.get_sockets_and_metas()[i];
                        if poll_list[i].is_readable() {
                            // this goes on heap
                            let recv_chan = handler.get_recv_chan(sm);
                            if !recv_chan.0.is_full() {
                                let bytes = socket.recv_bytes(zmq::DONTWAIT).unwrap();
                                let recv_chan = handler.get_recv_chan(sm);
                                recv_chan.0.send(Box::new(bytes)).unwrap();
                            }
                        }

                        if poll_list[i].is_writable() {
                            let send_chan = handler.get_send_chan(sm);
                            if !send_chan.1.is_empty() {
                                let bytes = send_chan.1.recv().unwrap();
                                socket.send(bytes.as_ref(), zmq::DONTWAIT).unwrap();
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
        self.sockets_monitor.wait_for_monitor_ready();
        let err = self.sockets_monitor.wait_for_all_connected();
        let io_loop_name = self.name.clone();
        println!("[Loop {io_loop_name}] All sockets connected");
        err
    }

    pub fn close(&self) {
        let name = &self.name;
        self.sockets_monitor.close();
        self.running.store(false, Ordering::Relaxed);
        while !self.io_threads.is_empty() {
            let handle = self.io_threads.pop();
            handle.unwrap().join().unwrap();
        }
        // TODO destroy zmq context
        println!("Closed loop {name}");
    }
}
