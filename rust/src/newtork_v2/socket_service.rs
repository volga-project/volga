use core::time;
use std::{collections::HashMap, sync::{atomic::{AtomicBool, Ordering}, Arc, Mutex}, thread::{self, JoinHandle}, time::SystemTime};

use crossbeam::{channel::{Receiver, Sender}, queue::SegQueue};
use pyo3::{pyclass, pymethods};
use serde::{Deserialize, Serialize};
use zmq::DONTWAIT;

use crate::newtork_v2::{buffer_utils::get_buffer_id, channel::DataReaderResponseMessage, sockets::{SocketKind, SocketManager, SocketMetadata}};

use super::{buffer_utils::Bytes, channel::Channel, socket_monitor::SocketMonitor};

pub const CROSSBEAM_DEFAULT_CHANNEL_SIZE: usize = 15000;

#[derive(Serialize, Deserialize, Clone)]
#[pyclass(name="RustZmqConfig")]
pub struct ZmqConfig {
    pub sndhwm: Option<i32>,
    pub rcvhwm: Option<i32>,
    pub sndbuf: Option<i32>,
    pub rcvbuf: Option<i32>,
    pub linger: Option<i32>,
    pub connect_timeout_s: Option<i32>,
    pub num_io_threads: Option<i32>
}

#[pymethods]
impl ZmqConfig { 
    #[new]
    pub fn new(sndhwm: Option<i32>, rcvhwm: Option<i32>, sndbuf: Option<i32>, rcvbuf: Option<i32>, linger: Option<i32>, connect_timeout_s: Option<i32>, num_io_threads: Option<i32>) -> Self {
        ZmqConfig{sndhwm, rcvhwm, sndbuf, rcvbuf, linger, connect_timeout_s, num_io_threads}
    }
}

pub type SocketMessage = (Option<String>, Bytes);

// TODO description
pub trait SocketServiceSubscriber {

    fn get_id(&self) -> &String;

    fn get_name(&self) -> &String;

    fn get_channels(&self) -> &Vec<Channel>;

    fn get_sockets_metas(&self) -> &Vec<SocketMetadata>;

    fn get_in_chan(&self, sm: &SocketMetadata) -> (Sender<SocketMessage>, Receiver<SocketMessage>);

    fn get_out_chan(&self,  sm: &SocketMetadata) -> (Sender<SocketMessage>, Receiver<SocketMessage>);

    fn start(&self);

    fn stop(&self);
}


/**
 * SocketServiceSubscriber - DataWriter, DataReader, TransferSender, TransferReceiver
 * 
 * Spawns a thread which polls sockets .. TODO
 */
pub struct SocketService {
    name: String,
    subscribers: Arc<Mutex<Vec<Arc<dyn SocketServiceSubscriber + Send + Sync>>>>,
    running: Arc<AtomicBool>,
    zmq_context: Arc<zmq::Context>,
    io_thread_handle: Arc<SegQueue<JoinHandle<()>>>,
    zmq_config: Option<ZmqConfig>,
    sockets_monitor: Arc<SocketMonitor>,
}

impl SocketService {

    pub fn new(name: String, zmq_config: Option<ZmqConfig>) -> SocketService {
        let zmq_ctx = Arc::new(zmq::Context::new());
        let mut num_io_threads = 1;
        if zmq_config.is_some() {
            let num_io_threads_opt = zmq_config.as_ref().unwrap().num_io_threads;
            if num_io_threads_opt.is_some() {
                num_io_threads = num_io_threads_opt.unwrap();
            }
        }
        zmq_ctx.set_io_threads(num_io_threads).unwrap();
        SocketService{
            name,
            subscribers: Arc::new(Mutex::new(Vec::new())),
            running: Arc::new(AtomicBool::new(false)), 
            zmq_context: zmq_ctx.clone(),
            io_thread_handle: Arc::new(SegQueue::new()),
            zmq_config,

            sockets_monitor: Arc::new(SocketMonitor::new(zmq_ctx.clone())),
        }
    }

    pub fn subscribe(&self, handler: Arc<dyn SocketServiceSubscriber + Send + Sync>) {
        self.subscribers.lock().unwrap().push(handler);
    }

    fn run_io_thread(&self, connection_timeout_ms: u128) {
        self.sockets_monitor.start();
        
        // since zmq::Sockets are not thread safe we will have a model where each socket can be polled by only 1 IO thread
        // each IO thread can have multiple sockets associated with it
        let name = self.name.clone();
        println!("[SocketService {name}] Launched");
        let locked_subscribers = self.subscribers.lock().unwrap();

        if locked_subscribers.len() == 0 {
            panic!("{name} SocketService started with no subscribers");
        }
        drop(locked_subscribers);

        let this_subscribers = self.subscribers.clone();
        let this_sockets_monitor = self.sockets_monitor.clone();
        let this_running = self.running.clone();
        let this_name = self.name.clone();

        let this_zmq_config = self.zmq_config.clone();
        let this_zmqctx = self.zmq_context.clone();

        let f = move || {
            let locked_subscribers = this_subscribers.lock().unwrap();
            
            let mut socket_manager = SocketManager::new(locked_subscribers.clone(), this_zmqctx, this_zmq_config);
            drop(locked_subscribers);
            
            socket_manager.create_sockets();
            this_sockets_monitor.register_sockets(socket_manager.get_sockets());
            this_sockets_monitor.wait_for_monitor_ready();
            socket_manager.bind_and_connect();

            let err = this_sockets_monitor.wait_for_all_connected(Some(connection_timeout_ms));
            if err.is_some() {
                return
            }

            Self::_wait_to_start_running(this_running.clone()); // TODO why is this needed?

            let sockets = socket_manager.get_sockets();
            // send HI from all DEALERS to ROUTERS - needed for ROUTERS to properly set identities and start working
            let hi = "HI";
            let mut num_dealers = 0;
            for (socket, socket_meta) in sockets {
                if socket_meta.kind == SocketKind::Dealer {
                    socket.send(hi, 0).expect("Unable to send HI");
                    num_dealers += 1;
                }
            }
            println!("[SocketService {name}] Sent HI from {num_dealers} DEALERs");

            // wait for all the ROUTERS to receive HI
            // TODO add timeout
            let mut num_routers = 0;
            for (socket, socket_meta) in sockets {
                if socket_meta.kind == SocketKind::Router {
                    let _identity = socket.recv_string(0).expect("Router failed receiving identity on HI").unwrap();
                    let _hi = socket.recv_string(0).expect("Router failed receiving HI").unwrap();
                    if _hi != hi {
                        panic!("HI is not HI: {hi}");
                    }
                    num_routers += 1;
                }
            }
            println!("[SocketService {name}] Received HI on {num_routers} ROUTERs");
            
            // contains bytes (+optional destination identity for DEALER) read from subscriber but not sent due to full socket
            let mut not_sent: HashMap<&SocketMetadata, SocketMessage> = HashMap::new();

            let lim = 5;

            let mut in_chans = HashMap::new();
            let mut out_chans = HashMap::new();

            for (_, sm) in sockets {
                in_chans.insert(sm.identity.clone(), socket_manager.get_subscriber_in_chan(sm));
                out_chans.insert(sm.identity.clone(), socket_manager.get_subscriber_out_chan(sm));
            }

            // run loop
            while this_running.load(Ordering::Relaxed) {
                let mut poll_list = Vec::new();
                for (socket, _) in sockets {
                    poll_list.push(socket.as_poll_item(zmq::POLLIN|zmq::POLLOUT));
                }

                zmq::poll(&mut poll_list, 1).unwrap();

                for i in 0..poll_list.len() {
                    let (socket, sm)  = &sockets[i];
                    if poll_list[i].is_readable() {
                        let in_chan = in_chans.get(&sm.identity).unwrap();
                        let mut j = 0;
                        while this_running.load(Ordering::Relaxed) {
                            if in_chan.0.is_full() {
                                break;
                            }

                            if j > lim {
                                break;
                            }
                            
                            let mut b: Option<Bytes> = None;
                            let mut identity: Option<String> = None;
                            if sm.kind == SocketKind::Router {
                                // zmq::ROUTER receives an identity frame first and actual data after, so we read twice
                                let _identity = socket.recv_string(0);
                                if _identity.is_ok() {
                                    // expect second frame to be ready right away
                                    b = Some(socket.recv_bytes(zmq::DONTWAIT).expect("zmq DEALER data frame is empty while identity is present"));
                                    identity = Some(_identity.unwrap().unwrap());
                                }
                            } else if sm.kind == SocketKind::Dealer {
                                // zmq::DELAER receives data directly
                                let _b = socket.recv_bytes(zmq::DONTWAIT);
                                if _b.is_ok() {
                                    b = Some(_b.unwrap());
                                }
                            } else {
                                panic!("Unknown socket kind")
                            }

                            if b.is_some() {
                                let _b = b.unwrap();
                                // let bid = get_buffer_id(&_b);
                                let socket_message = (identity, _b);
                                in_chan.0.try_send(socket_message).expect("In chan should not be full");
                                // println!("Rcvd {bid}");
                                // println!("Rcvd ---");
                            } else {
                                break;
                            }
                            j += 1;
                        }
                    }

                    if poll_list[i].is_writable() {
                        let mut j = 0;
                        
                        let out_chan = out_chans.get(&sm.identity).unwrap();
                        while this_running.load(Ordering::Relaxed) {
                            if j > lim {
                                break;
                            }

                            let mut b: Option<Bytes> = None;
                            let mut identity: Option<String> = None;
                            if not_sent.contains_key(sm) {
                                let (_identity, _bytes) = not_sent.get(sm).unwrap();
                                identity = _identity.clone();
                                b = Some(_bytes.clone());
                                not_sent.remove(sm);
                            } else {
                                if out_chan.1.is_empty() {
                                    break;
                                }

                                let (_identity, _bytes) = out_chan.1.try_recv().expect("Out chan should not be empty");
                                identity = _identity;
                                b = Some(_bytes.clone());
                            }

                            if b.is_none() {
                                break;
                            }

                            let mut identity_sent = false;
                            if identity.is_some() {
                                // this should be a Router socket
                                if sm.kind != SocketKind::Router {
                                    panic!("socket kind mismatch");
                                }
                                let _identity = identity.clone().unwrap();
                    
                                // send identity frame first
                                let res = socket.send(&_identity, zmq::SNDMORE);
                                if !res.is_ok() {
                                    not_sent.insert(sm, (Some(_identity), b.unwrap()));
                                    break;
                                }
                                identity_sent = true;
                            }
                            
                            let bytes = b.unwrap();
                            let res = socket.send(&bytes, zmq::DONTWAIT);
                            if !res.is_ok() {
                                if identity_sent {
                                    // should not happen - panic
                                    panic!("Unable to send data after sending identity");
                                }
                                not_sent.insert(sm, (identity, bytes));
                                break;
                            }
                            j += 1;
                        }
                    }
                }
            }
        };
        let thread_name = format!("{this_name}_io_thread");
        self.io_thread_handle.push(
            std::thread::Builder::new().name(thread_name).spawn(f).unwrap()
        );
    }

    fn _wait_to_start_running(running: Arc<AtomicBool>) -> bool {
        let timeout_ms = 5000;
        let start = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis();
        while SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() - start < timeout_ms {
            if running.load(Ordering::Relaxed) {
                return true
            }
            thread::sleep(time::Duration::from_millis(100));
        }
        false
    }

    pub fn start(&self) {
        let err = self.sockets_monitor.wait_for_all_connected(None);
        if err.is_some() {
            panic!("Can not start io loop - connection error")
        }
        let name = &self.name;
        self.running.store(true, Ordering::Relaxed);
        println!("[Loop {name}] Started data flow");
    }

    pub fn connect(&self, timeout_ms: u128) -> Option<String> {
        self.run_io_thread(timeout_ms);
        self.sockets_monitor.wait_for_monitor_ready();
        let err = self.sockets_monitor.wait_for_all_connected(Some(timeout_ms));
        let io_loop_name = self.name.clone();
        self.sockets_monitor.stop();
        if err.is_none() {
            println!("[Loop {io_loop_name}] All sockets connected");
        }
        err
    }

    pub fn stop(&self) {
        let name = &self.name;
        self.sockets_monitor.stop();
        self.running.store(false, Ordering::Relaxed);
        while !self.io_thread_handle.is_empty() {
            let handle = self.io_thread_handle.pop();
            handle.unwrap().join().unwrap();
        }
        // TODO destroy zmq context
        println!("Closed loop {name}");
    }
}
