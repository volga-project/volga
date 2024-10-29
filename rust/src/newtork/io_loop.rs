use core::time;
use std::{collections::HashMap, sync::{atomic::{AtomicBool, Ordering}, Arc, Mutex}, thread::{self, JoinHandle}, time::SystemTime};

use crossbeam::{channel::{Receiver, Sender}, queue::SegQueue};
use pyo3::{pyclass, pymethods};
use serde::{Deserialize, Serialize};

use crate::newtork::{buffer_utils::get_channeld_id, sockets::{SocketKind, SocketManager, SocketMetadata}};

use super::{buffer_utils::Bytes, channel::Channel, socket_monitor::SocketMonitor};

pub const CROSSBEAM_DEFAULT_CHANNEL_SIZE: usize = 10000;

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

// TODO description
pub trait IOHandler {

    fn get_id(&self) -> &String;

    fn get_name(&self) -> &String;

    fn get_channels(&self) -> &Vec<Channel>;

    fn get_sockets_metas(&self) -> &Vec<SocketMetadata>;

    fn get_in_sender(&self, sm: &SocketMetadata) -> Sender<Bytes>;

    fn get_out_receiver(&self,  sm: &SocketMetadata) -> Receiver<Bytes>;

    fn start(&self);

    fn stop(&self);
}


/**
 * IOHandler - DataWriter, DataReader, TransferSender, TransferReceiver
 * 
 * Spawns a thread which polls sockets .. TODO
 */
pub struct IOLoop {
    name: String,
    handlers: Arc<Mutex<Vec<Arc<dyn IOHandler + Send + Sync>>>>,
    running: Arc<AtomicBool>,
    zmq_context: Arc<zmq::Context>,
    io_thread_handle: Arc<SegQueue<JoinHandle<()>>>,
    zmq_config: Option<ZmqConfig>,
    sockets_monitor: Arc<SocketMonitor>,
}

#[derive(Serialize, Deserialize, Debug)]
struct DealerToRouterRegisterMessage {
    identity: String,
    channel_ids: Vec<String> 
}

impl IOLoop {

    pub fn new(name: String, zmq_config: Option<ZmqConfig>) -> IOLoop {
        let zmq_ctx = Arc::new(zmq::Context::new());
        let mut num_io_threads = 1;
        if zmq_config.is_some() {
            let num_io_threads_opt = zmq_config.as_ref().unwrap().num_io_threads;
            if num_io_threads_opt.is_some() {
                num_io_threads = num_io_threads_opt.unwrap();
            }
        }
        zmq_ctx.set_io_threads(num_io_threads).unwrap();
        IOLoop{
            name: name.clone(),
            handlers: Arc::new(Mutex::new(Vec::new())),
            running: Arc::new(AtomicBool::new(false)), 
            zmq_context: zmq_ctx.clone(),
            io_thread_handle: Arc::new(SegQueue::new()),
            zmq_config,
            sockets_monitor: Arc::new(SocketMonitor::new(name.clone(), zmq_ctx.clone())),
        }
    }

    pub fn register_handler(&self, handler: Arc<dyn IOHandler + Send + Sync>) {
        self.handlers.lock().unwrap().push(handler);
    }

    fn run_io_thread(&self, connection_timeout_ms: u128) {
        self.sockets_monitor.start();
        
        // since zmq::Sockets are not thread safe we will have a model where each socket can be polled by only 1 IO thread
        // each IO thread can have multiple sockets associated with it
        let name = self.name.clone();
        println!("[IOLoop {name}] Launched");
        let locked_handlers = self.handlers.lock().unwrap();

        if locked_handlers.len() == 0 {
            panic!("{name} IOLoop started with no handlers");
        }
        drop(locked_handlers);

        let this_handlers = self.handlers.clone();
        let this_sockets_monitor = self.sockets_monitor.clone();
        let this_running = self.running.clone();
        let this_name = self.name.clone();
        let _name = this_name.clone();

        let this_zmq_config = self.zmq_config.clone();
        let this_zmqctx = self.zmq_context.clone();

        let f = move || {
            let locked_handlers = this_handlers.lock().unwrap();
            
            let mut socket_manager = SocketManager::new(locked_handlers.clone(), this_zmqctx, this_zmq_config);
            drop(locked_handlers);
            
            socket_manager.create_sockets();
            this_sockets_monitor.register_sockets(socket_manager.get_sockets());

            this_sockets_monitor.wait_for_monitor_ready();
            socket_manager.bind_and_connect();

            let err = this_sockets_monitor.wait_for_all_connected(Some(connection_timeout_ms));
            if err.is_some() {
                return
            }

            Self::_wait_to_start_running(this_running.clone());

            let sockets = socket_manager.get_sockets();
            
            // send message from all DEALERS to their ROUTERS - needed for ROUTERS to properly set input identities, map channels and start working
            let mut num_dealers = 0;
            for (socket, socket_meta) in sockets {
                if socket_meta.kind == SocketKind::Dealer {
                    let register_message = DealerToRouterRegisterMessage{
                        identity: socket_meta.identity.clone(), channel_ids: socket_meta.channel_ids.clone()
                    };  
                    let _encoded = serde_json::to_string(&register_message).unwrap();
                    socket.send(&_encoded, 0).expect("Unable to send register message");
                    num_dealers += 1;
                }
            }
            println!("[IOLoop {name}] Sent register messages from {num_dealers} DEALERs");

            // wait for all the ROUTERS to receive register message and construct channel<->socket_identity mapping
            let mut channels_to_identities: HashMap<String, HashMap<String, String>> = HashMap::new(); // for each router socket identity, mapping of it's channel ids to sending dealer's identities

            // TODO add timeout
            let mut num_routers = 0;
            for (socket, socket_meta) in sockets {
                if socket_meta.kind == SocketKind::Router {
                    let mut channel_id_to_identity = HashMap::new();
                    // TODO timeout
                    while channel_id_to_identity.len() != socket_meta.channel_ids.len() {
                        // wait until we recive hi from all dealers using this router
                        let _identity = socket.recv_string(0).expect("Router failed receiving identity on register").unwrap();
                        let _encoded = socket.recv_string(0).expect("Router failed receiving register message").unwrap();
                        
                        let register_message: DealerToRouterRegisterMessage = serde_json::from_str(&_encoded).unwrap();
                        if _identity != register_message.identity {
                            panic!("Routre recived inconsistent identity on register");
                        }
                        for channel_id in register_message.channel_ids {
                            if channel_id_to_identity.contains_key(&channel_id) {
                                panic!("Routre recived duplicate channel ids on register")
                            }
                            channel_id_to_identity.insert(channel_id.clone(), _identity.clone());
                        }
                    }
                    
                    channels_to_identities.insert(socket_meta.identity.clone(), channel_id_to_identity);
                    
                    num_routers += 1;
                }
            }
            println!("[IOLoop {name}] Received register messages on {num_routers} ROUTERs");
            
            // contains bytes (+optional destination identity for DEALER) read from handler but not sent due to full socket
            let mut not_sent: HashMap<&SocketMetadata, Bytes> = HashMap::new();

            let lim = 100;

            let mut in_senders = HashMap::new();
            let mut out_receivers = HashMap::new();

            for (_, sm) in sockets {
                in_senders.insert(sm.identity.clone(), socket_manager.get_handler_in_sender(sm));
                out_receivers.insert(sm.identity.clone(), socket_manager.get_handler_out_receiver(sm));
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
                        let in_sender = in_senders.get(&sm.identity).unwrap();
                        let mut j = 0;
                        while this_running.load(Ordering::Relaxed) {
                            if in_sender.is_full() {
                                break;
                            }

                            if j > lim {
                                break;
                            }
                            
                            let mut b_opt: Option<Bytes> = None;
                            if sm.kind == SocketKind::Router {
                                // zmq::ROUTER receives an identity frame first and actual data after, so we read twice
                                let _identity_res = socket.recv_string(zmq::DONTWAIT);
                                if _identity_res.is_ok() {
                                    // expect second frame to be ready right away
                                    b_opt = Some(socket.recv_bytes(zmq::DONTWAIT).expect("zmq DEALER data frame is empty while identity is present"));
                                }
                            } else if sm.kind == SocketKind::Dealer {
                                // zmq::DELAER receives data directly
                                let _b_opt = socket.recv_bytes(zmq::DONTWAIT);
                                if _b_opt.is_ok() {
                                    b_opt = Some(_b_opt.unwrap());
                                }
                            } else {
                                panic!("Unknown socket kind")
                            }

                            if b_opt.is_some() {
                                let b = b_opt.unwrap();
                                in_sender.try_send(b).expect("In chan should not be full");
                            } else {
                                break;
                            }
                            j += 1;
                        }
                    }

                    if poll_list[i].is_writable() {
                        let mut j = 0;
                        
                        let out_receiver = out_receivers.get(&sm.identity).unwrap();
                        while this_running.load(Ordering::Relaxed) {
                            if j > lim {
                                break;
                            }

                            let mut b_opt: Option<Bytes> = None;
                            if not_sent.contains_key(sm) {
                                let _bytes = not_sent.get(sm).unwrap();
                                b_opt = Some(_bytes.clone());
                                not_sent.remove(sm);
                            } else {
                                if out_receiver.is_empty() {
                                    break;
                                }
                                let iden = &sm.identity;
                                let _bytes = out_receiver.try_recv().expect(&format!("Out chan should not be empty {iden}"));
                                b_opt = Some(_bytes.clone());
                            }

                            if b_opt.is_none() {
                                break;
                            }

                            let mut identity_sent = false;
                            let b = b_opt.unwrap();

                            if sm.kind == SocketKind::Router {
                                // Find delivery identity
                                let channel_id = get_channeld_id(&b);
                                let identity = channels_to_identities.get(&sm.identity).unwrap().get(&channel_id).unwrap();
                                
                                // send identity frame first
                                let res = socket.send(&identity, zmq::DONTWAIT|zmq::SNDMORE);
                                if !res.is_ok() {
                                    not_sent.insert(sm, b.clone());
                                    break;
                                }
                                identity_sent = true;
                            }
                            
                            let res = socket.send(&b, zmq::DONTWAIT);
                            if !res.is_ok() {
                                if identity_sent {
                                    // should not happen - panic
                                    panic!("Unable to send data after sending identity");
                                }
                                not_sent.insert(sm, b.clone());
                                break;
                            }
                            j += 1;
                        }
                    }
                }
            }
        };
        let thread_name = format!("{this_name}_io_loop_thread");
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
        let name = &self.name;
        let err = self.sockets_monitor.wait_for_all_connected(None);
        if err.is_some() {
            panic!("Can not start IOLoop {name} - connection error")
        }
        self.running.store(true, Ordering::Relaxed);
        println!("[IOLoop {name}] Started data flow");
    }

    pub fn connect(&self, timeout_ms: u128) -> Option<String> {
        self.run_io_thread(timeout_ms);
        self.sockets_monitor.wait_for_monitor_ready();
        let err = self.sockets_monitor.wait_for_all_connected(Some(timeout_ms));
        let name = self.name.clone();
        self.sockets_monitor.stop();
        if err.is_none() {
            println!("[IOLoop {name}] All sockets connected");
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
        println!("Stopped loop {name}");
    }
}
