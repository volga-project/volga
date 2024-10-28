use core::time;
use std::{sync::{atomic::{AtomicBool, Ordering}, Arc, Mutex}, thread::{self, JoinHandle}, time::SystemTime};

use crossbeam::queue::SegQueue;
use crossbeam_skiplist::SkipMap;

use super::sockets::{SocketKind, SocketMetadata};

// TODO class description
pub struct SocketMonitor {
    sockets_connected_status: Arc<SkipMap<SocketMetadata, AtomicBool>>,
    registered_sockets: Arc<Mutex<Vec<(SocketMetadata, String)>>>,
    zmq_context: Arc<zmq::Context>,
    monitor_thread_handle: Arc<SegQueue<JoinHandle<()>>>,
    running: Arc<AtomicBool>,
    ready: Arc<AtomicBool>
}

impl SocketMonitor {

    pub fn new(zmq_context: Arc<zmq::Context>) -> Self {
        SocketMonitor{
            sockets_connected_status: Arc::new(SkipMap::new()), 
            registered_sockets: Arc::new(Mutex::new(Vec::new())), 
            zmq_context: zmq_context,
            monitor_thread_handle: Arc::new(SegQueue::new()),
            running: Arc::new(AtomicBool::new(false)),
            ready: Arc::new(AtomicBool::new(false))
        }
    }

    pub fn register_sockets(&self, sockets_and_metas: &Vec<(zmq::Socket, SocketMetadata)>) {
        let this_registered_sockets = self.registered_sockets.clone();
        let mut locked_registered_sockets = this_registered_sockets.lock().unwrap();
        for (socket, sm) in sockets_and_metas {
            if sm.kind == SocketKind::Dealer {
                let fd = socket.get_fd().unwrap();
                let monitor_endpoint = format!("inproc://monitor.s-{fd}");
                socket.monitor(&monitor_endpoint, zmq::SocketEvent::CONNECTED as i32).unwrap();
                locked_registered_sockets.push((sm.clone(), monitor_endpoint));
            }
        }
    }

    pub fn wait_for_monitor_ready(&self) {
        let timeout_ms = 5000;
        let start = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis();
        while SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() - start < timeout_ms {
            if self.ready.load(Ordering::Relaxed) {
                return
            }
            thread::sleep(time::Duration::from_millis(100));
        }
        panic!("Timeout waiting for monitor to be ready");
    }

    pub fn wait_for_all_connected(&self, timeout_ms: Option<u128>) -> Option<String> {
        let mut timeout = 1000 as u128; // default
        if timeout_ms.is_some() {
            timeout = timeout_ms.unwrap();
        }
        let start = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis();
        while SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() - start < timeout {
            if self.all_connected() {
                return None
            }
            thread::sleep(time::Duration::from_millis(100));
        }

        let mut not_connected = Vec::new();
        let this_sockets_connected_status = self.sockets_connected_status.clone();
        for e in this_sockets_connected_status.as_ref() {
            if !e.value().load(Ordering::Relaxed) {
                let addr = e.key().addr.clone();
                not_connected.push(addr);
            }
        }
        let err = format!("{:?}", not_connected);
        Some(err)
    }

    pub fn all_connected(&self) -> bool {
        let this_sockets_connected_status = self.sockets_connected_status.clone();
        Self::_all_connected(this_sockets_connected_status.as_ref())
    }

    pub fn _all_connected(sockets_connected_status: &SkipMap<SocketMetadata, AtomicBool>) -> bool {
        for entry in sockets_connected_status {
            let connected = entry.value();
            if !connected.load(Ordering::Relaxed) {
                return false
            }
        }
        true
    }

    pub fn start(&self) {
        let this_zmqctx = self.zmq_context.clone();
        let this_runnning = self.running.clone();
        let this_registered_sockets = self.registered_sockets.clone();
        let this_sockets_connected_status = self.sockets_connected_status.clone();
        let this_ready = self.ready.clone();

        self.running.store(true, Ordering::Relaxed);

        let f = move || {
            // wait for ioloop to register sockets
            let mut registered = false;
            let register_timeout_ms = 5000;
            let start = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis();
            while SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() - start < register_timeout_ms {
                let locked_registered_sockets = this_registered_sockets.lock().unwrap();
                if locked_registered_sockets.len() != 0 {
                    drop(locked_registered_sockets);
                    registered = true;
                    break;
                }
                drop(locked_registered_sockets);
                thread::sleep(time::Duration::from_millis(100));
            }
            if !registered {
                panic!("IOLoop did not register sockets for monitoring within timeout");
            }
            
            // create and connect monitors
            let locked_registered_sockets = this_registered_sockets.lock().unwrap();
            let mut monitors = Vec::new();
            for (sm, monitor_endpoint) in locked_registered_sockets.iter() {
                if sm.kind == SocketKind::Dealer {
                    let monitor_socket = this_zmqctx.socket(zmq::PAIR).unwrap();
                    monitor_socket.connect(&monitor_endpoint).unwrap();

                    monitors.push((monitor_socket, sm.clone()));
                    this_sockets_connected_status.insert(sm.clone(), AtomicBool::new(false));
                }
            }

            // set ready so io loop can start sockets after we registered monitors
            this_ready.store(true, Ordering::Relaxed);

            // start reading monitor sockets events
            while this_runnning.load(Ordering::Relaxed) {
                if Self::_all_connected(this_sockets_connected_status.as_ref()) {
                    break;
                }

                for i in 0..monitors.len() {
                    let (monitor_socket, sm)  = &monitors[i];
                    let event = Self::try_get_monitor_event(monitor_socket);
                    if event.is_some() {
                        let event = event.unwrap();
                        if event == zmq::SocketEvent::CONNECTED {
                            this_sockets_connected_status.get(&sm).unwrap().value().store(true, Ordering::Relaxed);
                        }
                    }
                }

                thread::sleep(time::Duration::from_millis(1));
            }
        };
        let thread_name = format!("volga_monitor_thread");
        self.monitor_thread_handle.push(
            std::thread::Builder::new().name(thread_name).spawn(f).unwrap()
        );

    }

    fn try_get_monitor_event(monitor: &zmq::Socket) -> Option<zmq::SocketEvent> {
        let msg = monitor.recv_msg(zmq::DONTWAIT);
        if !msg.is_ok() {
            return None;
        }
        let msg = msg.unwrap();
        // TODO: could be simplified by using `TryInto` (since 1.34)
        let event = u16::from_ne_bytes([msg[0], msg[1]]);
    
        assert!(
            monitor.get_rcvmore().unwrap(),
            "Monitor socket should have two messages per event"
        );
    
        // the address, we'll ignore it
        let _ = monitor.recv_msg(0).unwrap();
    
        Some(zmq::SocketEvent::from_raw(event))
    }

    pub fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);
        while !self.monitor_thread_handle.is_empty() {
            let handle = self.monitor_thread_handle.pop();
            handle.unwrap().join().unwrap();
        }
    }
}