use core::time;
use std::{sync::{atomic::{AtomicBool, Ordering}, Arc}, thread::{self, JoinHandle}, time::SystemTime};

use crossbeam::queue::SegQueue;
use crossbeam_skiplist::SkipMap;

use super::sockets::{SocketKind, SocketMetadata};


pub struct SocketsMonitor {
    sockets_connected_status: Arc<SkipMap<SocketMetadata, AtomicBool>>,
    registered_sockets: Arc<SkipMap<usize, Vec<(SocketMetadata, String)>>>,
    zmq_context: Arc<zmq::Context>,
    monitor_thread: Arc<SegQueue<JoinHandle<()>>>,
    running: Arc<AtomicBool>,
    ready: Arc<AtomicBool>
}

impl SocketsMonitor {

    pub fn new(zmq_context: Arc<zmq::Context>) -> Self {
        SocketsMonitor{
            sockets_connected_status: Arc::new(SkipMap::new()), 
            registered_sockets: Arc::new(SkipMap::new()), 
            zmq_context: zmq_context,
            monitor_thread: Arc::new(SegQueue::new()),
            running: Arc::new(AtomicBool::new(false)),
            ready: Arc::new(AtomicBool::new(false))
        }
    }

    pub fn register_sockets(&self, thread_id: usize, sockets_and_metas: &Vec<(zmq::Socket, SocketMetadata)>) {
        let this_registered_sockets = self.registered_sockets.clone();
        let mut v = Vec::new();
        for (socket, sm) in sockets_and_metas {
            if sm.kind == SocketKind::Connect {
                let fd = socket.get_fd().unwrap();
                let monitor_endpoint = format!("inproc://monitor.s-{fd}");
                socket.monitor(&monitor_endpoint, zmq::SocketEvent::CONNECTED as i32).unwrap();
                v.push((sm.clone(), monitor_endpoint));
            }
        }
        this_registered_sockets.insert(thread_id, v);
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

    pub fn wait_for_all_connected(&self) -> Option<String> {
        let timeout_ms = 5000;
        let start = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis();
        while SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() - start < timeout_ms {
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

    pub fn mark_ready(&self) {
        self.ready.store(true, Ordering::Relaxed);
    }

    pub fn mark_connected(&self, sm: SocketMetadata) {
        let this_sockets_connected_status = self.sockets_connected_status.clone();
        this_sockets_connected_status.get(&sm).unwrap().value().store(true, Ordering::Relaxed);
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

    pub fn start(&self, num_expected_io_threads: usize) {
        let this_zmqctx = self.zmq_context.clone();
        let this_runnning = self.running.clone();
        let this_registered_sockets = self.registered_sockets.clone();
        let this_sockets_connected_status = self.sockets_connected_status.clone();
        let this_ready = self.ready.clone();

        self.running.store(true, Ordering::Relaxed);

        let f = move || {
            // wait for all io  threads to register sockets
            let mut all_registered = false;
            let register_timeout_ms = 5000;
            let start = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis();
            while SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() - start < register_timeout_ms {
                if this_registered_sockets.len() == num_expected_io_threads {
                    all_registered = true;
                    break;
                }
                thread::sleep(time::Duration::from_millis(100));
            }
            if !all_registered {
                panic!("Not all io threads registered within timeout");
            }
            
            // create and connect monitors
            let mut monitors = Vec::new();
            for e in this_registered_sockets.as_ref() {
                for (sm, monitor_endpoint) in e.value() {
                    if sm.kind == SocketKind::Connect {
                        let monitor_socket = this_zmqctx.socket(zmq::PAIR).unwrap();
                        monitor_socket.connect(&monitor_endpoint).unwrap();

                        monitors.push((monitor_socket, sm.clone()));
                        this_sockets_connected_status.insert(sm.clone(), AtomicBool::new(false));
                    }
                }
            }

            // set ready so io threads can start sockets after we registered monitors
            this_ready.store(true, Ordering::Relaxed);

            // start reading monitor sockets events
            while this_runnning.load(Ordering::Relaxed) {
                if Self::_all_connected(this_sockets_connected_status.as_ref()) {
                    break;
                }

                for i in 0..monitors.len() {
                    let (monitor_socket, sm)  = &monitors[i];
                    let addr = &sm.addr;
                    let event = Self::try_get_monitor_event(monitor_socket);
                    if event.is_some() {
                        let event = event.unwrap();
                        if event == zmq::SocketEvent::CONNECTED {
                            this_sockets_connected_status.get(&sm).unwrap().value().store(true, Ordering::Relaxed);
                        }
                    }
                }
            }
        };
        let thread_name = format!("volga_monitor_thread");
        self.monitor_thread.push(
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

    pub fn close(&self) {
        self.running.store(false, Ordering::Relaxed);
        while !self.monitor_thread.is_empty() {
            let handle = self.monitor_thread.pop();
            handle.unwrap().join().unwrap();
        }
    }
}