use std::{cmp::max, collections::HashMap, fs, sync::{atomic::{AtomicBool, Ordering}, Arc, Mutex, RwLock}, thread::{JoinHandle, ThreadId}, time};

use super::{channel::{self, Channel}, socket_meta::{SocketKind, SocketMetadata, SocketOwner}};

use super::socket_meta::ipc_path_from_addr;

type Bytes = Vec<i8>;

#[derive(PartialEq, Eq)]
enum Direction {
    Sender,
    Receiver
}

#[derive(PartialEq, Eq)]
enum IOHandlerType {
    DataReader,
    DataWriter,
    TransferSender,
    TransferReceiver
}

trait IOHandler {
    fn get_handler_type(&self) -> IOHandlerType;

    fn send(&self, channel_id: &String, b: &Bytes);

    fn recv(&self, channel_id: &String, b: &Bytes);

    fn get_channels(&self) -> &Vec<Channel>;
}


struct DataWriter {
    name: String
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

    fn close_sockets(&mut self) {}
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
            Channel::Remote {
                channel_id, 
                source_local_ipc_addr, 
                source_node_ip, 
                source_node_id, 
                target_local_ipc_addr, 
                target_node_ip, 
                target_node_id, 
                port 
            } => {
                panic!("Remote Not supported")
            }
        }
    }
    v
}

struct IOLoop {
    handlers: Arc<RwLock<Vec<Arc<dyn IOHandler + Send + Sync>>>>,
    running: Arc<AtomicBool>,
    zmq_context: Arc<zmq::Context>,
    io_threads: Vec<JoinHandle<()>>
}

impl IOLoop {

    fn new() -> IOLoop {
        let io_loop = IOLoop{
            handlers: Arc::new(RwLock::new(Vec::new())),
            running: Arc::new(AtomicBool::new(false)), 
            zmq_context: Arc::new(zmq::Context::new()),
            io_threads: Vec::new()
        };
        io_loop
    }

    fn register_handler(&mut self, handler: Arc<dyn IOHandler + Send + Sync>) {
        self.handlers.write().unwrap().push(handler);
    }

    fn start_io_threads(&mut self, num_threads: i16) {
        // since zmq::Sockets are not thread safe we will have a model where each socket can be polled by only 1 IO thread
        // each IO thread can have multiple sockets associated with it
        // let mut num_sockets = 0;
        let mut sockets_metadata = Vec::new();
        for handler in self.handlers.clone().read().unwrap().iter() {
            let handler_type = handler.get_handler_type();
            let channels = handler.get_channels();
            let dir;
            if (handler_type == IOHandlerType::DataWriter) | (handler_type == IOHandlerType::TransferSender) {
                dir = Direction::Sender;
            } else {
                dir = Direction::Receiver;
            }
            sockets_metadata.append(&mut create_local_sockets_meta(channels, dir));
        }

        let num_threads = max(num_threads, sockets_metadata.len() as i16);
        let mut cur_thread_id = 0;
        let mut sockets_meta_per_thread = HashMap::new();

        // round-robin distribution
        for sm in sockets_metadata {
            cur_thread_id = cur_thread_id%num_threads;
            if sockets_meta_per_thread.contains_key(&cur_thread_id) {
                sockets_meta_per_thread.insert(cur_thread_id, vec![sm]);
            } else {
                sockets_meta_per_thread.get_mut(&cur_thread_id).unwrap().push(sm);
            }
        }

        for (_thread_id, sms) in sockets_meta_per_thread.iter() {
            let new_sms = sms.to_vec();
            let this_zmqctx = Arc::clone(&self.zmq_context);
            let f = move |metas: &Vec<SocketMetadata>| {
                let mut socket_manager = SocketManager::new();
                socket_manager.create_sockets(this_zmqctx.as_ref(), metas);
                socket_manager.bind_and_connect();
                // socket_manager.sockets_and_metas
            };

            self.io_threads.push(std::thread::spawn(move || {f(&new_sms)}));
        }

        // thread_handles.into_iter().for_each(|c| c.join().unwrap());
        // let sockets_meta_per_thread = Arc::new(RwLock::new(HashMap::new()));



    }

    fn run(&mut self) -> usize {
        let hm = HashMap::new();
        let sockets_meta_per_thread = Arc::new(RwLock::new(hm));
        // let zmq_context = zmq::Context::new();

        let m = Arc::clone(&sockets_meta_per_thread);
        let r = self.running.clone();
        let h = self.handlers.clone();
        let z = self.zmq_context.clone();
        let f = move || {
            let sock = z.socket(zmq::PAIR).unwrap();
            let b = sock.recv_bytes(zmq::DONTWAIT).unwrap();
            sock.send(b, zmq::DONTWAIT).unwrap();
            let addr = String::from("ipc://test_addr");
            let _ = sock.bind(&addr);
            let socket_meta = SocketMetadata{
                owner: SocketOwner::Client,
                kind: SocketKind::Bind,
                channel_id: String::from("ch_0"),
                addr,
            };
            // let socket_meta_addr = socket_meta.get_addr();
            let thread_id = std::thread::current().id();
            let mut this_m = m.write().unwrap();
            this_m.insert(thread_id, vec![socket_meta]);
            let mut poll_list = [sock.as_poll_item(zmq::POLLIN|zmq::POLLOUT)];
            loop  {
                zmq::poll(&mut poll_list, 1).unwrap();
                let running = r.load(Ordering::Relaxed);
                let v =  h.read().unwrap().clone();                    
                for handler in v.into_iter(){
                    // handler
                    let _ = handler.get_handler_type(); 
                }
                    
                if !running {
                    break;
                }
            }            
        };

        // thread::scope(|scope| {
        //     scope.spawn(f)
        // });
        std::thread::spawn(f).join();
        let l = sockets_meta_per_thread.read().unwrap().len();
        l
        // sockets_meta_per_thread.as_ref();
    }

    fn stop_after(&mut self) {
        let r = self.running.clone();
        std::thread::spawn(move || {
            std::thread::sleep(time::Duration::from_millis(1000 * 5));
            r.store(false, Ordering::Relaxed);
        });
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{atomic::AtomicBool, Arc, RwLock};

    use super::IOLoop;


    #[test]
    fn test_io_loop() {
        let mut io_loop = IOLoop::new();
        io_loop.start_io_threads(4);
        io_loop.stop_after();
        let l = io_loop.run();
        assert_eq!(l, 0);
        print!("Size: {l}")
    }
}