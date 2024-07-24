use std::{cmp::{max, min}, collections::{HashMap, VecDeque}, fs, sync::{atomic::{AtomicBool, Ordering}, Arc, Mutex, RwLock}, thread::JoinHandle, time};

use serde::Serialize;
use zmq::{PollItem, POLLIN};

use super::{channel::{self, Channel, ChannelMessage}, socket_meta::{SocketKind, SocketMetadata, SocketOwner}};

use super::socket_meta::ipc_path_from_addr;

type Bytes = Vec<u8>;

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

    fn on_send_ready(&self, channel_id: &String) -> bool;

    fn on_recv_ready(&self, channel_id: &String) -> bool;

    fn get_channels(&self) -> &Vec<Channel>;

    fn get_in_buffer_queue(&self, key: &String) -> Arc<Mutex<VecDeque<Box<Bytes>>>>;

    fn get_out_buffer_queue(&self, key: &String) -> Arc<Mutex<VecDeque<Box<Bytes>>>>;
}


struct DataWriter {
    name: String,
    channels: Vec<Channel>,
    in_buffer_queues: Arc<RwLock<HashMap<String, Arc<Mutex<VecDeque<Box<Bytes>>>>>>>,
    out_buffer_queues: Arc<RwLock<HashMap<String, Arc<Mutex<VecDeque<Box<Bytes>>>>>>>,
    in_message_queues: Arc<RwLock<HashMap<String, Arc<Mutex<VecDeque<Box<ChannelMessage>>>>>>>,

    running: Arc<AtomicBool>,
}

impl DataWriter {
    fn new(name: String, channels: Vec<Channel>) -> DataWriter {
        let in_queues =  Arc::new(RwLock::new(HashMap::new()));
        let out_queues =  Arc::new(RwLock::new(HashMap::new()));
        let in_message_queues =  Arc::new(RwLock::new(HashMap::new()));

        for ch in &channels {
            in_queues.write().unwrap().insert(ch.get_channel_id().clone(), Arc::new(Mutex::new(VecDeque::new())));
            out_queues.write().unwrap().insert(ch.get_channel_id().clone(), Arc::new(Mutex::new(VecDeque::new())));
            in_message_queues.write().unwrap().insert(ch.get_channel_id().clone(), Arc::new(Mutex::new(VecDeque::new())));
        }

        DataWriter{
            name,
            channels,
            in_buffer_queues: in_queues,
            out_buffer_queues: out_queues,
            in_message_queues,
            running: Arc::new(AtomicBool::new(false))
        }
    }

    fn write_message(&self, channel_id: &String, message: ChannelMessage) {
        // TODO set limit for backpressure
        let queues = &self.in_message_queues.read().unwrap();
        let queue = queues.get(channel_id).unwrap();
        queue.lock().unwrap().push_back(Box::new(message));
    }

    fn start(&self) {
        // start dispatcher thread: takes message from in_message_queue, passes to serializators pool, puts result in out_buffer_queue
        self.running.store(true, Ordering::Relaxed);

        let this_in_message_queues = self.in_message_queues.clone();
        let this_in_buffer_queues = self.in_buffer_queues.clone();
        let this_runnning = self.running.clone();
        let f = move || {
            loop {
                let running = this_runnning.load(Ordering::Relaxed);
                if !running {
                    break;
                }
                let msg_queues = this_in_message_queues.read().unwrap();
                let b_in_queues = this_in_buffer_queues.read().unwrap();
                for channel_id in  msg_queues.keys() {
                    let mut locked_msg_queue = msg_queues.get(channel_id).unwrap().lock().unwrap();
                    let msg = locked_msg_queue.pop_front();
                    drop(locked_msg_queue); // release lock
                    if !msg.is_none() {
                        // TODO we should asynchronously dispatch to serde pool here, for simplicity we do sequential serde
                        let msg = msg.unwrap();
                        let bytes = Box::new(serde_json::to_string(&msg).unwrap().as_bytes().to_vec()); // move to heap
                        
                        // put to buffer queue
                        let mut locked_b_in_queue = b_in_queues.get(channel_id).unwrap().lock().unwrap();
                        locked_b_in_queue.push_back(bytes);
                    }
                }
            }
            
        };
        std::thread::spawn(f);
    }

    fn close (&self) {
        // TODO join thread
    }
}

impl IOHandler for DataWriter {
    fn get_handler_type(&self) -> IOHandlerType {
        IOHandlerType::DataWriter
    }

    fn on_send_ready(&self, channel_id: &String) -> bool {
        true
    }

    fn on_recv_ready(&self, channel_id: &String) -> bool {
        true
    }

    fn get_channels(&self) -> &Vec<Channel> {
        &self.channels
    }

    fn get_in_buffer_queue(&self, key: &String) -> Arc<Mutex<VecDeque<Box<Bytes>>>> {
        let hm = &self.in_buffer_queues.read().unwrap();
        let v = hm.get(key).unwrap();
        v.clone()
    }

    fn get_out_buffer_queue(&self, key: &String) -> Arc<Mutex<VecDeque<Box<Bytes>>>> {
        let hm = &self.out_buffer_queues.read().unwrap();
        let v = hm.get(key).unwrap();
        v.clone()
    }

}

struct DataReader {
    name: String,
    channels: Vec<Channel>,
    in_buffer_queues: Arc<RwLock<HashMap<String, Arc<Mutex<VecDeque<Box<Bytes>>>>>>>,
    out_buffer_queues: Arc<RwLock<HashMap<String, Arc<Mutex<VecDeque<Box<Bytes>>>>>>>,
    out_message_queue: Arc<Mutex<VecDeque<Box<ChannelMessage>>>>,

    running: Arc<AtomicBool>,
}

impl DataReader {
    fn new(name: String, channels: Vec<Channel>) -> DataReader {
        let in_queues =  Arc::new(RwLock::new(HashMap::new()));
        let out_queues =  Arc::new(RwLock::new(HashMap::new()));

        for ch in &channels {
            in_queues.write().unwrap().insert(ch.get_channel_id().clone(), Arc::new(Mutex::new(VecDeque::new())));
            out_queues.write().unwrap().insert(ch.get_channel_id().clone(), Arc::new(Mutex::new(VecDeque::new())));
        }

        DataReader{
            name,
            channels,
            in_buffer_queues: in_queues,
            out_buffer_queues: out_queues,
            out_message_queue: Arc::new(Mutex::new(VecDeque::new())),
            running: Arc::new(AtomicBool::new(false))
        }
    }

    fn read_message(&self) -> Option<Box<ChannelMessage>> {
        // TODO set limit for backpressure
        let msg = self.out_message_queue.lock().unwrap().pop_front();
        if !msg.is_none() {
            let msg = msg.unwrap();
            Some(msg)
        } else {
            None
        }
    }

    fn start(&self) {
        // start dispatcher thread: takes message from out_buffer_queue, passes to deserializators pool, puts result in out_msg_queue
        self.running.store(true, Ordering::Relaxed);

        let this_out_message_queue = self.out_message_queue.clone();
        let this_out_buffer_queues = self.out_buffer_queues.clone();
        let this_runnning = self.running.clone();
        let f = move || {
            loop {
                let running = this_runnning.load(Ordering::Relaxed);
                if !running {
                    break;
                }
                // let msg_queues = this_out_message_queues.read().unwrap();
                let b_out_queues = this_out_buffer_queues.read().unwrap();
                for channel_id in  b_out_queues.keys() {
                    let mut locked_b_out_queue = b_out_queues.get(channel_id).unwrap().lock().unwrap();
                    let b = locked_b_out_queue.pop_front();
                    drop(locked_b_out_queue); // release lock
                    if !b.is_none() {
                        // TODO we should asynchronously dispatch to serde pool here, for simplicity we do sequential serde
                        let b = b.unwrap();
                        let s = String::from_utf8(*b).unwrap();
                        let msg: ChannelMessage = serde_json::from_str(&s).unwrap();
                        
                        // put to out msg queue
                        this_out_message_queue.lock().unwrap().push_back(Box::new(msg));
                    }
                }
            }
            
        };
        std::thread::spawn(f);
    }

    fn close (&self) {
        // TODO join thread
    }
}

impl IOHandler for DataReader {

    fn get_handler_type(&self) -> IOHandlerType {
        IOHandlerType::DataReader
    }

    fn on_send_ready(&self, channel_id: &String) -> bool {
        true
    }

    fn on_recv_ready(&self, channel_id: &String) -> bool {
        true
    }

    fn get_channels(&self) -> &Vec<Channel> {
        &self.channels
    }

    fn get_in_buffer_queue(&self, key: &String) -> Arc<Mutex<VecDeque<Box<Bytes>>>> {
        let hm = &self.in_buffer_queues.read().unwrap();
        let v = hm.get(key).unwrap();
        v.clone()
    }

    fn get_out_buffer_queue(&self, key: &String) -> Arc<Mutex<VecDeque<Box<Bytes>>>> {
        let hm = &self.out_buffer_queues.read().unwrap();
        let v = hm.get(key).unwrap();
        v.clone()
    }

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

struct IOLoop {
    handlers: Arc<Mutex<Vec<Arc<dyn IOHandler + Send + Sync>>>>,
    running: Arc<AtomicBool>,
    zmq_context: Arc<zmq::Context>,
    io_threads: Vec<JoinHandle<()>>,
    socket_meta_to_handler: Arc<RwLock<HashMap<SocketMetadata, Arc<dyn IOHandler + Send + Sync>>>>
}

impl IOLoop {

    fn new() -> IOLoop {
        let io_loop = IOLoop{
            handlers: Arc::new(Mutex::new(Vec::new())),
            running: Arc::new(AtomicBool::new(false)), 
            zmq_context: Arc::new(zmq::Context::new()),
            io_threads: Vec::new(),
            socket_meta_to_handler: Arc::new(RwLock::new(HashMap::new()))
        };
        io_loop
    }

    fn register_handler(&mut self, handler: Arc<dyn IOHandler + Send + Sync>) {
        self.handlers.lock().unwrap().push(handler);
    }

    fn start_io_threads(&mut self, num_threads: i16) {
        // since zmq::Sockets are not thread safe we will have a model where each socket can be polled by only 1 IO thread
        // each IO thread can have multiple sockets associated with it
        // let mut num_sockets = 0;
        let mut sockets_metadata = Vec::new();
        let this_handlers = self.handlers.clone();
        let locked_handlers = this_handlers.lock().unwrap();
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
                let mut sockets_meta = create_local_sockets_meta(channels, dir);
                sockets_metadata.append(&mut sockets_meta);
                for sm in sockets_meta {
                    self.socket_meta_to_handler.write().unwrap().insert(sm, handler.clone());
                }
            } else {
                panic!("Transfer Handlers are not implemented yet");
            }
        }
        drop(locked_handlers);

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

        for (_thread_id, sms) in sockets_meta_per_thread.iter() {

            let this_runnning = self.running.clone();
            let this_zmqctx = self.zmq_context.clone();
            let this_meta_to_handlers = self.socket_meta_to_handler.clone();
            let new_sms = sms.to_vec();

            let f = move |metas: &Vec<SocketMetadata>| {
                let mut socket_manager = SocketManager::new();
                socket_manager.create_sockets(&this_zmqctx, metas);
                socket_manager.bind_and_connect();
                let mut handlers = Vec::new();
                let m: std::sync::RwLockReadGuard<HashMap<SocketMetadata, Arc<dyn IOHandler + Send + Sync>>> = this_meta_to_handlers.read().unwrap();
                for i in 0..socket_manager.sockets_and_metas.len() {
                    let sm = socket_manager.sockets_and_metas[i].1.clone(); 
                    let handler = m.get(&sm).unwrap();
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
                                let in_queue = handler.get_in_buffer_queue(channel_id);
                                let mut l = in_queue.lock().unwrap();
                                l.push_back(Box::new(bytes));
                                drop(l); // release lock
                            }
                        }

                        if poll_list[i].is_writable() {
                            let ready = handler.on_send_ready(channel_id);
                            if ready {
                                // TODO for transfer handlers we should use peer ndoe id as key
                                let out_queue = handler.get_out_buffer_queue(channel_id);
                                
                                let mut l = out_queue.lock().unwrap();
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

            self.io_threads.push(std::thread::spawn(move || {f(&new_sms)}));
        }

        // thread_handles.into_iter().for_each(|c| c.join().unwrap());

    }
}

#[cfg(test)]
mod tests {
    use std::{sync::{Arc}, time::Duration};

    use crate::network::channel::{Channel, ChannelMessage};

    use super::{DataReader, DataWriter, IOLoop};


    #[test]
    fn test_io_loop() {
        let channel = Channel::Local { channel_id: String::from("ch_0"), ipc_addr: String::from("ipc:///tmp/ipc_0") };
        let data_reader = DataReader::new(
            String::from("data_reader"),
            vec![channel.clone()]
        );
        data_reader.start();
        let data_writer = DataWriter::new(
            String::from("data_reader"),
            vec![channel.clone()]
        );
        data_writer.start();
        let l_r = Arc::new(data_reader);
        let l_w = Arc::new(data_writer);
        let mut io_loop = IOLoop::new();
        io_loop.register_handler(l_r.clone());
        io_loop.register_handler(l_w.clone());
        io_loop.start_io_threads(1);
        let msg = ChannelMessage{key: String::from(""), value: String::from("")};
        l_w.clone().write_message(channel.get_channel_id(), msg);
        std::thread::sleep(Duration::from_millis(1000));
        let _msg = l_r.read_message();
        // assert!(_msg.is_some());
        std::thread::sleep(Duration::from_millis(1000));
        // TODO close loops

        print!("TEST OK");
    }

}