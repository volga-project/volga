use std::{collections::HashMap, sync::Arc};

use crossbeam::channel::{Receiver, Sender};

use super::socket_service::{SocketMessage, SocketServiceSubscriber, ZmqConfig};

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SocketKind {
    Router, // maps to zmq::ROUTER - we bind these
    Dealer //  maps to zmq::DEALER - we connect with these
}

// TODO description
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SocketMetadata {
    pub identity: String,
    pub owner_id: String,
    pub kind: SocketKind,
    pub channel_ids: Vec<String>,
    pub addr: String,
}

pub struct SocketIdentityGenerator {
    owner_id: String,
    last_socket_id: usize
}

impl SocketIdentityGenerator {
    pub fn new(owner_id: String) -> SocketIdentityGenerator{
        SocketIdentityGenerator{owner_id, last_socket_id: 0}
    }

    pub fn gen(&mut self) -> String {
        let owner_id = self.owner_id.clone();
        let count = self.last_socket_id;
        let socket_identity = format!("{owner_id}-{count}");
        self.last_socket_id = count + 1;
        socket_identity
    }
}


// TODO description
pub struct SocketManager {
    socket_to_subscriber: HashMap<String, Arc<dyn SocketServiceSubscriber + Send + Sync>>,
    sockets: Vec<(zmq::Socket, SocketMetadata)>,
    subscribers: Vec<Arc<dyn SocketServiceSubscriber + Send + Sync>>,
    zmq_context: Arc<zmq::Context>,
    zmq_config: Option<ZmqConfig>
}

impl SocketManager {

    pub fn new(subscribers: Vec<Arc<dyn SocketServiceSubscriber + Send + Sync>>, zmq_context: Arc<zmq::Context>, zmq_config: Option<ZmqConfig>) -> Self {
        SocketManager{socket_to_subscriber: HashMap::new(), sockets: Vec::new(), subscribers, zmq_context, zmq_config}
    }

    fn create_socket(&self, socket_meta: &SocketMetadata) -> zmq::Socket {
        let socket;
        if socket_meta.kind == SocketKind::Dealer {
            socket = self.zmq_context.socket(zmq::DEALER).unwrap();
        } else if socket_meta.kind == SocketKind::Router {
            socket = self.zmq_context.socket(zmq::ROUTER).unwrap();
            socket.set_router_mandatory(true).unwrap();
        } else {
            panic!("Unknown socket kind")
        }
        socket.set_identity(socket_meta.identity.as_bytes()).expect("failed setting socket identity");
        if self.zmq_config.is_some() {
            let config = self.zmq_config.as_ref().unwrap();
            if config.sndbuf.is_some() {
                socket.set_sndbuf(config.sndbuf.unwrap()).unwrap();
            }
            if config.rcvbuf.is_some() {
                socket.set_rcvbuf(config.rcvbuf.unwrap()).unwrap();
            }
            if config.sndhwm.is_some() {
                socket.set_sndhwm(config.sndhwm.unwrap()).unwrap();
            }
            if config.rcvhwm.is_some() {
                socket.set_rcvhwm(config.rcvhwm.unwrap()).unwrap();
            }
            if config.linger.is_some() {
                socket.set_linger(config.linger.unwrap()).unwrap();
            }
            if config.connect_timeout_s.is_some() {
                socket.set_connect_timeout(config.connect_timeout_s.unwrap()).unwrap();
            }
        }
        socket
    }
    
    pub fn create_sockets(&mut self) {
        let subscribers = self.subscribers.clone();
        for subscriber in subscribers {
            let metas = subscriber.get_sockets_metas();
            // let in_chan = subscriber.get_in_chan();
            // let out_chan = subscriber.get_out_chan();
            for socket_meta in metas {
                let socket = self.create_socket(&socket_meta);
                let socket_identity = &socket_meta.identity;
                if self.socket_to_subscriber.contains_key(socket_identity) {
                    panic!("Duplicate socket identity {socket_identity}");
                }
                self.socket_to_subscriber.insert(socket_identity.clone(), subscriber.clone());
                self.sockets.push((socket, socket_meta.clone()));
            }
        }
    }

    pub fn get_sockets(&self) -> &Vec<(zmq::Socket, SocketMetadata)> {
        &self.sockets
    }

    pub fn bind_and_connect(&self) {
        for (socket, sm) in &self.sockets {
            if sm.kind == SocketKind::Router {
                // TODO handle Address already in use
                let b = socket.bind(&sm.addr);
                if b.is_err() {
                    let addr =  &sm.addr;
                    let err = b.err().unwrap().message();
                    panic!("Unable to bind addr {addr}: {err}")
                }
            } else {
                socket.connect(&sm.addr).unwrap();
            }
        }
    }

    pub fn get_subscriber_in_sender(&self, sm: &SocketMetadata) -> Sender<SocketMessage> {
        let subscriber = self.socket_to_subscriber.get(&sm.identity).unwrap();
        subscriber.get_in_sender(sm)
    }

    pub fn get_subscriber_out_receiver(&self, sm: &SocketMetadata) -> Receiver<SocketMessage> {
        let subscriber = self.socket_to_subscriber.get(&sm.identity).unwrap();
        subscriber.get_out_receiver(sm)
    }
}

pub fn channels_to_socket_identities(socket_metas: Vec<SocketMetadata>) -> HashMap<String, String> {
    let mut res = HashMap::new();
    for sm in socket_metas {
        for channel_id in sm.channel_ids {
            if res.contains_key(&channel_id) {
                panic!("Duplicate channel id {channel_id}");
            }
            res.insert(channel_id.clone(), sm.identity.clone());
        }
    }
    res
}

// TODO this should be in sync with Py's Channel ipc_addr format
pub fn parse_ipc_path_from_addr(ipc_addr: &String) -> String {
    let parts = ipc_addr.split("/");
    let last = parts.last();
    if last.is_none() {
        panic!("Malformed ipc addr: {ipc_addr}");
    } 
    let suff = last.unwrap();
    let end = ipc_addr.len() - suff.len();
    let path = ipc_addr.get(6..end);
    if path.is_none() {
        panic!("Malformed ipc addr: {ipc_addr}");
    }
    path.unwrap().to_string()
}


#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_parse_ipc_path() {
        let ipc_addr = String::from("ipc:///tmp/source_local_0");
        let res = parse_ipc_path_from_addr(&ipc_addr);
        let expected = String::from("/tmp/");
        assert_eq!(res, expected);
    }
}