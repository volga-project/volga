use std::{collections::{HashMap, HashSet}, fs, rc::Rc, sync::{Arc, Mutex, RwLock}};

use super::{channel::Channel, io_loop::{Direction, IOHandler, IOHandlerType}};

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SocketOwner {
    TransferRemote,
    TransferLocal,
    Client
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SocketKind {
    Bind, // socket is used as a bind access point
    Connect // socket is used as a connecting client
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SocketMetadata {
    pub owner: SocketOwner,
    pub kind: SocketKind,
    pub channel_id: String,
    pub addr: String,
}

// TODO implement waiting for connection

// per-thread SocketsManager
pub struct SocketsManager {
    sockets_and_metas: Vec<(zmq::Socket, SocketMetadata)>
}

impl SocketsManager {

    pub fn new() -> SocketsManager {
        SocketsManager{sockets_and_metas: Vec::new()}
    }

    pub fn create_sockets(&mut self, zmq_context: &zmq::Context, socket_metas: &Vec<SocketMetadata>) {
        for sm in socket_metas {
            let socket = zmq_context.socket(zmq::PAIR).unwrap();
            self.sockets_and_metas.push((socket, sm.clone()));
        }
    }

    pub fn bind_and_connect(&mut self) {
        for (socket, sm) in &self.sockets_and_metas {
            if sm.kind == SocketKind::Bind {
                socket.bind(&sm.addr).unwrap();
            } else {
                socket.connect(&sm.addr).unwrap();
            }
        }
    }

    pub fn close_sockets(&mut self) {
        for (socket, _) in &self.sockets_and_metas {
            // TODO unbind/disconnect?
        }
    }

    pub fn get_sockets_and_metas(&self) -> &Vec<(zmq::Socket, SocketMetadata)> {
        &self.sockets_and_metas
    }
}

// global (for io loop) sockets metadata manager
pub struct SocketsMeatadataManager {
    socket_meta_to_handler: RwLock<HashMap<SocketMetadata, Arc<dyn IOHandler + Send + Sync>>>,
    _remote_node_ids: Mutex<HashSet<String>>
}

impl SocketsMeatadataManager {

    pub fn new() -> Self {
        SocketsMeatadataManager{socket_meta_to_handler: RwLock::new(HashMap::new()), _remote_node_ids: Mutex::new(HashSet::new())}
    }
    
    pub fn create_for_handlers(&self, handlers: &Vec<Arc<dyn IOHandler + Send + Sync>>) -> Vec<SocketMetadata> {
        let mut sockets_metadata = Vec::new();
        for handler in handlers.iter() {
            let handler_type = handler.get_handler_type();
            let channels = handler.get_channels();
            let dir;
            if (handler_type == IOHandlerType::DataWriter) | (handler_type == IOHandlerType::TransferSender) {
                dir = Direction::Sender;
            } else {
                dir = Direction::Receiver;
            }
            let sockets_meta;
            if (handler_type == IOHandlerType::DataWriter) | (handler_type == IOHandlerType::DataReader) {
                sockets_meta = SocketsMeatadataManager::create_local_sockets_meta(channels, dir);
            } else {
                sockets_meta = self.create_remote_transfer_sockets_meta(channels, dir);
            }

            for sm in sockets_meta {
                sockets_metadata.push(sm.clone());
                let mut locked_socket_meta_to_handler = self.socket_meta_to_handler.write().unwrap();
                if locked_socket_meta_to_handler.contains_key(&sm) {
                    panic!("Duplicate socket metadata");
                }
                locked_socket_meta_to_handler.insert(sm.clone(), handler.clone());
            }
        }
        sockets_metadata
    }

    pub fn get_handler_for_meta(&self, sm: &SocketMetadata) -> Arc<dyn IOHandler + Send + Sync> {
        self.socket_meta_to_handler.read().unwrap().get(sm).unwrap().clone()
    }

    // used for DataReader/DataWriter
    fn create_local_sockets_meta(channels: &Vec<Channel>, direction: Direction) -> Vec<SocketMetadata> {
        let mut v: Vec<SocketMetadata> = Vec::new();
        let is_reader = direction == Direction::Receiver;
        for channel in channels {
            match channel {
                Channel::Local{channel_id, ipc_addr} => {
                    let ipc_path = parse_ipc_path_from_addr(ipc_addr);
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
                    target_local_ipc_addr, 
                    ..
                } => {
                    let ipc_path = parse_ipc_path_from_addr(
                        if is_reader {target_local_ipc_addr} else {source_local_ipc_addr}
                    );
                    fs::create_dir_all(ipc_path).unwrap();
                    let socket_meta = SocketMetadata{
                        owner: SocketOwner::Client,
                        kind: if is_reader {SocketKind::Connect} else {SocketKind::Bind},
                        channel_id: channel_id.clone(),
                        addr: if is_reader {target_local_ipc_addr.clone()} else {source_local_ipc_addr.clone()},
                    };
                    v.push(socket_meta);
                }
            }
        }
        v
    }


    // used for RemoteTransferHandler (in any direction)
    fn create_remote_transfer_sockets_meta(&self, channels: &Vec<Channel>, direction: Direction) -> Vec<SocketMetadata> {
        let mut v: Vec<SocketMetadata> = Vec::new();
        let is_sender = direction == Direction::Sender;
        for channel in channels {
            match channel {
                Channel::Local{..} => {panic!("Remote Transfer should have no local channels")} 
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
                    let ipc_path;
                    let local_addr;
                    let local_socket_kind;

                    if is_sender {
                        ipc_path = parse_ipc_path_from_addr(source_local_ipc_addr);
                        local_addr = source_local_ipc_addr;
                        local_socket_kind = SocketKind::Connect;
                    } else {
                        ipc_path = parse_ipc_path_from_addr(target_local_ipc_addr);
                        local_addr = target_local_ipc_addr;
                        local_socket_kind = SocketKind::Bind;
                    }
                    fs::create_dir_all(ipc_path).unwrap();
                    let local_socket_metadata = SocketMetadata{
                        owner: SocketOwner::TransferLocal,
                        kind: local_socket_kind,
                        channel_id: channel_id.clone(),
                        addr: local_addr.clone()
                    };
                    v.push(local_socket_metadata);

                    let peer_node_id =  if is_sender {target_node_id} else {source_node_id};
                    let mut locked_remote_node_ids = self._remote_node_ids.lock().unwrap();
                    if locked_remote_node_ids.contains(peer_node_id) {
                        // already inited for this peer
                        continue;
                    }
                    locked_remote_node_ids.insert(peer_node_id.clone());

                    let tcp_addr;
                    let remote_socket_kind;
                    if is_sender {
                        tcp_addr = format!("tcp://{target_node_ip}:{port}");
                        remote_socket_kind = SocketKind::Connect;
                    } else {
                        tcp_addr = format!("tcp://0.0.0.0:{port}");
                        remote_socket_kind = SocketKind::Bind;
                    }
                    let remote_socket_metadata = SocketMetadata{
                        owner: SocketOwner::TransferRemote,
                        kind: remote_socket_kind,
                        channel_id: channel_id.clone(),
                        addr: tcp_addr.clone()
                    };

                    v.push(remote_socket_metadata);
                }
            }
        }
        v
    }
}


// TODO this should be in sync with Py's Channel ipc_addr format
fn parse_ipc_path_from_addr(ipc_addr: &String) -> String {
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