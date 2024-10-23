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
    // socket_to_in_chan: HashMap<String, (Sender<SocketMessage>, Receiver<SocketMessage>)>,
    // socket_to_out_chan: HashMap<String, (Sender<SocketMessage>, Receiver<SocketMessage>)>,
    sockets: Vec<(zmq::Socket, SocketMetadata)>,
    subscribers: Vec<Arc<dyn SocketServiceSubscriber + Send + Sync>>,
    zmq_context: Arc<zmq::Context>,
    zmq_config: Option<ZmqConfig>
}

impl SocketManager {

    pub fn new(subscribers: Vec<Arc<dyn SocketServiceSubscriber + Send + Sync>>, zmq_context: Arc<zmq::Context>, zmq_config: Option<ZmqConfig>) -> Self {
        SocketManager{socket_to_subscriber: HashMap::new(), sockets: Vec::new(), subscribers, zmq_context, zmq_config}
    }

    // fn gen_socket_identity(&mut self, subscriber_id: &String) -> String {
    //     if self.socket_identity_generator.contains_key(subscriber_id) {
    //         let count = self.socket_identity_generator.get(subscriber_id).unwrap();
    //         let socket_identity = format!("{subscriber_id}-{count}");
    //         self.socket_identity_generator.insert(subscriber_id.clone(), count + 1);
    //         return socket_identity
    //     } else {
    //         let socket_identity = format!("{subscriber_id}-0");
    //         self.socket_identity_generator.insert(subscriber_id.clone(), 1);
    //         return socket_identity
    //     }
    // }

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

    // fn create_socket_metas_for_subscriber(&mut self, subscriber: &Arc<dyn SocketServiceSubscriber + Send + Sync>) -> Vec<SocketMetadata> {
    //     let mut socket_metas = Vec::new();
    //     let subscriber_type = subscriber.get_type();
    //     let subscriber_id = subscriber.get_id();
    //     let subscriber_name = subscriber.get_name();
    //     let channels = subscriber.get_channels();
    //     let (local_channels, remote_channels) = to_local_and_remote(channels);

    //     if subscriber_type == SocketServiceSubscriberType::DataWriter {
    //         // one DEALER per local channel, one DEALER for all remote channels
    //         for channel in local_channels {
    //             match channel {
    //                 Channel::Local{channel_id, ipc_addr} => {
    //                     let ipc_path = parse_ipc_path_from_addr(&ipc_addr);
    //                     fs::create_dir_all(ipc_path).unwrap();
    //                     let local_socket_meta = SocketMetadata{
    //                         identity: self.gen_socket_identity(&subscriber_id),
    //                         owner_id: subscriber_id.clone(),
    //                         kind: SocketKind::Dealer,
    //                         channel_ids: vec![channel_id.clone()],
    //                         addr: ipc_addr.clone(),
    //                     };
    //                     socket_metas.push(local_socket_meta);
    //                 },
    //                 Channel::Remote{..} => {panic!("only local")}
    //             }
    //         }

    //         if remote_channels.len() != 0 {
    //             // assert all remote channel source ipc_addr are the same
    //             let mut ipc_addrs: HashSet<String> = HashSet::new();
    //             for channel in &remote_channels {
    //                 match channel {
    //                     Channel::Local{..} => {panic!("only remote")},
    //                     Channel::Remote { channel_id, source_local_ipc_addr, ..} => {
    //                         ipc_addrs.insert(source_local_ipc_addr.clone());       
    //                     }
    //                 }
    //             }

    //             if ipc_addrs.len() != 1 {
    //                 panic!("Duplicate ipc addrs for {subscriber_name}")
    //             }
    //             let ipc_addr = ipc_addrs.iter().next().unwrap().clone();
    //             let ipc_path = parse_ipc_path_from_addr(&ipc_addr);
    //             fs::create_dir_all(ipc_path).unwrap();

    //             let remote_socket_meta = SocketMetadata{
    //                 identity: self.gen_socket_identity(&subscriber_id),
    //                 owner_id: subscriber_id.clone(),
    //                 kind: SocketKind::Dealer,
    //                 channel_ids: remote_channels.iter().map(|channel| channel.get_channel_id().clone()).collect(),
    //                 addr: ipc_addr,
    //             };
    //             socket_metas.push(remote_socket_meta);
    //         }
    //     } else if subscriber_type == SocketServiceSubscriberType::DataReader {
    //         // one ROUTER for all channels (both local and remote)
    //         let mut ipc_addrs: HashSet<String> = HashSet::new();
    //         for channel in channels {
    //             match channel {
    //                 Channel::Local{channel_id, ipc_addr} => {
    //                     ipc_addrs.insert(ipc_addr.clone());       
    //                 },
    //                 Channel::Remote { channel_id, source_local_ipc_addr, source_node_ip, source_node_id, target_local_ipc_addr, .. } => {
    //                     ipc_addrs.insert(target_local_ipc_addr.clone());       
    //                 }
    //             }
    //         }

    //         if ipc_addrs.len() != 1 {
    //             panic!("Duplicate ipc addrs for {subscriber_name}")
    //         }
    //         let ipc_addr = ipc_addrs.iter().next().unwrap().clone();
    //         let ipc_path = parse_ipc_path_from_addr(&ipc_addr);
    //         fs::create_dir_all(ipc_path).unwrap();

    //         let socket_meta = SocketMetadata{
    //             identity: self.gen_socket_identity(&subscriber_id),
    //             owner_id: subscriber_id.clone(),
    //             kind: SocketKind::Router,
    //             channel_ids: channels.iter().map(|channel| channel.get_channel_id().clone()).collect(),
    //             addr: ipc_addr,
    //         };
    //         socket_metas.push(socket_meta);
    //     } else if subscriber_type == SocketServiceSubscriberType::TransferSender {
    //         if remote_channels.len() != channels.len() {
    //             panic!("{subscriber_name} should have only remote channels")
    //         }

    //         // one ROUTER to receive from all local inputs, one DEALER to send per remote node, 
    //         let mut remote_channels_per_node_id: HashMap<String, Vec<Channel>> = HashMap::new();
    //         let mut ipc_addrs: HashSet<String> = HashSet::new();
    //         for channel in &remote_channels {
    //             match channel {
    //                 Channel::Local{..} => {panic!("only remote")},
    //                 Channel::Remote { channel_id, source_local_ipc_addr, source_node_ip, source_node_id, target_local_ipc_addr, target_node_ip, target_node_id, port } => {
    //                     if remote_channels_per_node_id.contains_key(target_node_id) {
    //                         remote_channels_per_node_id.get_mut(target_node_id).unwrap().push(channel.clone());
    //                     } else {
    //                         remote_channels_per_node_id.insert(target_node_id.clone(), vec![channel.clone()]);
    //                     }
    //                     ipc_addrs.insert(source_local_ipc_addr.clone());
    //                 }
    //             }
    //         }

    //         if ipc_addrs.len() != 1 {
    //             panic!("Duplicate ipc addrs for {subscriber_name}")
    //         }
    //         let ipc_addr = ipc_addrs.iter().next().unwrap().clone();
    //         let ipc_path = parse_ipc_path_from_addr(&ipc_addr);
    //         fs::create_dir_all(ipc_path).unwrap();
    //         let router_socket_meta = SocketMetadata{
    //             identity: self.gen_socket_identity(&subscriber_id),
    //             owner_id: subscriber_id.clone(),
    //             kind: SocketKind::Router,
    //             channel_ids: remote_channels.iter().map(|channel| channel.get_channel_id().clone()).collect(),
    //             addr: ipc_addr,
    //         };
    //         socket_metas.push(router_socket_meta);

    //         for (_, node_channels) in remote_channels_per_node_id {
    //             let mut ports = HashSet::new();
    //             let mut target_node_ips = HashSet::new();
    //             let mut channel_ids = vec![];
    //             for channel in node_channels {
    //                 match channel {
    //                     Channel::Local{..} => {panic!("only remote")},
    //                     Channel::Remote { channel_id, source_local_ipc_addr, source_node_ip, source_node_id, target_local_ipc_addr, target_node_ip, target_node_id, port } => {
    //                         ports.insert(port);
    //                         channel_ids.push(channel_id);   
    //                         target_node_ips.insert(target_node_ip);
    //                     }
    //                 }
    //             }

    //             if ports.len() != 1 {
    //                 panic!("Duplicate ports for {subscriber_name}")
    //             }
    //             let port = ports.iter().next().unwrap();

    //             if target_node_ips.len() != 1 {
    //                 panic!("Duplicate target_node_ip for {subscriber_name}")
    //             }
    //             let target_node_ip = target_node_ips.iter().next().unwrap();

    //             let addr = format!("tcp://{target_node_ip}:{port}");
    //             let dealer_socket_meta = SocketMetadata{
    //                 identity: self.gen_socket_identity(&subscriber_id),
    //                 owner_id: subscriber_id.clone(),
    //                 kind: SocketKind::Dealer,
    //                 channel_ids: channel_ids,
    //                 addr: addr,
    //             };    
    //             socket_metas.push(dealer_socket_meta);
    //         }
    //     } else if subscriber_type == SocketServiceSubscriberType::TransferReceiver {
    //         if remote_channels.len() != channels.len() {
    //             panic!("{subscriber_name} should have only remote channels")
    //         }

    //         // one ROUTER to receive from all remote connection, one DEALER per local connection to send, 
    //         let mut ports = HashSet::new();
    //         for channel in &remote_channels {
    //             match channel {
    //                 Channel::Local{..} => {panic!("only remote")},
    //                 Channel::Remote { channel_id, source_local_ipc_addr, source_node_ip, source_node_id, target_local_ipc_addr, target_node_ip, target_node_id, port } => {
    //                     ports.insert(port);
    //                     let dealer_socket_meta = SocketMetadata{
    //                         identity: self.gen_socket_identity(&subscriber_id),
    //                         owner_id: subscriber_id.clone(),
    //                         kind: SocketKind::Dealer,
    //                         channel_ids: vec![channel_id.clone()],
    //                         addr: target_local_ipc_addr.clone(),
    //                     };
    //                     socket_metas.push(dealer_socket_meta);
    //                 }
    //             }
    //         }

    //         if ports.len() != 1 {
    //             panic!("Duplicate ports for {subscriber_name}")
    //         }
    //         let port = ports.iter().next().unwrap();
    //         let addr = format!("tcp://0.0.0.0:{port}");

    //         let router_socket_meta = SocketMetadata{
    //             identity: self.gen_socket_identity(&subscriber_id),
    //             owner_id: subscriber_id.clone(),
    //             kind: SocketKind::Router,
    //             channel_ids: remote_channels.iter().map(|channel| channel.get_channel_id().clone()).collect(),
    //             addr: addr,
    //         };
    //         socket_metas.push(router_socket_meta);
    //     } else {
    //         panic!("Unknown SocketServiceSubscriberType");
    //     }
    //     socket_metas
    // }
    
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