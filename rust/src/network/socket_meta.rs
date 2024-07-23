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

pub fn ipc_path_from_addr(ipc_addr: &String) -> String {
    let parts = ipc_addr.split("/");
    let suff = parts.last().unwrap();
    let end = ipc_addr.len() - suff.len();
    ipc_addr.get(6..end).unwrap().to_string()
}
