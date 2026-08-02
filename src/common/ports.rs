use std::collections::HashSet;
use std::net::TcpListener;
use std::sync::Mutex;

use lazy_static::lazy_static;

lazy_static! {
    static ref USED_PORTS: Mutex<HashSet<u16>> = Mutex::new(HashSet::new());
}

pub fn gen_unique_grpc_port() -> u16 {
    let mut used_ports = USED_PORTS.lock().unwrap();

    loop {
        let listener = TcpListener::bind("127.0.0.1:0")
            .expect("failed to ask the OS for an available test port");
        let port = listener
            .local_addr()
            .expect("test port listener has no local address")
            .port();
        if used_ports.insert(port) {
            return port;
        }
    }
}
