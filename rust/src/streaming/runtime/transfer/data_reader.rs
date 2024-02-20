use std::iter::Map;
use std::collections::HashMap;
use zmq;
use cha

/// https://github.com/erickt/rust-zmq/blob/master/examples/msgsend/main.rs

struct DataReader {
    name: &str,
    zmq_context: &mut zmq::Context,
    channels: &Vec<(&str, &str, u32)>,
    sockets: HashMap<&str, &zmq::Socket>,
    cur_read_id: u32,
    running: bool
}

impl DataReader {

    fn new(name: &str, channels: Vec<(&str, &str, u32)>) -> DataReader {
        let mut ctx = zmq::Context::new();
        let mut sockets = HashMap::new();
        for channel_info in channels {
            let channel_id = channel_info.0;
            let source_ip = channel_info.1;
            let source_port = channel_info.2;
            let pull_socket = ctx.socket(zmq::PULL).unwrap();
            let addr = format!("tcp://{source_ip}:{source_port}");
            /// TODO pull_socket.setsockopt(zmq::LINGER, 0)
            pull_socket.connect(addr).unwrap();
            sockets.insert(channel_info[0], pull_socket)
        }

        DataReader {
            name: name,
            zmq_context: ctx,
            channels: &channels,
            sockets: &sockets,
            cur_read_id: 0,
            running: true,
        }
    }

    fn read_message(&self) -> ChannelMessage {
        let channel_id = &self.channels[&self.cur_read_id].0;
        let socket = &self.sockets[channel_id];
        let json_str: Option<&str> = None;

        while &self.running && json_str.is_none() {
            json_str = socket.recv_string(zmq::NOBLOCK)
        }

        let msg: ChannelMessage = serde_json::from_str(data)?;
        msg
    }

    fn close() {
        /// TODO
    }
}