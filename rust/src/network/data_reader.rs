// use std::iter::Map;
use std::collections::HashMap;
use zmq;
// use 

use super::channel::Channel;

/// https://github.com/erickt/rust-zmq/blob/master/examples/msgsend/main.rs

// struct DataReader {
//     name: String,
//     zmq_context: zmq::Context,
//     channels: Vec<Box<dyn Channel>>,
//     sockets: HashMap<String, zmq::Socket>,
//     running: bool
// }



#[cfg(test)]
mod tests {

    #[test]
    fn test_dr() {
        assert_eq!(4, 4);
    }
}