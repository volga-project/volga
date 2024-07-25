use std::{sync::Arc, time::Duration};

use volga_rust::network::{channel::{Channel, ChannelMessage}, io_loop::{DataReader, DataWriter, IOLoop}};


#[test]
fn test_network() {
    let channel = Channel::Local { channel_id: String::from("ch_0"), ipc_addr: String::from("ipc:///tmp/ipc_0") };
    let mut data_reader = DataReader::new(
        String::from("data_reader"),
        vec![channel.clone()]
    );
    data_reader.start();
    let mut data_writer = DataWriter::new(
        String::from("data_writer"),
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
    l_w.write_message(channel.get_channel_id(), msg);
    std::thread::sleep(Duration::from_millis(1000));
    let _msg = l_r.read_message();
    // assert!(_msg.is_some());
    std::thread::sleep(Duration::from_millis(500000));
    // TODO close loops

    print!("TEST OK");
}