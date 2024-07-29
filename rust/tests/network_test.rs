use std::{collections::HashMap, rc::Rc, sync::Arc, time::{Duration, SystemTime}};

use volga_rust::network::{channel::{Channel}, data_reader::DataReader, data_writer::DataWriter, io_loop::IOLoop, utils::random_string};

use rmpv::decode;
use rmpv::Value;
use std::io::Cursor;


#[test]
fn test_one_to_one() {
    let channel = Channel::Local { channel_id: String::from("ch_0"), ipc_addr: String::from("ipc:///tmp/ipc_0") };
    let data_reader = DataReader::new(
        String::from("data_reader"),
        vec![channel.clone()],
    );
    data_reader.start();
    let data_writer = DataWriter::new(
        String::from("data_writer"),
        vec![channel.clone()],
    );
    data_writer.start();
    let l_r = Arc::new(data_reader);
    let l_w = Arc::new(data_writer);

    let io_loop = IOLoop::new();
    io_loop.register_handler(l_r.clone());
    io_loop.register_handler(l_w.clone());
    io_loop.start_io_threads(1);

    let num_msgs = 1000;
    let payload_size = 128;

    let data_alloc_start_ts = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis();
    let mut to_send = vec![];
    for i in 0..num_msgs {
        let mut msg = HashMap::new();
        msg.insert("key", i.to_string());
        msg.insert("value", random_string(payload_size));
        to_send.push(Box::new(bincode::serialize(&msg).unwrap().to_vec()));
    }

    let to_send = Arc::new(to_send);
    let data_alloc_time = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis()- data_alloc_start_ts;
    println!("Data allocated in (ms): {data_alloc_time}");

    let moved_w = l_w.clone();
    let start_ts = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis();
    let local_to_send = to_send.clone();
    let j_handle = std::thread::spawn(move|| {
        let mut backp = 0;
        for msg in local_to_send.as_ref() {
            backp += moved_w.write_bytes(channel.get_channel_id(), msg.clone(), 1000, 0).unwrap();
        }
        backp
    });
    
    let mut recvd = vec![];

    while recvd.len() != to_send.len() {
        let _msg = l_r.read_bytes();
        if _msg.is_some() {
            recvd.push(_msg.unwrap());
        }
    }
    let total_ms = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() - start_ts;
    let backp_s = j_handle.join().unwrap()/1000;
    println!("Transfered in (ms): {total_ms}");
    println!("Backpressure (ms): {backp_s}");
    l_r.close();
    l_w.close();
    io_loop.close();
    assert_eq!(to_send.len(), recvd.len());
    for i in 0..to_send.len() {
        assert_eq!(to_send[i], recvd[i])
    }

    println!("TEST OK");

}
