
use std::{collections::HashMap, sync::{Arc, RwLock}, time::{SystemTime, UNIX_EPOCH}};

use volga_rust::network_deprecated::{buffer_utils::{dummy_bytes, get_buffer_id}, channel::Channel, data_reader::DataReader, data_writer::{self, DataWriter}, io_loop::{Direction, IOHandler, IOLoop}, network_config::NetworkConfig, remote_transfer_handler::RemoteTransferHandler, utils::random_string};


#[test]
fn test_one_to_one_local_deprecated() {
    test_one_to_one(true);
}

#[test]
fn test_one_to_one_remote_deprecated() {
    test_one_to_one(false);
}

// TODO add unreliable channel test (out-of-orders, drops and duplicates)
// TODO add transfer handler disconnect/reconnect test 
// TODO test in-flight resends
// TODO test backpressure

fn test_one_to_one(local: bool) {
    let network_config = NetworkConfig::new("/Users/anov/IdeaProjects/volga/rust/tests/default_network_config_deprecated.yaml");

    let channel;
    if local {
        channel = Channel::Local { 
            channel_id: String::from("local_ch_0"), 
            ipc_addr: String::from("ipc:///tmp/ipc_0") 
        };
    } else {
        channel = Channel::Remote { 
            channel_id: String::from("remote_ch_0"),
            source_local_ipc_addr: String::from("ipc:///tmp/source_local_0"), 
            source_node_ip: String::from("127.0.0.1"), 
            source_node_id: String::from("node_1"), 
            target_local_ipc_addr: String::from("ipc:///tmp/target_local_0"), 
            target_node_ip: String::from("127.0.0.1"), 
            target_node_id: String::from("node_2"), 
            port: 1234 
        }
    }
    let now_ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
    let job_name = format!("job-{now_ts}");
    let data_reader = Arc::new(DataReader::new(
        String::from("data_reader"),
        job_name.clone(),
        network_config.data_reader,
        vec![channel.clone()],
    ));
    let data_writer = Arc::new(DataWriter::new(
        String::from("data_writer"),
        job_name.clone(),
        network_config.data_writer,
        vec![channel.clone()],
    ));

    let mut remote_transfer_handlers = Vec::new();

    let io_loop = IOLoop::new(String::from("io_loop"), network_config.zmq);
    io_loop.register_handler(data_reader.clone());
    io_loop.register_handler(data_writer.clone());
    if !local {
        let transfer_sender = Arc::new(RemoteTransferHandler::new(
            String::from("transfer_sender"),
            job_name.clone(),
            vec![channel.clone()],
            network_config.transfer.clone(),
            Direction::Sender
        ));
        let transfer_receiver = Arc::new(RemoteTransferHandler::new(
            String::from("transfer_receiver"),
            job_name.clone(),
            vec![channel.clone()],
            network_config.transfer.clone(),
            Direction::Receiver
        ));
        io_loop.register_handler(transfer_sender.clone());
        io_loop.register_handler(transfer_receiver.clone());
        remote_transfer_handlers.push(transfer_sender.clone());
        remote_transfer_handlers.push(transfer_receiver.clone());
        transfer_sender.start();
        transfer_receiver.start();
    }

    data_reader.start();
    data_writer.start();

    let err = io_loop.connect(1, 5000);
    if err.is_some() {
        let err = err.unwrap();
        panic!("{err}")
    }
    io_loop.start();

    let num_msgs = 300000;
    let payload_size = 128;

    let data_alloc_start_ts = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis();
    let mut to_send = vec![];
    for i in 0..num_msgs {
        let b = dummy_bytes(i as u32, channel.get_channel_id(), payload_size);
        to_send.push(b);
    }

    let to_send = Arc::new(to_send);
    let data_alloc_time = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() - data_alloc_start_ts;
    println!("Data allocated in (ms): {data_alloc_time}");

    let moved_data_writer: Arc<DataWriter> = data_writer.clone();
    let start_ts = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis();
    let local_to_send = to_send.clone();
    let j_handle = std::thread::spawn(move|| {
        let mut backp = 0;
        let ch_id = channel.get_channel_id();
        let max_retries = 5;
        let write_timeout_ms = 1000;
        for b in local_to_send.as_ref() {
            let mut write_res = moved_data_writer.write_bytes(ch_id, b.clone(), write_timeout_ms);
            let mut r = 0;
            while write_res.is_none() {
                if r > max_retries {
                    panic!("Max retries");
                }
                r += 1;
                backp += write_timeout_ms as u128;
                write_res = moved_data_writer.write_bytes(ch_id, b.clone(), write_timeout_ms);
            }
            backp += write_res.unwrap();
            let buffer_id = get_buffer_id(b.clone());
            if buffer_id%1000 == 0 {
                println!("Sent {buffer_id}");
            }
        }
        backp
    });
    
    let mut recvd = vec![];

    while recvd.len() != to_send.len() {
        let b = data_reader.read_bytes();
        if b.is_some() {
            let b = b.unwrap();
            recvd.push(b.clone());
            let buffer_id = get_buffer_id(b.clone());
            if buffer_id%1000 == 0 {
                println!("Rcvd {buffer_id}");
            }
            // thread::sleep(time::Duration::from_millis(100));
        }
    }
    
    let total_ms = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() - start_ts;
    let backp_ms = j_handle.join().unwrap();
    let throughput = ((num_msgs as f64)/(total_ms as f64) * 1000.0) as u16;
    println!("Transfered in (ms): {total_ms}");
    println!("Backpressure (ms): {backp_ms}");
    println!("Throughput (msg/s): {throughput}");
    
    data_reader.close();
    data_writer.close();
    if !local {
        while remote_transfer_handlers.len() != 0 {
            remote_transfer_handlers.pop().unwrap().close();
        }
    }

    io_loop.close();
    assert_eq!(to_send.len(), recvd.len());
    for i in 0..to_send.len() {
        assert_eq!(to_send[i], recvd[i])
    }

    println!("TEST OK");
}

#[test]
fn test_one_to_n_local_deprecated() {
    test_one_to_n(true, 10); // TODO n >= 8 sometimes locks, why? - because we need to notify sender when receiver's que is unlocked after being full
}

#[test]
fn test_one_to_n_remote_deprecated() {
    test_one_to_n(false, 10);
}

fn test_one_to_n(local: bool, n: i32) {

    let now_ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
    let job_name = format!("job-{now_ts}");
    let network_config = NetworkConfig::new("/Users/anov/IdeaProjects/volga/rust/tests/default_network_config_deprecated.yaml");
    let mut channels = vec![];
    let mut data_readers = HashMap::new();
    let mut base_port = 2345;
    for i in 0..n {
        let channel;
        let channel_id;
        if local {
            channel_id = format!("local_ch_{i}");
            channel = Channel::Local { 
                channel_id: channel_id.clone(), 
                ipc_addr: format!("ipc:///tmp/ipc_{i}") 
            };
        } else {
            channel_id = format!("remote_ch_{i}");
            channel = Channel::Remote { 
                channel_id: channel_id.clone(),
                source_local_ipc_addr: format!("ipc:///tmp/source_local_{i}"), 
                source_node_ip: String::from("127.0.0.1"), 
                source_node_id: format!("source_node_{i}"), // TODO this does not properly configure sockets if source_node_id is same, why?
                target_local_ipc_addr: format!("ipc:///tmp/target_local_{i}"), 
                target_node_ip: String::from("127.0.0.1"), 
                target_node_id: format!("target_node_{i}"), 
                port: base_port 
            };
            base_port += 1;
        }
        channels.push(channel.clone());
        let data_reader = Arc::new(DataReader::new(
            String::from("data_reader"),
            job_name.clone(),
            network_config.data_reader.clone(),
            vec![channel.clone()],
        ));
        data_readers.insert(channel_id.clone(), data_reader);
    }
    
    let data_readers = Arc::new(RwLock::new(data_readers));

    let data_writer = Arc::new(DataWriter::new(
        String::from("data_writer"),
        job_name.clone(),
        network_config.data_writer,
        channels.to_vec(),
    ));

    let mut remote_transfer_handlers = Vec::new();

    let io_loop = IOLoop::new(String::from("io_loop"), network_config.zmq);
    for (_, data_reader) in data_readers.read().unwrap().iter() {
        io_loop.register_handler(data_reader.clone());
    }
    io_loop.register_handler(data_writer.clone());
    if !local {
        let transfer_sender = Arc::new(RemoteTransferHandler::new(
            String::from("transfer_sender"),
            job_name.clone(),
            channels.to_vec(),
            network_config.transfer.clone(),
            Direction::Sender
        ));
        for channel in channels.to_vec() { 
            let ch_id = channel.get_channel_id().clone();
            let transfer_receiver = Arc::new(RemoteTransferHandler::new(
                format!("transfer_receiver_{ch_id}"),
                job_name.clone(),
                vec![channel.clone()],
                network_config.transfer.clone(),
                Direction::Receiver
            ));
            io_loop.register_handler(transfer_receiver.clone());
            transfer_receiver.start();
            remote_transfer_handlers.push(transfer_receiver.clone());
        }
        io_loop.register_handler(transfer_sender.clone());
        remote_transfer_handlers.push(transfer_sender.clone());
        transfer_sender.start();
    }

    for (_, data_reader) in data_readers.read().unwrap().iter() {
        data_reader.start();
    }
    data_writer.start();

    let err = io_loop.connect(1, 5000);
    if err.is_some() {
        let err = err.unwrap();
        panic!("{err}")
    }
    io_loop.start();

    let num_msgs_per_channel = 10000;
    let payload_size = 128;

    let data_alloc_start_ts = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis();
    let mut to_send_per_channel_map = HashMap::new();
    for channel in &channels {
        let mut to_send = vec![];
        for i in 0..num_msgs_per_channel {
            let b = dummy_bytes(i as u32, channel.get_channel_id(), payload_size);
            to_send.push(b);
        }
        to_send_per_channel_map.insert(channel.get_channel_id().clone(), to_send);
    }

    let to_send_per_channel = Arc::new(RwLock::new(to_send_per_channel_map));
    let data_alloc_time = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis()- data_alloc_start_ts;
    println!("Data allocated in (ms): {data_alloc_time}");

    let moved_data_writer: Arc<DataWriter> = data_writer.clone();
    let start_ts = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis();
    let local_channels = channels.to_vec();
    let local_to_send_per_channel = to_send_per_channel.clone();
    let writer_handle = std::thread::spawn(move|| {
        let mut backp = 0;

        let mut num_sent = 0;
        let mut cur_channel_index = 0;
        let mut indexes_per_channel = HashMap::new();
        for channel in &local_channels {
            indexes_per_channel.insert(channel.get_channel_id(), 0);
        }

        let locked = local_to_send_per_channel.read().unwrap();
        let max_retries = 5;
        let write_timeout_ms = 1000;
        while num_sent != num_msgs_per_channel * &local_channels.len() {
            let channel = &local_channels[cur_channel_index];
            let ch_id = &channel.get_channel_id();
            let index = indexes_per_channel[*ch_id];
            let b = &locked.get(*ch_id).unwrap()[index];
            let buffer_id = get_buffer_id(b.clone());
            let mut write_res = moved_data_writer.write_bytes(ch_id, b.clone(), write_timeout_ms);
            let mut r = 0;
            while write_res.is_none() {
                if r > max_retries {
                    panic!("Max retries");
                }
                r += 1;
                backp += write_timeout_ms as u128;
                write_res = moved_data_writer.write_bytes(ch_id, b.clone(), write_timeout_ms);
            }
            backp += write_res.unwrap();

            if buffer_id%1000 == 0 {
                println!("[{ch_id}] Sent {buffer_id}");   
            }
            num_sent += 1;
            indexes_per_channel.insert(&ch_id, index + 1);
            cur_channel_index = (cur_channel_index + 1)%&local_channels.len();
        }
        backp
    });

    let mut reader_handles: Vec<std::thread::JoinHandle<()>> = vec![];
    for channel in &channels {
        let local_to_send_per_channel = to_send_per_channel.clone();
        let local_data_readers = data_readers.clone();
        let ch_id = channel.get_channel_id().clone();
        let reader_handle = std::thread::spawn(move|| {
            let local_data_reader = local_data_readers.read().unwrap().get(&ch_id).unwrap().clone();
            let locked = local_to_send_per_channel.read().unwrap();
            let local_to_send = locked.get(&ch_id).unwrap();
    
            let mut recvd = vec![];
            
            while recvd.len() != num_msgs_per_channel {
                let b = local_data_reader.read_bytes();
                if b.is_some() {
                    let b = b.unwrap();
                    recvd.push(b.clone());
                    let buffer_id = get_buffer_id(b.clone());
                    if buffer_id%1000 == 0 {
                        println!("[{ch_id}] Rcvd {buffer_id}");
                    }
                }
            }

            assert_eq!(local_to_send.len(), recvd.len());
            for i in 0..local_to_send.len() {
                assert_eq!(local_to_send[i], recvd[i])
            }
            
            println!("{ch_id} assert ok");
        });
        reader_handles.push(reader_handle);
    }

    while reader_handles.len() != 0 {
        reader_handles.pop().unwrap().join().unwrap();
    }
    
    let total_ms = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() - start_ts;
    let backp_ms = writer_handle.join().unwrap();
    let throughput = (((num_msgs_per_channel * &channels.len()) as f64)/(total_ms as f64) * 1000.0) as u16;
    println!("Transfered in (ms): {total_ms}");
    println!("Backpressure (ms): {backp_ms}");
    println!("Throughput (msg/s): {throughput}");
    
    for (_, data_reader) in data_readers.read().unwrap().iter() {
        data_reader.close();
    }
    data_writer.close();
    if !local {
        while remote_transfer_handlers.len() != 0 {
            remote_transfer_handlers.pop().unwrap().close();
        }
    }

    io_loop.close();

    println!("TEST OK");

}