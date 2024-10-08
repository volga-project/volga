use std::collections::{HashMap, HashSet};
use std::fmt::format;
// use rand::{thread_rng, Rng};
use std::time::{Duration, SystemTime};
use std::{str, thread};

use volga_rust::network::utils::random_string;


#[test]
fn test_zmq_router_dealer() {
    let num_readers = 16;
    let num_writers = 16;
    let num_to_send_per_writer = 200000;
    let msg_size = 32;

    let mut reader_identities = vec![];
    let mut writer_addresses = vec![];

    for i in 0..num_readers {
        reader_identities.push(i.to_string());
    }

    let mut writer_handles = HashMap::new();
    let mut reader_handles = HashMap::new();

    let start_ts = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis();
    for i in 0..num_writers {
        let _reader_identities = reader_identities.clone();
        let addr = format!("ipc://writer_addr_{i}");
        writer_addresses.push(addr.clone());
        writer_handles.insert(i.to_string(), thread::spawn(move || {
            writer(&addr, _reader_identities, num_to_send_per_writer, msg_size);
        }));
    }
    for identity in reader_identities {
        let _writer_addresses = writer_addresses.clone();
        reader_handles.insert(identity.clone(), thread::spawn(move || {
            return reader(&identity, _writer_addresses);
        }));
    }
    for (_, handle) in writer_handles {
        handle.join().unwrap();
    }
    println!("Writers finished");

    let mut num_rcvd = HashMap::new();
    for (identity, handle) in reader_handles {
        let _rcvd = handle.join().unwrap();
        num_rcvd.insert(identity, _rcvd);
    }

    for (identity, _rcvd) in num_rcvd {
        println!("{identity} reader finished: {_rcvd} rcvd");
    }

    let total_ms = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() - start_ts;

    let throughput = (((num_writers * num_to_send_per_writer) as f64)/(total_ms as f64) * 1000.0) as u64;
    println!("Transfered in (ms): {total_ms}");
    println!("Throughput (msg/s): {throughput}");

}

fn reader(reader_identity: &str, writer_addreses: Vec<String>) -> u32{
    let context = zmq::Context::new();
    let mut socks = Vec::new();
    for addr in writer_addreses {
        let sock = context.socket(zmq::DEALER).unwrap();
        sock
            .set_identity(reader_identity.as_bytes())
            .expect("failed setting reader id");
        sock
            .connect(&addr)
            .expect("failed connecting reader");
        socks.push(sock);
    }
    
    // say hi to routers
    for i in 0..socks.len() {
        // sock.send("", zmq::SNDMORE).unwrap();
        socks[i].send("HI", 0).unwrap();
    }
    let mut poll_list = vec![];

    for i in 0..socks.len() {
        poll_list.push(socks[i].as_poll_item(zmq::POLLIN|zmq::POLLOUT));
    }
    let mut num_done = 0;
    let mut num_rcvd = 0;
    loop {
        zmq::poll(&mut poll_list, 1).unwrap();
        
        for i in 0..poll_list.len() {
            let poll_item = &poll_list[i];
            let sock = &socks[i];
            if poll_item.is_readable() {
                loop {
                    let msg = sock.recv_string(zmq::DONTWAIT);
                    if msg.is_ok() {
                        let msg = msg.unwrap().unwrap();
                        println!("Reader {} rcvd {}", reader_identity, num_rcvd);
                        if msg.eq("DONE") {
                            num_done += 1;
                            if num_done == socks.len() {
                                break;
                            }
                        }
                        num_rcvd += 1;
                    } else {
                        break;
                    }
                }
            }
        }
        if num_done == socks.len() {
            break;
        }
    }


    // loop {
    //     let msg = sock.recv_string(0).unwrap().unwrap();
    //     if msg.eq("DONE") {
    //         break;
    //     }
    //     num_rcvd += 1;
    //     println!("Reader {} rcvd {}", identity, num_rcvd);
    //     // let msg = sock.recv_string(0).unwrap().unwrap();
    //     // println!("Reader {} rcvd {}", identity, msg);
    //     // sock.send("ACK1", 0).unwrap();
    //     // sock.send("ACK2", 0).unwrap();
    //     // sock.send("ACK", 0).unwrap();
    // }
    // // println!("Reader {} rcvd total {}", identity, num_rcvd);
    return num_rcvd
}

fn writer(addr: &str, reader_identities: Vec<String>, num_to_send: u32, msg_size: usize) {
    let context = zmq::Context::new();
    let sock = context.socket(zmq::ROUTER).unwrap();
    sock.set_router_mandatory(true).unwrap();
    sock
        .bind(addr)
        .expect("Writer failed to bind");

    let mut to_connect: HashSet<String> = HashSet::from_iter(reader_identities.clone());
    let mut index = 0;
    

    // wait for connected dealers to say hi first
    loop {
        let identity = sock
            .recv_string(0)
            .expect("writer failed receiving identity")
            .unwrap();
        let rcvd_msg = sock
            .recv_string(0)
            .expect("writer failed receiving message")
            .unwrap();
        assert_eq!(rcvd_msg, "HI");

        assert!(reader_identities.contains(&identity));

        if to_connect.contains(&identity) {
            to_connect.remove(&identity);
            println!("{identity} connected")
        }

        if to_connect.len() == 0 {
            break;
        }
    }
    println!("All connected");

    let mut num_sent = num_to_send;
    loop {
        // let identity = sock
        //     .recv_string(0)
        //     .expect("writer failed receiving identity")
        //     .unwrap();
        // let rcvd_msg = sock
        //     .recv_string(0)
        //     .expect("writer failed receiving message")
        //     .unwrap();

        // println!("Writer rcvd {} from {}", rcvd_msg, identity);

        let identity = reader_identities.get(index).unwrap();
        let message = random_string(msg_size);
        sock
            .send(&identity, zmq::SNDMORE)
            .expect("writer failed sending identity");
        sock
            .send(&message, 0)
            .expect("writer failed sending message");
        index = (index + 1)%reader_identities.len();
        num_sent -= 1;
        if num_sent == 1 {
            break;
        }
    }

    for identity in reader_identities {
        sock
            .send(&identity, zmq::SNDMORE)
            .expect("writer failed sending identity");
        sock
            .send("DONE", 0)
            .expect("writer failed sending message");
    }
}