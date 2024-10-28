use std::{collections::HashMap, time::Duration};
use crossbeam::channel::{Receiver, Select, Sender};

use super::{buffer_utils::get_channeld_id, io_loop::Bytes};

/**
 * Incapsulates logic to route data from in-channels receivers to corresponding out-channels senders based on receiver_to_sender_mapping
 * 
 * Uses crossbeam::Select to register all senders/receivers and iteratively get ready pairs and route data.
 * 
 * Each call to .iter() looks up a matching receiver->sender pair and routes data or blocks until a pair is found.
 */

pub struct ChannelsRouter<'a> {
    indexes: HashMap<usize, (&'a String, bool)>,
    senders: HashMap<&'a String, &'a Sender<Box<Bytes>>>,
    receivers: HashMap<&'a String, &'a Receiver<Box<Bytes>>>,
    buffers_ready_to_send: HashMap<&'a String, Box<Bytes>>,
    ready_senders: HashMap<&'a String, &'a Sender<Box<Vec<u8>>>>,
    sel: Select<'a>,
    receiver_to_sender_mapping: &'a HashMap<String, Vec<String>>,
    sender_to_receiver_mapping: HashMap<&'a String, Vec<&'a String>>
    
}

impl<'a> ChannelsRouter<'a> {

    pub fn new(senders: HashMap<&'a String, &'a Sender<Box<Bytes>>>, receivers: HashMap<&'a String, &'a Receiver<Box<Bytes>>>, receiver_to_sender_mapping: &'a HashMap<String, Vec<String>>) -> Self {

        assert_ne!(senders.len(), 0);
        assert_ne!(receivers.len(), 0);
        assert_ne!(receiver_to_sender_mapping.len(), 0);

        let mut sel = Select::new();
        let mut indexes = HashMap::new();
        for (receiver_key, receiver) in receivers.iter() {
            let i = sel.recv(receiver);
            indexes.insert(i, (*receiver_key, false));
        }

        for (sender_key, sender) in senders.iter() {
            let i = sel.send(sender);
            indexes.insert(i, (*sender_key, true));
        }

        // for (s, rs) in sender_to_receiver_mapping.into_iter() {
        //     for r in rs {
        //         println!("[s->r] {s}->{r}");
        //     }
        // }

        let sender_to_receiver_mapping = Self::_sender_to_receiver_mapping(&receiver_to_sender_mapping);

        assert_ne!(sender_to_receiver_mapping.len(), 0);
        // for (r, ss) in &receiver_to_sender_mapping {
        //     for s in ss {
        //         println!("[r->s] {r}->{s}");
        //     }
        // }

        ChannelsRouter{
            indexes,
            senders, 
            receivers,
            buffers_ready_to_send: HashMap::new(),
            ready_senders: HashMap::new(),
            sel,
            receiver_to_sender_mapping,
            sender_to_receiver_mapping
        }
    }

    /**
     * use_message_channel_id_for_routing - in case of multiple matching senders for a receiver
     * we also make sure that intented channel (get_channel_id(msg)) matches receiver key on per-message basis
     */
    pub fn iter(&mut self, use_message_channel_id_for_routing: bool) -> Option<(usize, &String, &String)> {
        let index = self.sel.ready_timeout(Duration::from_millis(100));
        if !index.is_ok() {
            return None;
        }

        let index: usize = index.unwrap();
        let (key, is_sender) = self.indexes.get(&index).copied().unwrap();

        let mut result = None;

        if is_sender {
            // sender
            let sender_key = key;
            let receiver_key = self.ready_receiver_for_sender(sender_key, use_message_channel_id_for_routing);
            let sender = self.senders.get(sender_key).unwrap();
            if receiver_key.is_some() {
                let receiver_key = receiver_key.unwrap();
                let receiver = self.receivers.get(receiver_key).unwrap();
                // we have a match, send
                let b = self.buffers_ready_to_send.get(receiver_key).unwrap();
                let res = sender.try_send(b.clone());
                if !res.is_ok() {
                    println!("Unable to send");
                    return None;
                }

                let sent_size = b.len();
                result = Some((sent_size, sender_key, receiver_key));

                // remove stored data and re-register receiver
                self.buffers_ready_to_send.remove(receiver_key);
                let i = self.sel.recv(receiver);
                self.indexes.insert(i, (receiver_key, false));
            } else {
                // mark as ready and remove from selector until we have a matching receiver
                self.ready_senders.insert(sender_key, sender);
                self.sel.remove(index);
                self.indexes.remove(&index);
            }
        } else {
            // receiver
            let receiver_key = key;
            let receiver = self.receivers.get(receiver_key).unwrap();
            let b = receiver.try_recv();
            if !b.is_ok() {
                println!("Unable to rcv");
                return None;
            }
            let b = b.unwrap();
            let mut channel_id_for_routing = None;
            if use_message_channel_id_for_routing {
                channel_id_for_routing = Some(get_channeld_id(b.clone()));
            }
            let sender_key = self.ready_sender_for_receiver(receiver_key, channel_id_for_routing);
            
            if sender_key.is_some() {
                let sender_key = sender_key.unwrap();
                // we have a match, send
                let s = self.ready_senders.get(sender_key).copied().unwrap();
                s.send(b.clone()).expect("Unable to send"); // TODO timeout?

                let sent_size = b.len();
                result = Some((sent_size, sender_key, receiver_key));

                // re-register sender
                self.ready_senders.remove(sender_key);
                let i = self.sel.send(s);
                self.indexes.insert(i, (&sender_key, true));
            } else {
                // store received data and remove from selector until we have a matching sender
                self.buffers_ready_to_send.insert(receiver_key, b);
                self.sel.remove(index);
                self.indexes.remove(&index);
            }
        }
        result
    }


    fn ready_sender_for_receiver(&self, receiver_key: &'a String, channel_id_for_routing: Option<String>) -> Option<&'a String> {
        for sender_key in self.receiver_to_sender_mapping.get(receiver_key).unwrap() {
            if self.ready_senders.contains_key(sender_key) {
                if !channel_id_for_routing.is_none() {
                    let channel_id = channel_id_for_routing.unwrap();
                    if channel_id == *sender_key {
                        return Some(sender_key)
                    }
                }
                return Some(sender_key)
            }
        }
        None
    }

    fn ready_receiver_for_sender(&self, sender_key: &'a String, use_message_channel_id_for_routing: bool) -> Option<&'a String> {
        for receiver_key in self.sender_to_receiver_mapping.get(sender_key).unwrap() {
            if self.buffers_ready_to_send.contains_key(receiver_key) {
                if use_message_channel_id_for_routing {
                    let b = self.buffers_ready_to_send.get(receiver_key).unwrap();
                    let channel_id = get_channeld_id(b.clone());
                    if channel_id == *sender_key {
                        return Some(receiver_key)
                    }
                } else {
                    return Some(receiver_key)
                }
            }
        }
        None
    }

    fn _sender_to_receiver_mapping(receiver_to_sender_mapping: &'a HashMap<String, Vec<String>>) -> HashMap<&'a String, Vec<&'a String>> {
        let mut res = HashMap::new();
    
        for (receiver_key, sender_keys) in receiver_to_sender_mapping {
            for sender_key in sender_keys {
                res.entry(sender_key).or_insert_with(Vec::new).push(receiver_key)
            }
        }
    
        res
    }
}



#[cfg(test)]
mod tests {

    use std::{collections::HashSet, sync::{atomic::{AtomicBool, AtomicU64, Ordering}, Arc}, thread};

    use crossbeam::channel::unbounded;

    use crate::network_deprecated::buffer_utils::dummy_bytes;

    use super::*;

    #[test]
    fn test_channels_router() {
        _test_channels_router(1, 1);
        _test_channels_router(4, 8);
        _test_channels_router(8, 4);
    }


    fn _test_channels_router(num_receivers: usize, num_senders: usize) {
        let mut receiver_to_sender_mapping: HashMap<String, Vec<String>> = HashMap::new();
        let mut in_chans: HashMap<String, (Sender<Box<Vec<u8>>>, Receiver<Box<Vec<u8>>>)>  = HashMap::new();
        let mut out_chans: HashMap<String, (Sender<Box<Vec<u8>>>, Receiver<Box<Vec<u8>>>)> = HashMap::new();
        let mut sender_keys: HashSet<_> = HashSet::new();
        let mut receiver_keys = HashSet::new();

        // round robin matching
        if num_senders >= num_receivers {
            for i in 0..num_senders {
                let sender_key = format!("sender_{i}");
                let receiver_id = i%num_receivers;
                let receiver_key: String = format!("receiver_{receiver_id}");
                sender_keys.insert(sender_key.clone());
                receiver_keys.insert(receiver_key.clone());

                in_chans.insert(receiver_key.clone(), unbounded());
                out_chans.insert(sender_key.clone(), unbounded());

                if receiver_to_sender_mapping.contains_key(&receiver_key) {
                    receiver_to_sender_mapping.get_mut(&receiver_key).unwrap().push(sender_key.clone());
                } else {
                    receiver_to_sender_mapping.insert(receiver_key.clone(), vec![sender_key.clone()]);
                }
            }
        } else {
            for i in 0..num_receivers {
                let sender_id = i%num_senders;
                let sender_key = format!("sender_{sender_id}");
                let receiver_key = format!("receiver_{i}");
                sender_keys.insert(sender_key.clone());
                receiver_keys.insert(receiver_key.clone());

                in_chans.insert(receiver_key.clone(), unbounded());
                out_chans.insert(sender_key.clone(), unbounded());

                receiver_to_sender_mapping.insert(receiver_key.clone(), vec![sender_key.clone()]);
            }
        }

        let mut writers = vec![];
        let mut readers = vec![];

        let out_chans = Arc::new(out_chans);
        let in_chans = Arc::new(in_chans);

        for receiver_key in receiver_keys {
            // let _s = s.clone();
            let _receiver_key = receiver_key.clone();
            let _in_chans = in_chans.clone();
            let writer = thread::spawn(move || {
                let (s, _) = _in_chans.get(&_receiver_key).unwrap();
                for i in 0..1000 {
                    let ch_id = String::from("ch");
                    s.send(dummy_bytes(i, &ch_id, 1)).unwrap();
                    // println!("{_receiver_key} write {i}");
                }
                println!("{_receiver_key} write all ok");
            });
            writers.push(writer);
        }

        let read_counter = Arc::new(AtomicU64::new(0));
        // read_counter.fetch_update(set_order, fetch_order, f)

        for sender_key  in sender_keys {
            let _sender_key = sender_key.clone();
            let _out_chans = out_chans.clone();
            let _read_counter = read_counter.clone();
            let reader = thread::spawn(move || {
                let (_, r) = _out_chans.get(&sender_key).unwrap();
                loop {
                    let i = _read_counter.fetch_add(1, Ordering::Relaxed);
                    if i >= 1000 {
                        break;
                    }
                    r.recv().unwrap();
                    // println!("{_sender_key} read {i}");
                }
                println!("{_sender_key} read all ok");
            });
            readers.push(reader);
        }
        let receiver_to_sender_mapping = Arc::new(receiver_to_sender_mapping);
        let running = Arc::new(AtomicBool::new(true));
        let _running = running.clone();
        let switcher = thread::spawn(move || {

            let mut senders = HashMap::new();
            let mut receivers = HashMap::new();

            for (sender_key, (sender, _)) in out_chans.iter() {
                senders.insert(sender_key, sender);
            }
    
            for (receiver_key, (_, receiver)) in in_chans.iter() {
                receivers.insert(receiver_key, receiver);
            }

            let mut switch = ChannelsRouter::new(senders, receivers, &receiver_to_sender_mapping);
            while _running.load(Ordering::Relaxed) {
                switch.iter(false);
            }
            println!("switcher ok")
        });

        while !writers.is_empty() {
            writers.pop().unwrap().join().unwrap();
        }

        while !readers.is_empty() {
            readers.pop().unwrap().join().unwrap();
        }

        running.store(false, Ordering::Relaxed);
        switcher.join().unwrap();

        println!("assert ok")
        
    }
}