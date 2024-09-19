use core::time;
use std::thread;

use rand::{distributions::Alphanumeric, Rng};

pub fn random_string(len: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

pub fn sleep_thread() {
    let t = 50;
    thread::sleep(time::Duration::from_micros(t));
}