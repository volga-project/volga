use core::time;
use std::thread;

use rand::{distributions::Alphanumeric, Rng};
use itertools::Itertools;

pub fn random_string(len: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

pub fn consecutive_slices(data: &Vec<u32>) -> Vec<Vec<u32>> {
    (&(0..data.len()).chunk_by(|&i| data[i] as usize - i))
        .into_iter()
        .map(|(_, group)| group.map(|i| data[i]).collect())
        .collect()
}

pub fn sleep_thread() {
    let t = 50;
    thread::sleep(time::Duration::from_micros(t));
}
