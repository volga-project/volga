
use std::{collections::HashMap, fs::{self, File}, io::{Read, Seek, SeekFrom, Write}, sync::{atomic::{AtomicBool, AtomicU64, Ordering}, Arc, RwLock, RwLockReadGuard}, thread::JoinHandle, time::Duration};
use advisory_lock::{AdvisoryFileLock, FileLockMode};
use crossbeam::queue::ArrayQueue;

// TODO we need to explicitly add new metric names to MetricsRecorder counters map
pub const NUM_BUFFERS_SENT: &str = "volga_num_buffers_sent";
pub const NUM_BUFFERS_RECVD: &str = "volga_num_buffers_recvd";

pub const NUM_BUFFERS_RESENT: &str = "volga_num_buffers_resent";

pub const NUM_BYTES_SENT: &str = "volga_num_buffers_sent";
pub const NUM_BYTES_RECVD: &str = "volga_num_buffers_recvd";


const METRICS_PATH_PREFIX: &str = "/tmp/volga/rust/metrics";
const FLUSH_PERIOD_S: u64 = 1;

const METRIC_KEY_DELIMITER: &str = ";";

pub struct MetricsRecorder {
    counters: Arc<RwLock<HashMap<String, AtomicU64>>>,
    io_handler_name: String,
    job_name: String,

    running: Arc<AtomicBool>,
    flush_thread_handle: Arc<ArrayQueue<JoinHandle<()>>> // array queue so we do not mutate and keep ownership
}

impl MetricsRecorder {

    pub fn new(io_handler_name: String, job_name: String) -> Self {
        MetricsRecorder{
            counters: Arc::new(RwLock::new(HashMap::new())),
            io_handler_name,
            job_name,
            running: Arc::new(AtomicBool::new(false)),
            flush_thread_handle: Arc::new(ArrayQueue::new(1))
        }
    }

    pub fn inc(&self, metric_name: &str, channel_or_peer_id: &str, value: u64) {
        let metric_key = metric_key(metric_name, channel_or_peer_id);
        let locked_read = self.counters.read().unwrap();
        if locked_read.contains_key(&metric_key) {
            let counter = locked_read.get(&metric_key).unwrap();
            counter.fetch_add(value, Ordering::Relaxed);
        } else {
            drop(locked_read); // avoid deadlock
            let mut locked_write = self.counters.write().unwrap();
            locked_write.insert(metric_key, AtomicU64::new(value));
        }
    }

    pub fn start(&self) {
        self.running.store(true, Ordering::Relaxed);


        let this_runnning = self.running.clone();
        let this_counters = self.counters.clone();
        let this_io_handler_name = self.io_handler_name.clone();
        let this_job_name = self.job_name.clone();
        let f = move || {
            while this_runnning.load(Ordering::Relaxed) {
                let locked_counters = this_counters.read().unwrap();
                MetricsRecorder::flush_all(locked_counters, this_io_handler_name.clone(), this_job_name.clone());

                std::thread::sleep(Duration::from_secs(FLUSH_PERIOD_S));
            }
        };

        self.flush_thread_handle.push(std::thread::spawn(f)).unwrap();
                
    }

    pub fn close(&self) {
        self.running.store(false, Ordering::Relaxed);
        let handle = self.flush_thread_handle.pop();
        handle.unwrap().join().unwrap();
        let locked_counters = self.counters.read().unwrap();
        MetricsRecorder::flush_all(locked_counters, self.io_handler_name.clone(), self.job_name.clone());
    }

    fn flush_all(counters: RwLockReadGuard<HashMap<String, AtomicU64>>, io_handler_name: String, job_name: String) {
        let mut to_flush = HashMap::new();
        for (metric_key, counter) in counters.iter() {
            // load value and reset counter
            let val = counter.swap(0, Ordering::Relaxed);
            to_flush.insert(metric_key.clone(), val);
        }
        flush_map(to_flush, io_handler_name, job_name.clone());
    }

}

fn metric_key(metric_name: &str, channel_or_peer_id: &str) -> String {
    format!("{metric_name}{METRIC_KEY_DELIMITER}{channel_or_peer_id}")
}

fn flush_map(to_flush: HashMap<String, u64>, io_handler_name: String, job_name: String) {
    // load previously stored data
    let path = format!("{METRICS_PATH_PREFIX}/{job_name}");
    fs::create_dir_all(path.clone()).unwrap();
    let filename = format!("{path}/{io_handler_name}_metrics.metrics");

    // locking
    // https://rust.code-maven.com/update-file-using-advisory-lock
    let mut file =  File::options().read(true).write(true).create(true).open(filename).unwrap();
    file.lock(FileLockMode::Exclusive).unwrap();
    let mut v = Vec::new();
    file.read_to_end(&mut v).unwrap();
    let mut stored: HashMap<String, u64> = HashMap::new();
    if v.len() != 0 {
        stored = rmp_serde::from_slice(&v).unwrap();
    }
    // merge stored and new
    for (metric, value) in to_flush.iter() {
        if stored.contains_key(metric) {
            let v = stored.get(metric).unwrap();
            stored.insert(metric.clone(), *v + *value);
        } else {
            stored.insert(metric.clone(), value.clone());
        }
    }
    let b = rmp_serde::to_vec(&stored).unwrap();
    // clean file
    file.seek(SeekFrom::Start(0)).unwrap();
    file.set_len(0).unwrap(); // truncate
    file.write_all(&b).unwrap();
    file.unlock().unwrap();
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    #[test]
    fn test_flush_map() {

        let now_ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
        let job_name = format!("job-{now_ts}");
        let io_handler_name = String::from("dummy_handler");
        let channel_id = String::from("ch_0");
        // let mr = MetricsRecorder::new(io_handler_name, job_name);

        let mut m1 = HashMap::new();
        m1.insert(metric_key(NUM_BUFFERS_SENT, &channel_id), 1);
        flush_map(m1, io_handler_name.clone(), job_name.clone());

        let mut m2 = HashMap::new();
        m2.insert(metric_key(NUM_BUFFERS_SENT, &channel_id), 3);
        m2.insert(metric_key(NUM_BUFFERS_RECVD, &channel_id), 5);
        flush_map(m2, io_handler_name.clone(), job_name.clone());

        // read
        let path = format!("{METRICS_PATH_PREFIX}/{job_name}");
        let filename = format!("{path}/{io_handler_name}_metrics.metrics");
        let b = fs::read(filename.clone()).unwrap();
        fs::remove_file(filename).unwrap();
        let mut res: HashMap<String, u64> = rmp_serde::from_slice(&b).unwrap();

        let mut expected = HashMap::new();
        expected.insert(metric_key(NUM_BUFFERS_SENT, &channel_id), 4);
        expected.insert(metric_key(NUM_BUFFERS_RECVD, &channel_id), 5);

        assert_eq!(res, expected);
        println!("Assert ok")
    }


    #[test]
    fn test_metrics_recorder() {
        let now_ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
        let job_name = format!("job-{now_ts}");
        let io_handler_name = String::from("dummy_handler");
        let channel_id = "ch_0";

        let mr = MetricsRecorder::new(io_handler_name.clone(), job_name.clone());
        mr.start();
        mr.inc(NUM_BUFFERS_SENT, channel_id, 1);
        std::thread::sleep(Duration::from_secs(FLUSH_PERIOD_S));
        mr.inc(NUM_BUFFERS_SENT, channel_id, 2);
        std::thread::sleep(Duration::from_secs(FLUSH_PERIOD_S));
        mr.inc(NUM_BUFFERS_RECVD, channel_id, 4);
        std::thread::sleep(Duration::from_millis(100));
        mr.close();

        let path = format!("{METRICS_PATH_PREFIX}/{job_name}");
        let filename = format!("{path}/{io_handler_name}_metrics.metrics");
        let b = fs::read(filename.clone()).unwrap();
        fs::remove_file(filename).unwrap();
        let mut res: HashMap<String, u64> = rmp_serde::from_slice(&b).unwrap();
        // strip zero vals
        for (metric, value) in res.clone().into_iter() {
            if value == 0 {
                res.remove(&metric);
            }
        }
        let mut expected = HashMap::new();
        expected.insert(metric_key(NUM_BUFFERS_SENT, &channel_id), 3);
        expected.insert(metric_key(NUM_BUFFERS_RECVD, &channel_id), 4);

        assert_eq!(res, expected);
    }
}
