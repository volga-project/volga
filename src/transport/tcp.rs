//! Per-logical-edge TCP hop: one connection, one pump, queue + TCP window for flow control.

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use metrics::{counter, gauge};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{oneshot, Notify};
use tokio::task::JoinHandle;

use crate::common::message::Message;
use crate::runtime::consts::{
    runtime_consts, TRANSPORT_TCP_CONNECT_MAX_RETRIES, TRANSPORT_TCP_CONNECT_RETRY_DELAY,
};
use crate::runtime::health::{WorkerFatalReason, WorkerHealth};
use crate::runtime::metrics::{
    MetricsLabels, LABEL_PIPELINE_ID, LABEL_TARGET_TASK_ID, LABEL_TASK_ID, LABEL_WORKER_ID,
    METRIC_STREAM_TASK_TRANSPORT_DISCONNECTS,
    METRIC_STREAM_TASK_TRANSPORT_WRITE_BLOCK_MS_PER_SECOND,
};
use crate::transport::batch_channel::{BatchReceiver, BatchSender};

/// Same 12MiB cap as control-plane gRPC messages; TCP frames are length-prefixed.
const MAX_FRAME_BYTES: u32 = 12 * 1024 * 1024;
/// Userspace write buffer (tokio default). Auto-flushes when full.
const WRITE_BUF_BYTES: usize = 8 * 1024;
/// Flush leftover bytes this long after the last write if the edge queue is empty.
const FLUSH_COALESCE: Duration = Duration::from_millis(2);
const METRICS_TICK: Duration = Duration::from_millis(100);

struct AbortOnDropHandles(Vec<JoinHandle<()>>);

impl Drop for AbortOnDropHandles {
    fn drop(&mut self) {
        for handle in self.0.drain(..) {
            handle.abort();
        }
    }
}

impl AbortOnDropHandles {
    fn push(&mut self, handle: JoinHandle<()>) {
        self.0.push(handle);
    }
}

#[derive(Clone, Debug)]
pub struct EdgeIdentity {
    pub channel_id: String,
    pub task_id: String,
    pub target_task_id: String,
}

/// Docker compose advertises DNS names (`worker-0`); kube uses pod IPs. Do not parse as `SocketAddr`.
#[derive(Clone, Debug)]
pub struct RemoteEndpoint {
    pub host: String,
    pub port: u16,
}

impl fmt::Display for RemoteEndpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

/// Length-prefixed frame. Does not flush; callers coalesce via `BufWriter`.
async fn write_frame<W>(stream: &mut W, payload: &[u8]) -> io::Result<()>
where
    W: AsyncWriteExt + Unpin,
{
    let len = u32::try_from(payload.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "transport frame exceeds u32"))?;
    if len > MAX_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("transport frame {len} exceeds max {MAX_FRAME_BYTES}"),
        ));
    }
    stream.write_all(&len.to_le_bytes()).await?;
    stream.write_all(payload).await?;
    Ok(())
}

async fn read_frame(stream: &mut TcpStream) -> io::Result<Option<Vec<u8>>> {
    let mut len_buf = [0u8; 4];
    match stream.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let len = u32::from_le_bytes(len_buf);
    if len > MAX_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("transport frame {len} exceeds max {MAX_FRAME_BYTES}"),
        ));
    }
    let mut buf = vec![0u8; len as usize];
    stream.read_exact(&mut buf).await?;
    Ok(Some(buf))
}

fn increment_disconnects(identity: &EdgeIdentity, labels: Option<&MetricsLabels>) {
    if let Some(labels) = labels {
        counter!(
            METRIC_STREAM_TASK_TRANSPORT_DISCONNECTS,
            LABEL_TASK_ID => identity.task_id.clone(),
            LABEL_TARGET_TASK_ID => identity.target_task_id.clone(),
            LABEL_PIPELINE_ID => labels.pipeline_id.clone(),
            LABEL_WORKER_ID => labels.worker_id.clone()
        )
        .increment(1);
    } else {
        counter!(
            METRIC_STREAM_TASK_TRANSPORT_DISCONNECTS,
            LABEL_TASK_ID => identity.task_id.clone(),
            LABEL_TARGET_TASK_ID => identity.target_task_id.clone()
        )
        .increment(1);
    }
}

async fn connect_with_retry(
    endpoint: &RemoteEndpoint,
    running: &AtomicBool,
) -> io::Result<TcpStream> {
    let max_attempts = runtime_consts()
        .u64(TRANSPORT_TCP_CONNECT_MAX_RETRIES)
        .max(1);
    let delay = runtime_consts().duration(TRANSPORT_TCP_CONNECT_RETRY_DELAY);
    let mut last_err = None;
    for attempt in 0..max_attempts {
        if !running.load(Ordering::Relaxed) {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "transport shutting down",
            ));
        }
        match TcpStream::connect((endpoint.host.as_str(), endpoint.port)).await {
            Ok(stream) => {
                let _ = stream.set_nodelay(true);
                return Ok(stream);
            }
            Err(e) => {
                last_err = Some(e);
                if attempt + 1 < max_attempts {
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }
    Err(last_err.unwrap_or_else(|| io::Error::other("tcp connect failed")))
}

/// Drain one egress queue onto a dedicated TCP connection.
///
/// Frames stay length-prefixed. `BufWriter` flushes when full; we also flush
/// when the edge queue stays empty for `FLUSH_COALESCE` — not after every `Message`.
pub async fn pump_egress(
    mut rx: BatchReceiver,
    endpoint: RemoteEndpoint,
    identity: EdgeIdentity,
    worker_health: Arc<WorkerHealth>,
    running: Arc<AtomicBool>,
    labels: Option<MetricsLabels>,
) {
    let stream = match connect_with_retry(&endpoint, &running).await {
        Ok(stream) => stream,
        Err(e) => {
            if running.load(Ordering::Relaxed) {
                increment_disconnects(&identity, labels.as_ref());
                worker_health.report_fatal(
                    WorkerFatalReason::TransportDisconnect,
                    format!(
                        "[TCP] connect failed channel {} addr {}: {}",
                        identity.channel_id, endpoint, e
                    ),
                );
            }
            return;
        }
    };
    let mut writer = BufWriter::with_capacity(WRITE_BUF_BYTES, stream);

    if let Err(e) = async {
        write_frame(&mut writer, identity.channel_id.as_bytes()).await?;
        writer.flush().await
    }
    .await
    {
        if running.load(Ordering::Relaxed) {
            increment_disconnects(&identity, labels.as_ref());
            worker_health.report_fatal(
                WorkerFatalReason::TransportDisconnect,
                format!(
                    "[TCP] handshake write failed channel {}: {}",
                    identity.channel_id, e
                ),
            );
        }
        return;
    }

    let mut window_start = Instant::now();
    let mut blocked_ns: u64 = 0;

    loop {
        if !running.load(Ordering::Relaxed) {
            let _ = writer.flush().await;
            return;
        }

        let pending = !writer.buffer().is_empty();
        tokio::select! {
            biased;

            message = rx.recv() => {
                let Some(message) = message else {
                    let _ = writer.flush().await;
                    return;
                };
                let bytes = message.to_bytes();
                if let Err(e) = sampled_io(
                    write_frame(&mut writer, &bytes),
                    &identity,
                    labels.as_ref(),
                    &mut window_start,
                    &mut blocked_ns,
                )
                .await
                {
                    if running.load(Ordering::Relaxed) {
                        increment_disconnects(&identity, labels.as_ref());
                        worker_health.report_fatal(
                            WorkerFatalReason::TransportDisconnect,
                            format!("[TCP] write failed channel {}: {}", identity.channel_id, e),
                        );
                    }
                    return;
                }
            }

            _ = tokio::time::sleep(FLUSH_COALESCE), if pending => {
                if let Err(e) = sampled_io(
                    writer.flush(),
                    &identity,
                    labels.as_ref(),
                    &mut window_start,
                    &mut blocked_ns,
                )
                .await
                {
                    if running.load(Ordering::Relaxed) {
                        increment_disconnects(&identity, labels.as_ref());
                        worker_health.report_fatal(
                            WorkerFatalReason::TransportDisconnect,
                            format!("[TCP] write failed channel {}: {}", identity.channel_id, e),
                        );
                    }
                    return;
                }
            }

            _ = tokio::time::sleep(METRICS_TICK), if !pending => {
                report_write_block_gauge(
                    &identity,
                    labels.as_ref(),
                    &mut window_start,
                    &mut blocked_ns,
                );
            }
        }
    }
}

/// Sample write-block while `write_all` / `flush` is in flight so a multi-second
/// TCP window stall shows up on the gauge before the IO returns.
async fn sampled_io<F>(
    fut: F,
    identity: &EdgeIdentity,
    labels: Option<&MetricsLabels>,
    window_start: &mut Instant,
    blocked_ns: &mut u64,
) -> io::Result<()>
where
    F: Future<Output = io::Result<()>>,
{
    let mut io_start = Instant::now();
    tokio::pin!(fut);
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    interval.tick().await;
    loop {
        tokio::select! {
            result = &mut fut => {
                *blocked_ns = blocked_ns.saturating_add(io_start.elapsed().as_nanos() as u64);
                report_write_block_gauge(identity, labels, window_start, blocked_ns);
                return result;
            }
            _ = interval.tick() => {
                *blocked_ns = blocked_ns.saturating_add(io_start.elapsed().as_nanos() as u64);
                io_start = Instant::now();
                report_write_block_gauge(identity, labels, window_start, blocked_ns);
            }
        }
    }
}

fn report_write_block_gauge(
    identity: &EdgeIdentity,
    labels: Option<&MetricsLabels>,
    window_start: &mut Instant,
    blocked_ns: &mut u64,
) {
    let elapsed = window_start.elapsed();
    if elapsed < Duration::from_secs(1) {
        return;
    }
    let ms_per_s = ((*blocked_ns as f64 / 1_000_000.0) / elapsed.as_secs_f64()).min(1000.0);
    if let Some(labels) = labels {
        gauge!(
            METRIC_STREAM_TASK_TRANSPORT_WRITE_BLOCK_MS_PER_SECOND,
            LABEL_TASK_ID => identity.task_id.clone(),
            LABEL_TARGET_TASK_ID => identity.target_task_id.clone(),
            LABEL_PIPELINE_ID => labels.pipeline_id.clone(),
            LABEL_WORKER_ID => labels.worker_id.clone()
        )
        .set(ms_per_s);
    } else {
        gauge!(
            METRIC_STREAM_TASK_TRANSPORT_WRITE_BLOCK_MS_PER_SECOND,
            LABEL_TASK_ID => identity.task_id.clone(),
            LABEL_TARGET_TASK_ID => identity.target_task_id.clone()
        )
        .set(ms_per_s);
    }
    *blocked_ns = 0;
    *window_start = Instant::now();
}

/// Bind the per-worker ingress port. Call before spawning egress pumps so connect retries are not racing bind.
pub async fn bind_ingress(port: i32) -> std::io::Result<TcpListener> {
    let addr: SocketAddr = format!("0.0.0.0:{port}")
        .parse()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    let listener = TcpListener::bind(addr).await?;
    println!("[TCP] listening on {addr}");
    Ok(listener)
}

/// Accept until every expected ingress `channel_id` is claimed, then drop the
/// listener (pumps keep running until shutdown). Returning here would abort
/// those pumps via `AbortOnDropHandles`.
pub async fn listen_ingress(
    listener: TcpListener,
    senders: Arc<Mutex<HashMap<String, (EdgeIdentity, BatchSender)>>>,
    worker_health: Arc<WorkerHealth>,
    running: Arc<AtomicBool>,
    labels: Option<MetricsLabels>,
    shutdown_rx: oneshot::Receiver<()>,
) {
    tokio::pin!(shutdown_rx);
    let mut ingress_tasks = AbortOnDropHandles(Vec::new());
    let expected_done = Arc::new(Notify::new());
    let mut listener = Some(listener);
    loop {
        tokio::select! {
            _ = &mut shutdown_rx => {
                println!("[TCP] listen shutdown");
                return;
            }
            _ = expected_done.notified(), if listener.is_some() => {
                println!("[TCP] expected ingress edges connected");
                listener = None;
            }
            accept = async {
                listener
                    .as_mut()
                    .expect("accept armed only while listening")
                    .accept()
                    .await
            }, if listener.is_some() => {
                match accept {
                    Ok((mut stream, peer)) => {
                        let _ = stream.set_nodelay(true);
                        let senders = senders.clone();
                        let worker_health = worker_health.clone();
                        let running = running.clone();
                        let labels = labels.clone();
                        let expected_done = expected_done.clone();
                        ingress_tasks.push(tokio::spawn(async move {
                            serve_ingress(
                                &mut stream,
                                peer,
                                senders,
                                worker_health,
                                running,
                                labels,
                                expected_done,
                            )
                            .await;
                        }));
                    }
                    Err(e) => {
                        if running.load(Ordering::Relaxed) {
                            worker_health.report_fatal(
                                WorkerFatalReason::TransportDisconnect,
                                format!("[TCP] accept failed: {e}"),
                            );
                        }
                        return;
                    }
                }
            }
        }
    }
}

async fn serve_ingress(
    stream: &mut TcpStream,
    peer: SocketAddr,
    senders: Arc<Mutex<HashMap<String, (EdgeIdentity, BatchSender)>>>,
    worker_health: Arc<WorkerHealth>,
    running: Arc<AtomicBool>,
    labels: Option<MetricsLabels>,
    expected_done: Arc<Notify>,
) {
    let handshake = match read_frame(stream).await {
        Ok(Some(bytes)) => bytes,
        Ok(None) => return,
        Err(e) => {
            if running.load(Ordering::Relaxed) {
                worker_health.report_fatal(
                    WorkerFatalReason::TransportDisconnect,
                    format!("[TCP] handshake read from {peer} failed: {e}"),
                );
            }
            return;
        }
    };
    let channel_id = match String::from_utf8(handshake) {
        Ok(id) => id,
        Err(_) => {
            if running.load(Ordering::Relaxed) {
                worker_health.report_fatal(
                    WorkerFatalReason::TransportDisconnect,
                    format!("[TCP] invalid handshake from {peer}"),
                );
            }
            return;
        }
    };
    let (identity, sender, all_claimed) = {
        let mut guard = senders.lock().unwrap_or_else(|e| e.into_inner());
        let Some((identity, sender)) = guard.remove(&channel_id) else {
            drop(guard);
            if running.load(Ordering::Relaxed) {
                worker_health.report_fatal(
                    WorkerFatalReason::TransportDisconnect,
                    format!("[TCP] unknown or duplicate channel {channel_id} from {peer}"),
                );
            }
            return;
        };
        let all_claimed = guard.is_empty();
        (identity, sender, all_claimed)
    };
    if all_claimed {
        expected_done.notify_one();
    }

    loop {
        if !running.load(Ordering::Relaxed) {
            return;
        }
        let payload = match read_frame(stream).await {
            Ok(Some(bytes)) => bytes,
            Ok(None) => {
                if running.load(Ordering::Relaxed) {
                    increment_disconnects(&identity, labels.as_ref());
                    worker_health.report_fatal(
                        WorkerFatalReason::TransportDisconnect,
                        format!("[TCP] peer closed channel {}", identity.channel_id),
                    );
                }
                return;
            }
            Err(e) => {
                if running.load(Ordering::Relaxed) {
                    increment_disconnects(&identity, labels.as_ref());
                    worker_health.report_fatal(
                        WorkerFatalReason::TransportDisconnect,
                        format!("[TCP] read failed channel {}: {}", identity.channel_id, e),
                    );
                }
                return;
            }
        };
        let message = match catch_unwind(AssertUnwindSafe(|| Message::from_bytes(&payload))) {
            Ok(message) => message,
            Err(_) => {
                if running.load(Ordering::Relaxed) {
                    increment_disconnects(&identity, labels.as_ref());
                    worker_health.report_fatal(
                        WorkerFatalReason::TransportDisconnect,
                        format!("[TCP] decode failed channel {}", identity.channel_id),
                    );
                }
                return;
            }
        };
        if sender.send(message, None).await.is_err() {
            if running.load(Ordering::Relaxed) {
                increment_disconnects(&identity, labels.as_ref());
                worker_health.report_fatal(
                    WorkerFatalReason::TransportDisconnect,
                    format!("[TCP] local queue closed channel {}", identity.channel_id),
                );
            }
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::message::WatermarkMessage;
    use crate::transport::batch_channel::batch_bounded_channel;

    fn wm(i: u64) -> Message {
        Message::Watermark(WatermarkMessage::new("t".to_string(), i, Some(0)))
    }

    async fn connected_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let connect = tokio::spawn(async move { TcpStream::connect(addr).await.unwrap() });
        let (server, _) = listener.accept().await.unwrap();
        (connect.await.unwrap(), server)
    }

    #[tokio::test]
    async fn read_frame_rejects_oversize_len() {
        let (mut client, mut server) = connected_pair().await;
        client
            .write_all(&(MAX_FRAME_BYTES + 1).to_le_bytes())
            .await
            .unwrap();
        let err = read_frame(&mut server).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn connect_resolves_localhost_hostname() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let running = AtomicBool::new(true);
        let endpoint = RemoteEndpoint {
            host: "localhost".into(),
            port,
        };
        let connect =
            tokio::spawn(async move { connect_with_retry(&endpoint, &running).await.unwrap() });
        let (_server, _) = listener.accept().await.unwrap();
        connect.await.unwrap();
    }

    #[tokio::test]
    async fn listen_stops_accepting_after_expected_edges_claimed() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, _rx) = batch_bounded_channel(8);
        let senders = Arc::new(Mutex::new(HashMap::from([(
            "a_to_b".to_string(),
            (
                EdgeIdentity {
                    channel_id: "a_to_b".into(),
                    task_id: "a".into(),
                    target_task_id: "b".into(),
                },
                tx,
            ),
        )])));
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let running = Arc::new(AtomicBool::new(true));
        let health = Arc::new(WorkerHealth::new());
        let listen = tokio::spawn(async move {
            listen_ingress(listener, senders, health, running, None, shutdown_rx).await;
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        write_frame(&mut client, b"a_to_b").await.unwrap();
        client.flush().await.unwrap();

        let start = Instant::now();
        let refused = loop {
            match TcpStream::connect(addr).await {
                Err(e) if e.kind() == io::ErrorKind::ConnectionRefused => break true,
                Ok(_) | Err(_) if start.elapsed() > Duration::from_secs(2) => break false,
                Ok(_) | Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        };
        assert!(
            refused,
            "listener should drop after the expected channel is claimed"
        );
        let _ = shutdown_tx.send(());
        listen.await.unwrap();
    }

    #[tokio::test]
    async fn write_frame_does_not_flush() {
        let (client, mut server) = connected_pair().await;
        let mut writer = BufWriter::with_capacity(WRITE_BUF_BYTES, client);
        write_frame(&mut writer, b"hello").await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(50), read_frame(&mut server))
                .await
                .is_err(),
            "frame must stay in the write buf until flush"
        );
        writer.flush().await.unwrap();
        assert_eq!(read_frame(&mut server).await.unwrap().unwrap(), b"hello");
    }

    /// Many 1-record messages on one remote edge: frames stay length-prefixed
    /// and all arrive. Coalesced flush is write-side only.
    #[tokio::test]
    async fn pump_egress_coalesces_many_small_frames() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (tx, rx) = batch_bounded_channel(8192);
        let identity = EdgeIdentity {
            channel_id: "soak".into(),
            task_id: "a".into(),
            target_task_id: "b".into(),
        };
        let running = Arc::new(AtomicBool::new(true));
        let health = Arc::new(WorkerHealth::new());
        let endpoint = RemoteEndpoint {
            host: "127.0.0.1".into(),
            port,
        };
        let pump = tokio::spawn(pump_egress(
            rx,
            endpoint,
            identity,
            health,
            running.clone(),
            None,
        ));

        let (mut server, _) = listener.accept().await.unwrap();
        let handshake = read_frame(&mut server).await.unwrap().unwrap();
        assert_eq!(handshake, b"soak");

        const N: u64 = 512;
        for i in 0..N {
            tx.send(wm(i), None).await.unwrap();
        }
        drop(tx);

        let mut got = Vec::with_capacity(N as usize);
        while got.len() < N as usize {
            let payload = tokio::time::timeout(Duration::from_secs(5), read_frame(&mut server))
                .await
                .expect("timed out waiting for coalesced frames")
                .unwrap()
                .expect("peer closed before all frames arrived");
            let Message::Watermark(wm) = Message::from_bytes(&payload) else {
                panic!("expected watermark");
            };
            got.push(wm.watermark_value);
        }
        assert_eq!(got, (0..N).collect::<Vec<_>>());

        pump.await.unwrap();
        running.store(false, Ordering::Relaxed);
    }
}
