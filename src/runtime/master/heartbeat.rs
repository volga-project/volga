use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};
use tokio_stream::wrappers::ReceiverStream;

use super::failure::{FailureEvent, FailureKind};
use super::worker_service::{
    worker_service_client::WorkerServiceClient, MasterHeartbeatMessage,
    WorkerFatalReason as WorkerFatalReasonProto,
};

const MAX_STREAM_ATTEMPTS: u32 = 5;
const RECONNECT_DELAY_MS: u64 = 1000;

enum StreamOutcome {
    /// Worker reported unhealthy via heartbeat payload — do not reconnect.
    WorkerFatal(FailureKind, String),
    /// Control-plane stream died; caller may reconnect.
    Transient(String),
}

pub struct WorkerHeartbeatMonitor {
    task_handle: tokio::task::JoinHandle<()>,
}

impl Drop for WorkerHeartbeatMonitor {
    fn drop(&mut self) {
        self.task_handle.abort();
    }
}

impl WorkerHeartbeatMonitor {
    pub fn spawn(
        worker_id: String,
        worker_ip: String,
        execution_attempt_id: u64,
        client: WorkerServiceClient<tonic::transport::Channel>,
        failure_tx: mpsc::Sender<FailureEvent>,
    ) -> Self {
        let task_handle = tokio::spawn(Self::run(
            worker_id,
            worker_ip,
            execution_attempt_id,
            client,
            failure_tx,
        ));
        Self { task_handle }
    }

    async fn report(
        worker_id: &str,
        worker_ip: &str,
        failure_tx: &mpsc::Sender<FailureEvent>,
        kind: FailureKind,
        detail: String,
    ) {
        println!(
            "[MASTER] Worker failure {} ({}): {}",
            worker_id, worker_ip, detail
        );
        let _ = failure_tx
            .send(FailureEvent {
                worker_id: worker_id.to_string(),
                kind,
                detail,
            })
            .await;
    }

    async fn run(
        worker_id: String,
        worker_ip: String,
        execution_attempt_id: u64,
        mut client: WorkerServiceClient<tonic::transport::Channel>,
        failure_tx: mpsc::Sender<FailureEvent>,
    ) {
        let mut last_detail = String::new();

        for attempt in 0..MAX_STREAM_ATTEMPTS {
            if attempt > 0 {
                println!(
                    "[MASTER] Heartbeat reconnect {}/{} for {} ({})",
                    attempt + 1,
                    MAX_STREAM_ATTEMPTS,
                    worker_id,
                    worker_ip
                );
                sleep(Duration::from_millis(RECONNECT_DELAY_MS * attempt as u64)).await;
                // Re-dial in case the underlying channel is dead.
                match WorkerServiceClient::connect(format!("http://{}", worker_ip)).await {
                    Ok(c) => client = c,
                    Err(e) => {
                        last_detail = format!("heartbeat re-dial failed: {}", e);
                        continue;
                    }
                }
            }

            match Self::run_one_stream(&worker_id, &worker_ip, execution_attempt_id, &mut client)
                .await
            {
                StreamOutcome::WorkerFatal(kind, detail) => {
                    Self::report(&worker_id, &worker_ip, &failure_tx, kind, detail).await;
                    return;
                }
                StreamOutcome::Transient(detail) => {
                    last_detail = detail;
                }
            }
        }

        Self::report(
            &worker_id,
            &worker_ip,
            &failure_tx,
            FailureKind::HeartbeatUnavailable,
            format!(
                "heartbeat stream failed after {} attempts: {}",
                MAX_STREAM_ATTEMPTS, last_detail
            ),
        )
        .await;
    }

    async fn run_one_stream(
        worker_id: &str,
        worker_ip: &str,
        execution_attempt_id: u64,
        client: &mut WorkerServiceClient<tonic::transport::Channel>,
    ) -> StreamOutcome {
        let (tx, rx) = tokio::sync::mpsc::channel::<MasterHeartbeatMessage>(32);
        let outbound = ReceiverStream::new(rx);
        let response = match client.stream_heartbeat(tonic::Request::new(outbound)).await {
            Ok(response) => response,
            Err(e) => {
                return StreamOutcome::Transient(format!(
                    "heartbeat stream setup failed for {} ({}): {}",
                    worker_id, worker_ip, e
                ));
            }
        };

        let mut inbound = response.into_inner();
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if tx.send(MasterHeartbeatMessage {
                        sent_at_ms: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as u64,
                        execution_attempt_id,
                    }).await.is_err() {
                        return StreamOutcome::Transient(
                            "heartbeat outbound closed".to_string(),
                        );
                    }
                }
                msg = inbound.message() => {
                    match msg {
                        Ok(Some(heartbeat)) => {
                            if heartbeat.execution_attempt_id != execution_attempt_id {
                                continue;
                            }
                            if !heartbeat.healthy {
                                let kind = match WorkerFatalReasonProto::try_from(
                                    heartbeat.fatal_reason,
                                ) {
                                    Ok(WorkerFatalReasonProto::Panic) => FailureKind::WorkerPanic,
                                    Ok(WorkerFatalReasonProto::TaskFailure) => {
                                        FailureKind::TaskFailure
                                    }
                                    Ok(WorkerFatalReasonProto::TransportDisconnect) => {
                                        FailureKind::TransportDisconnect
                                    }
                                    _ => panic!("unknown fatal reason: {:?}", heartbeat.fatal_reason),
                                };
                                return StreamOutcome::WorkerFatal(kind, heartbeat.fatal_message);
                            }
                        }
                        Ok(None) => {
                            return StreamOutcome::Transient(
                                "heartbeat stream closed".to_string(),
                            );
                        }
                        Err(e) => {
                            return StreamOutcome::Transient(format!(
                                "heartbeat stream error: {}",
                                e
                            ));
                        }
                    }
                }
            }
        }
    }
}
