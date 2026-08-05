use std::collections::HashSet;

use tokio::time::timeout;

use crate::runtime::consts::{runtime_consts, MASTER_RESET_WORKER_TIMEOUT};
use crate::runtime::observability::StreamTaskStatus;

use super::session::{CloseTasks, ResetWorker, ShutdownWorker};
use super::ExecutionAttempt;

impl ExecutionAttempt {
    pub(in crate::runtime::master) async fn recover(
        &mut self,
        mut replace: HashSet<String>,
    ) -> anyhow::Result<()> {
        self.state.clear_workers_execution_attempt().await;
        let _ = self
            .state
            .abort_in_flight_checkpoint(self.id, "recovering".to_string())
            .await;
        let reset_timeout = runtime_consts().duration(MASTER_RESET_WORKER_TIMEOUT);
        let reset_futures: Vec<_> = self
            .sessions
            .drain()
            .map(|(worker_id, session)| async move {
                let result = timeout(reset_timeout, session.ask(ResetWorker)).await;
                let _ = session.stop_gracefully().await;
                (worker_id, result)
            })
            .collect();

        for (worker_id, result) in futures::future::join_all(reset_futures).await {
            match result {
                Ok(Ok(true)) => {}
                Ok(Ok(false)) => {
                    println!(
                        "[MASTER] reset_worker rejected for {}; replacing",
                        worker_id
                    );
                    replace.insert(worker_id);
                }
                Ok(Err(error)) => {
                    println!(
                        "[MASTER] reset_worker failed for {}: {:?}; replacing",
                        worker_id, error
                    );
                    replace.insert(worker_id);
                }
                Err(_) => {
                    println!(
                        "[MASTER] reset_worker timed out for {}; replacing",
                        worker_id
                    );
                    replace.insert(worker_id);
                }
            }
        }

        if replace.is_empty() {
            return Ok(());
        }

        let worker_ids: Vec<_> = replace.into_iter().collect();
        println!("[MASTER] Requesting replacement {:?}", worker_ids);
        self.state.request_replacement(&worker_ids).await
    }

    pub(in crate::runtime::master) async fn finish(&mut self) {
        let rpc_timeout = runtime_consts().duration(MASTER_RESET_WORKER_TIMEOUT);
        // Close first (while workers still bound to this attempt).
        let close_tasks: Vec<_> = self
            .sessions
            .iter()
            .map(|(worker_id, session)| {
                let worker_id = worker_id.clone();
                let session = session.clone();
                async move {
                    match timeout(rpc_timeout, session.ask(CloseTasks)).await {
                        Ok(Ok(success)) => {
                            log_close("close_worker_tasks", &worker_id, Ok(success))
                        }
                        Ok(Err(error)) => log_close(
                            "close_worker_tasks",
                            &worker_id,
                            Err(anyhow::anyhow!("{error:?}")),
                        ),
                        Err(_) => println!(
                            "[MASTER] finish: close_worker_tasks timed out on {worker_id}"
                        ),
                    }
                }
            })
            .collect();
        futures::future::join_all(close_tasks).await;

        if let Err(workers) = self.wait_status(StreamTaskStatus::Closed).await {
            println!(
                "[MASTER] finish: workers did not reach Closed (continuing cleanup): {:?}",
                workers
            );
        }

        self.state.clear_workers_execution_attempt().await;

        let shutdown_workers: Vec<_> = self
            .sessions
            .drain()
            .map(|(worker_id, session)| async move {
                match timeout(rpc_timeout, session.ask(ShutdownWorker)).await {
                    Ok(Ok(success)) => log_close("shutdown_worker", &worker_id, Ok(success)),
                    Ok(Err(error)) => log_close(
                        "shutdown_worker",
                        &worker_id,
                        Err(anyhow::anyhow!("{error:?}")),
                    ),
                    Err(_) => println!(
                        "[MASTER] finish: shutdown_worker timed out on {worker_id}"
                    ),
                }
                let _ = session.stop_gracefully().await;
            })
            .collect();
        futures::future::join_all(shutdown_workers).await;
    }
}

fn log_close(operation: &str, worker_id: &str, result: anyhow::Result<bool>) {
    match result {
        Ok(true) => {}
        Ok(false) => println!(
            "[MASTER] finish: {} soft-failed on {}",
            operation, worker_id
        ),
        Err(error) => println!(
            "[MASTER] finish: {} error on {}: {}",
            operation, worker_id, error
        ),
    }
}
