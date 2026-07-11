use std::collections::HashSet;

use tokio::time::{timeout, Duration};

use crate::runtime::observability::StreamTaskStatus;

use super::ExecutionAttempt;

const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);

impl ExecutionAttempt {
    pub(in crate::runtime::master) async fn recover(
        &mut self,
        mut replace: HashSet<String>,
    ) -> anyhow::Result<()> {
        let close_futures: Vec<_> = self
            .clients
            .drain()
            .map(|(worker_id, client)| async move {
                (
                    worker_id,
                    timeout(CLOSE_TIMEOUT, client.close_worker(false)).await,
                )
            })
            .collect();

        for (worker_id, result) in futures::future::join_all(close_futures).await {
            match result {
                Ok(Ok(true)) => {
                    replace.remove(&worker_id);
                }
                Ok(Ok(false)) => {
                    println!(
                        "[MASTER] close_worker rejected for {}; replacing",
                        worker_id
                    );
                    replace.insert(worker_id);
                }
                Ok(Err(error)) => {
                    println!(
                        "[MASTER] close_worker failed for {}: {}; replacing",
                        worker_id, error
                    );
                    replace.insert(worker_id);
                }
                Err(_) => {
                    println!(
                        "[MASTER] close_worker timed out for {}; replacing",
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
        let close_tasks: Vec<_> = self
            .clients
            .iter()
            .map(|(worker_id, client)| {
                let worker_id = worker_id.clone();
                async move {
                    log_close(
                        "close_worker_tasks",
                        &worker_id,
                        client.close_worker_tasks().await,
                    );
                }
            })
            .collect();
        futures::future::join_all(close_tasks).await;

        if let Err(error) = self.wait_status(StreamTaskStatus::Closed).await {
            println!(
                "[MASTER] finish: wait Closed timed out (continuing cleanup): {}",
                error
            );
        }

        let close_workers: Vec<_> = self
            .clients
            .drain()
            .map(|(worker_id, client)| async move {
                log_close("close_worker", &worker_id, client.close_worker(true).await);
            })
            .collect();
        futures::future::join_all(close_workers).await;
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
