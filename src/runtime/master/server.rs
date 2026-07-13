use std::collections::HashMap;
use std::sync::Arc;

use super::service::MasterServiceImpl;
use crate::orchestrator::orchestrator::MasterOrchestrator;
use crate::runtime::master::MasterConfig;
use crate::runtime::master::LifecycleEventRecord;
use crate::runtime::observability::WorkerSnapshot;

pub mod master_service {
    tonic::include_proto!("master_service");
}

/// Server that hosts MasterService
pub struct MasterServer {
    service: MasterServiceImpl,
    server_handle: Option<tokio::task::JoinHandle<()>>,
    shutdown_sender: Option<tokio::sync::oneshot::Sender<()>>,
}

impl MasterServer {
    pub fn new(orchestrator: Arc<dyn MasterOrchestrator>) -> Self {
        Self {
            service: MasterServiceImpl::new(orchestrator),
            server_handle: None,
            shutdown_sender: None,
        }
    }

    pub async fn configure(&self, config: MasterConfig) {
        self.service.master().configure(config).await;
    }

    pub async fn execute(&mut self) -> anyhow::Result<()> {
        self.service.master().execute().await
    }

    pub fn master(&self) -> Arc<super::Master> {
        self.service.master()
    }

    pub async fn get_worker_states(&self) -> HashMap<String, WorkerSnapshot> {
        self.service.master().get_worker_states().await
    }

    pub async fn lifecycle_events_since(&self, sequence: u64) -> Vec<LifecycleEventRecord> {
        self.service.master().lifecycle_events_since(sequence).await
    }

    pub fn subscribe_lifecycle_events(
        &self,
    ) -> tokio::sync::broadcast::Receiver<LifecycleEventRecord> {
        self.service.master().subscribe_lifecycle_events()
    }

    pub async fn start(&mut self, addr: &str) -> anyhow::Result<()> {
        let addr = addr.parse()?;
        let service =
            master_service::master_service_server::MasterServiceServer::new(self.service.clone());

        println!("[MASTER_SERVER] Starting MasterService server on {}", addr);

        let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel::<()>();
        let server_handle = tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_shutdown(addr, async {
                    shutdown_receiver.await.ok();
                })
                .await;
        });

        self.server_handle = Some(server_handle);
        self.shutdown_sender = Some(shutdown_sender);
        Ok(())
    }

    pub async fn stop(&mut self) {
        if let Some(shutdown_sender) = self.shutdown_sender.take() {
            let _ = shutdown_sender.send(());
        }
        if let Some(handle) = self.server_handle.take() {
            let _ = handle.await;
        }
    }
}
