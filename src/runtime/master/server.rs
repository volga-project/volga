use std::sync::Arc;

use super::service::MasterServiceImpl;
use crate::common::grpc::{
    master::master_server, server_builder, spawn_with_shutdown, GrpcServeHandle,
};
use crate::orchestrator::orchestrator::MasterOrchestrator;
use crate::runtime::master::MasterConfig;
use crate::runtime::master::LifecycleEventRecord;

/// Re-export generated stubs (single include lives in `common::grpc::stubs`).
pub use crate::common::grpc::stubs::master_service;

/// Server that hosts MasterService
pub struct MasterServer {
    service: MasterServiceImpl,
    serve: Option<GrpcServeHandle>,
}

impl MasterServer {
    pub fn new(orchestrator: Arc<dyn MasterOrchestrator>) -> Self {
        Self {
            service: MasterServiceImpl::new(orchestrator),
            serve: None,
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

    pub async fn lifecycle_events_since(&self, sequence: u64) -> Vec<LifecycleEventRecord> {
        self.service.master().lifecycle_events_since(sequence).await
    }

    pub fn subscribe_lifecycle_events(
        &self,
    ) -> tokio::sync::broadcast::Receiver<LifecycleEventRecord> {
        self.service.master().subscribe_lifecycle_events()
    }

    pub async fn start(&mut self, addr: &str) -> anyhow::Result<()> {
        crate::runtime::metrics::install_metrics_http_from_env()?;
        let addr = addr.parse()?;
        let service = master_server(self.service.clone());

        println!("[MASTER_SERVER] Starting MasterService server on {}", addr);

        self.serve = Some(spawn_with_shutdown(
            addr,
            server_builder().add_service(service),
        ));
        Ok(())
    }

    pub async fn stop(&mut self) {
        if let Some(mut serve) = self.serve.take() {
            serve.stop().await;
        }
    }
}

impl Drop for MasterServer {
    fn drop(&mut self) {
        if let Some(mut serve) = self.serve.take() {
            serve.abort();
        }
    }
}
