use anyhow::{anyhow, Result};

use crate::executor::bootstrap::WorkerBootstrapPayload;
use crate::runtime::master_server::master_service::master_service_client::MasterServiceClient;
use crate::runtime::master_server::master_service::GetWorkerBootstrapRequest;

pub async fn fetch_worker_bootstrap(master_addr: &str, worker_id: &str) -> Result<WorkerBootstrapPayload> {
    let mut client = MasterServiceClient::connect(format!("http://{}", master_addr)).await?;
    let resp = client
        .get_worker_bootstrap(tonic::Request::new(GetWorkerBootstrapRequest {
            worker_id: worker_id.to_string(),
        }))
        .await?
        .into_inner();

    if !resp.has_payload || resp.payload_bytes.is_empty() {
        return Err(anyhow!("worker bootstrap payload is not available yet"));
    }

    let payload: WorkerBootstrapPayload = bincode::deserialize(&resp.payload_bytes)?;
    Ok(payload)
}
