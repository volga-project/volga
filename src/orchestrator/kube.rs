use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use reqwest::Client;
use serde_json::Value;

use crate::api::{KubePipelineSpec, PipelineSpec};

use super::orchestrator::{MasterOrchestrator, WorkerNode, WorkerOrchestrator};

const VOLGA_CRD_GROUP: &str = "volga.io";
const VOLGA_CRD_VERSION: &str = "v1alpha1";
const VOLGA_CRD_PLURAL: &str = "volgapipelines";

#[derive(Clone)]
struct KubeApiClient {
    base_url: String,
    namespace: String,
    client: Client,
}

impl KubeApiClient {
    fn in_cluster(namespace: String) -> Result<Self> {
        let base_url = env::var("KUBE_API_SERVER")
            .unwrap_or_else(|_| panic!("KUBE_API_SERVER is required for kube orchestrator"));
        let token_path = PathBuf::from("/var/run/secrets/kubernetes.io/serviceaccount/token");
        let ca_path = PathBuf::from("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt");
        let token = fs::read_to_string(&token_path)
            .with_context(|| format!("failed reading serviceaccount token: {:?}", token_path))?;
        let ca_bytes = fs::read(&ca_path)
            .with_context(|| format!("failed reading serviceaccount CA cert: {:?}", ca_path))?;
        let cert = reqwest::Certificate::from_pem(&ca_bytes)
            .context("failed to parse serviceaccount CA cert")?;
        let mut headers = HeaderMap::new();
        let bearer = format!("Bearer {}", token.trim());
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&bearer).context("invalid bearer token header")?,
        );
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        let client = Client::builder()
            .add_root_certificate(cert)
            .default_headers(headers)
            .build()
            .context("failed building kube reqwest client")?;
        Ok(Self {
            base_url,
            namespace,
            client,
        })
    }

    async fn get_json(&self, path: &str, query: &[(&str, &str)]) -> Result<Value> {
        let base = format!("{}{}", self.base_url.trim_end_matches('/'), path);
        let mut url = reqwest::Url::parse(&base)
            .with_context(|| format!("invalid kube api URL: {}", base))?;
        {
            let mut qp = url.query_pairs_mut();
            for (k, v) in query {
                qp.append_pair(k, v);
            }
        }
        let resp = self
            .client
            .get(url.clone())
            .send()
            .await
            .with_context(|| format!("kube api GET failed: {}", url))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!("kube api GET failed: {} body={}", status, body));
        }
        let value = resp
            .json::<Value>()
            .await
            .context("failed to decode kube api response JSON")?;
        Ok(value)
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let url = format!("{}{}", self.base_url.trim_end_matches('/'), path);
        let resp = self
            .client
            .delete(&url)
            .send()
            .await
            .with_context(|| format!("kube api DELETE failed: {}", url))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!("kube api DELETE failed: {} body={}", status, body));
        }
        Ok(())
    }

    async fn patch_json(&self, path: &str, body: &Value) -> Result<()> {
        let url = format!("{}{}", self.base_url.trim_end_matches('/'), path);
        let resp = self
            .client
            .patch(&url)
            .header(CONTENT_TYPE, "application/merge-patch+json")
            .json(body)
            .send()
            .await
            .with_context(|| format!("kube api PATCH failed: {}", url))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!("kube api PATCH failed: {} body={}", status, body));
        }
        Ok(())
    }
}

fn json_get_path<'a>(value: &'a Value, path: &[&str]) -> Option<&'a Value> {
    let mut cur = value;
    for key in path {
        cur = cur.get(*key)?;
    }
    Some(cur)
}

fn json_get_string(value: &Value, candidate_paths: &[&[&str]]) -> Option<String> {
    candidate_paths.iter().find_map(|path| {
        json_get_path(value, path)
            .and_then(|v| v.as_str())
            .map(ToString::to_string)
    })
}

fn json_get_usize(value: &Value, candidate_paths: &[&[&str]]) -> Option<usize> {
    candidate_paths.iter().find_map(|path| {
        json_get_path(value, path)
            .and_then(|v| v.as_u64())
            .map(|v| v as usize)
    })
}

/// Usable for discovery: Running, Ready, not terminating, has podIP.
fn worker_pod_ready(pod: &Value) -> bool {
    if json_get_path(pod, &["metadata", "deletionTimestamp"]).is_some() {
        return false;
    }
    let phase = json_get_path(pod, &["status", "phase"])
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if phase != "Running" {
        return false;
    }
    let ready = json_get_path(pod, &["status", "conditions"])
        .and_then(|v| v.as_array())
        .map(|conds| {
            conds.iter().any(|c| {
                c.get("type").and_then(|t| t.as_str()) == Some("Ready")
                    && c.get("status").and_then(|s| s.as_str()) == Some("True")
            })
        })
        .unwrap_or(false);
    if !ready {
        return false;
    }
    json_get_path(pod, &["status", "podIP"])
        .and_then(|v| v.as_str())
        .map(|ip| !ip.is_empty())
        .unwrap_or(false)
}

fn build_api_from_env() -> Result<Arc<KubeApiClient>> {
    let namespace = env::var("KUBE_NAMESPACE")
        .or_else(|_| env::var("POD_NAMESPACE"))
        .unwrap_or_else(|_| {
            panic!("KUBE_NAMESPACE or POD_NAMESPACE is required for kube orchestrator")
        });
    Ok(Arc::new(KubeApiClient::in_cluster(namespace)?))
}

#[derive(Clone)]
pub struct KubeMasterOrchestrator {
    api: Arc<KubeApiClient>,
    crd_name: String,
    worker_label_selector: String,
    worker_id_label_key: String,
    worker_port: u16,
    transport_port: u16,
}

#[derive(Clone)]
pub struct KubeWorkerOrchestrator {
    master_service_addr: String,
    worker_id: String,
}

impl KubeMasterOrchestrator {
    pub fn from_env() -> Result<Self> {
        let api = build_api_from_env()?;
        let crd_name = env::var("VOLGA_PIPELINE_CRD_NAME")
            .unwrap_or_else(|_| panic!("VOLGA_PIPELINE_CRD_NAME is required"));
        let worker_label_selector = env::var("VOLGA_WORKER_LABEL_SELECTOR")
            .unwrap_or_else(|_| panic!("VOLGA_WORKER_LABEL_SELECTOR is required"));
        let worker_id_label_key = env::var("VOLGA_WORKER_ID_LABEL")
            .unwrap_or_else(|_| panic!("VOLGA_WORKER_ID_LABEL is required"));
        let worker_port = env::var("VOLGA_WORKER_PORT")
            .unwrap_or_else(|_| panic!("VOLGA_WORKER_PORT is required"))
            .parse::<u16>()
            .unwrap_or_else(|e| panic!("failed to parse VOLGA_WORKER_PORT as u16: {e}"));
        let transport_port = env::var("VOLGA_WORKER_TRANSPORT_PORT")
            .unwrap_or_else(|_| panic!("VOLGA_WORKER_TRANSPORT_PORT is required"))
            .parse::<u16>()
            .unwrap_or_else(|e| panic!("failed to parse VOLGA_WORKER_TRANSPORT_PORT as u16: {e}"));
        Ok(Self {
            api,
            crd_name,
            worker_label_selector,
            worker_id_label_key,
            worker_port,
            transport_port,
        })
    }

    fn crd_path(&self) -> String {
        format!(
            "/apis/{}/{}/namespaces/{}/{}/{}",
            VOLGA_CRD_GROUP, VOLGA_CRD_VERSION, self.api.namespace, VOLGA_CRD_PLURAL, self.crd_name
        )
    }

    async fn get_crd(&self) -> Result<Value> {
        self.api.get_json(&self.crd_path(), &[]).await
    }
}

impl KubeWorkerOrchestrator {
    pub fn from_env() -> Result<Self> {
        let master_service_addr = env::var("MASTER_SERVICE_ADDR")
            .or_else(|_| env::var("VOLGA_MASTER_SERVICE_ADDR"))
            .context("MASTER_SERVICE_ADDR (or VOLGA_MASTER_SERVICE_ADDR) is required")?;
        let worker_id = env::var("VOLGA_WORKER_ID")
            .context("VOLGA_WORKER_ID is required to resolve kube worker id")?;
        Ok(Self {
            master_service_addr,
            worker_id,
        })
    }
}

#[async_trait]
impl MasterOrchestrator for KubeMasterOrchestrator {
    async fn get_worker_nodes(&self) -> HashMap<String, WorkerNode> {
        let path = format!("/api/v1/namespaces/{}/pods", self.api.namespace);
        let pods = match self
            .api
            .get_json(
                &path,
                &[("labelSelector", self.worker_label_selector.as_str())],
            )
            .await
        {
            Ok(v) => v,
            Err(e) => panic!("failed to discover worker pods from kube api: {}", e),
        };
        let mut out = HashMap::new();
        if let Some(items) = pods.get("items").and_then(|v| v.as_array()) {
            for item in items {
                if !worker_pod_ready(item) {
                    continue;
                }
                let pod_ip = json_get_path(item, &["status", "podIP"])
                    .and_then(|v| v.as_str())
                    .unwrap_or_default();
                let worker_id =
                    json_get_path(item, &["metadata", "labels", &self.worker_id_label_key])
                        .and_then(|v| v.as_str())
                        .map(ToString::to_string)
                        .or_else(|| {
                            json_get_path(item, &["metadata", "name"])
                                .and_then(|v| v.as_str())
                                .map(ToString::to_string)
                        });
                if let Some(worker_id) = worker_id {
                    out.insert(
                        worker_id.clone(),
                        WorkerNode::new(
                            worker_id,
                            pod_ip.to_string(),
                            self.worker_port,
                            self.transport_port,
                        ),
                    );
                }
            }
        }
        out
    }

    async fn get_pipeline_id(&self) -> String {
        let crd = match self.get_crd().await {
            Ok(v) => v,
            Err(e) => panic!("failed to fetch pipeline CRD from kube api: {}", e),
        };
        json_get_string(&crd, &[&["spec", "pipelineId"], &["status", "pipelineId"]])
            .unwrap_or_else(|| panic!("pipelineId not found in CRD {}", self.crd_name))
    }

    async fn get_spec(&self) -> PipelineSpec {
        let crd = match self.get_crd().await {
            Ok(v) => v,
            Err(e) => panic!("failed to fetch pipeline CRD from kube api: {}", e),
        };
        let spec_json = json_get_path(&crd, &["spec", "pipelineSpec"])
            .or_else(|| json_get_path(&crd, &["status", "pipelineSpec"]))
            .unwrap_or_else(|| panic!("pipelineSpec not found in CRD {}", self.crd_name));
        let kube_spec: KubePipelineSpec = serde_json::from_value(spec_json.clone())
            .unwrap_or_else(|e| panic!("failed to deserialize pipelineSpec from CRD: {}", e));
        PipelineSpec::try_from(kube_spec)
            .unwrap_or_else(|e| panic!("failed to convert pipelineSpec from CRD: {}", e))
    }

    async fn get_num_expected_workers(&self) -> usize {
        let crd = match self.get_crd().await {
            Ok(v) => v,
            Err(e) => panic!("failed to fetch pipeline CRD from kube api: {}", e),
        };
        if let Some(v) = json_get_usize(
            &crd,
            &[
                &["spec", "expectedWorkers"],
                &["spec", "workers", "replicas"],
            ],
        ) {
            return v;
        }
        let nodes = self.get_worker_nodes().await;
        nodes.len()
    }

    /// Delete the pods backing the given worker ids so the StatefulSet recreates them.
    /// The master then re-discovers the replacements via `get_worker_nodes` polling.
    async fn request_replacement(&self, worker_ids: &[String]) -> Result<()> {
        if worker_ids.is_empty() {
            return Ok(());
        }
        let target: std::collections::HashSet<&str> =
            worker_ids.iter().map(|s| s.as_str()).collect();
        let list_path = format!("/api/v1/namespaces/{}/pods", self.api.namespace);
        let pods = self
            .api
            .get_json(
                &list_path,
                &[("labelSelector", self.worker_label_selector.as_str())],
            )
            .await?;

        let mut deleted = 0usize;
        if let Some(items) = pods.get("items").and_then(|v| v.as_array()) {
            for item in items {
                let worker_id =
                    json_get_path(item, &["metadata", "labels", &self.worker_id_label_key])
                        .and_then(|v| v.as_str())
                        .or_else(|| {
                            json_get_path(item, &["metadata", "name"]).and_then(|v| v.as_str())
                        });
                let pod_name = json_get_path(item, &["metadata", "name"]).and_then(|v| v.as_str());
                if let (Some(worker_id), Some(pod_name)) = (worker_id, pod_name) {
                    if target.contains(worker_id) {
                        let delete_path = format!(
                            "/api/v1/namespaces/{}/pods/{}",
                            self.api.namespace, pod_name
                        );
                        self.api.delete(&delete_path).await?;
                        deleted += 1;
                        println!(
                            "[MASTER] Requested replacement: deleted pod {} (worker_id={})",
                            pod_name, worker_id
                        );
                    }
                }
            }
        }
        if deleted == 0 {
            println!(
                "[MASTER] request_replacement: no pods matched worker_ids={:?}",
                worker_ids
            );
        }
        Ok(())
    }

    async fn record_lifecycle_event(&self, sequence: u64, event_json: &str) -> Result<()> {
        const MAX_STATUS_EVENTS: usize = 64;

        let crd = self.get_crd().await?;
        let mut events = json_get_path(&crd, &["status", "lifecycleEvents"])
            .and_then(|value| value.as_array())
            .cloned()
            .unwrap_or_default();
        let event = serde_json::from_str(event_json)
            .unwrap_or_else(|_| Value::String(event_json.to_string()));
        events.push(serde_json::json!({
            "sequence": sequence,
            "event": event,
        }));
        if events.len() > MAX_STATUS_EVENTS {
            events.drain(..events.len() - MAX_STATUS_EVENTS);
        }
        self.api
            .patch_json(
                &format!("{}/status", self.crd_path()),
                &serde_json::json!({
                    "status": {
                        "lifecycleEvents": events,
                    }
                }),
            )
            .await
    }
}

#[async_trait]
impl WorkerOrchestrator for KubeWorkerOrchestrator {
    async fn get_master_service_addr(&self) -> String {
        self.master_service_addr.clone()
    }

    async fn resolve_worker_id(&self) -> Result<String> {
        Ok(self.worker_id.clone())
    }
}
