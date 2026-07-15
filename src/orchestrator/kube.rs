use std::collections::{HashMap, HashSet};
use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use reqwest::Client;
use serde_json::Value;
use tokio::sync::mpsc;
use tokio::time::{interval, MissedTickBehavior};

use crate::api::{KubePipelineSpec, PipelineSpec};
use crate::common::failure::{FailureEvent, FailureKind};

use super::orchestrator::{
    worker_health_poll_interval, MasterOrchestrator, WorkerHealthWatchHandle, WorkerNode,
    WorkerOrchestrator,
};

const VOLGA_CRD_GROUP: &str = "volga.io";
const VOLGA_CRD_VERSION: &str = "v1alpha1";
const VOLGA_CRD_PLURAL: &str = "volgapipelines";
/// Pipeline annotation; harness sets this so master can enable/disable Ready polling
/// without CRD/operator env plumbing. Env `VOLGA_KUBE_WORKER_HEALTH_POLL` overrides.
pub const KUBE_WORKER_HEALTH_POLL_ANNOTATION: &str = "volga.io/kube-worker-health-poll";
/// Pipeline annotation → master loads `runtime_consts.test.json` (same as `VOLGA_USE_TEST_CONSTS`).
pub const USE_TEST_CONSTS_ANNOTATION: &str = "volga.io/use-test-consts";
/// Consecutive unhealthy polls before emitting a failure (filters Ready flaps).
const WORKER_HEALTH_UNHEALTHY_GRACE_TICKS: u32 = 2;

fn parse_boolish(value: &str) -> Option<bool> {
    match value {
        "1" | "true" | "TRUE" | "yes" => Some(true),
        "0" | "false" | "FALSE" | "no" => Some(false),
        _ => None,
    }
}

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
        let status = resp.status();
        // Already gone is fine for replacement (STS may have recreated / test deleted first).
        if status.is_success() || status.as_u16() == 404 {
            return Ok(());
        }
        let body = resp.text().await.unwrap_or_default();
        Err(anyhow!("kube api DELETE failed: {} body={}", status, body))
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

enum PodHealth {
    Ready,
    Unhealthy(String),
}

fn describe_unhealthy_pod(pod: &Value) -> String {
    if json_get_path(pod, &["metadata", "deletionTimestamp"]).is_some() {
        return "pod terminating".to_string();
    }
    let phase = json_get_path(pod, &["status", "phase"])
        .and_then(|v| v.as_str())
        .unwrap_or("Unknown");
    if let Some(reason) = json_get_path(pod, &["status", "reason"]).and_then(|v| v.as_str()) {
        if !reason.is_empty() {
            return format!("phase={phase} reason={reason}");
        }
    }
    if let Some(statuses) = json_get_path(pod, &["status", "containerStatuses"]).and_then(|v| v.as_array())
    {
        for status in statuses {
            if let Some(terminated) = json_get_path(status, &["state", "terminated"]) {
                let reason = terminated
                    .get("reason")
                    .and_then(|v| v.as_str())
                    .unwrap_or("Terminated");
                let exit_code = terminated
                    .get("exitCode")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(-1);
                return format!("container terminated reason={reason} exit={exit_code}");
            }
            if let Some(waiting) = json_get_path(status, &["state", "waiting"]) {
                let reason = waiting
                    .get("reason")
                    .and_then(|v| v.as_str())
                    .unwrap_or("Waiting");
                if matches!(
                    reason,
                    "CrashLoopBackOff" | "ImagePullBackOff" | "ErrImagePull" | "CreateContainerError"
                ) {
                    return format!("container waiting reason={reason}");
                }
            }
        }
    }
    if phase != "Running" {
        return format!("phase={phase}");
    }
    "not ready".to_string()
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

    fn worker_id_from_pod(&self, pod: &Value) -> Option<String> {
        json_get_path(pod, &["metadata", "labels", &self.worker_id_label_key])
            .and_then(|v| v.as_str())
            .map(ToString::to_string)
            .or_else(|| {
                json_get_path(pod, &["metadata", "name"])
                    .and_then(|v| v.as_str())
                    .map(ToString::to_string)
            })
    }

    /// List matching worker pods and classify Ready vs unhealthy (includes non-ready pods).
    async fn inspect_worker_pod_health(&self) -> Result<HashMap<String, PodHealth>> {
        let path = format!("/api/v1/namespaces/{}/pods", self.api.namespace);
        let pods = self
            .api
            .get_json(
                &path,
                &[("labelSelector", self.worker_label_selector.as_str())],
            )
            .await?;
        let mut out = HashMap::new();
        if let Some(items) = pods.get("items").and_then(|v| v.as_array()) {
            for item in items {
                let Some(worker_id) = self.worker_id_from_pod(item) else {
                    continue;
                };
                let health = if worker_pod_ready(item) {
                    PodHealth::Ready
                } else {
                    PodHealth::Unhealthy(describe_unhealthy_pod(item))
                };
                out.insert(worker_id, health);
            }
        }
        Ok(out)
    }

    fn spawn_health_poll_task(
        &self,
        worker_ids: HashSet<String>,
        failure_tx: mpsc::Sender<FailureEvent>,
        poll_interval: Duration,
    ) -> WorkerHealthWatchHandle {
        let orch = self.clone();
        let join = tokio::spawn(async move {
            if !orch.worker_health_poll_enabled().await {
                println!("[MASTER] kube worker health poll disabled");
                return;
            }
            if worker_ids.is_empty() {
                return;
            }
            let mut ticker = interval(poll_interval);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            let mut unhealthy_ticks: HashMap<String, u32> = HashMap::new();
            let mut emitted: HashSet<String> = HashSet::new();

            loop {
                ticker.tick().await;
                let health = match orch.inspect_worker_pod_health().await {
                    Ok(h) => h,
                    Err(e) => {
                        println!("[MASTER] kube worker health poll failed: {e}");
                        continue;
                    }
                };

                for worker_id in &worker_ids {
                    if emitted.contains(worker_id) {
                        continue;
                    }
                    let detail = match health.get(worker_id) {
                        Some(PodHealth::Ready) => {
                            unhealthy_ticks.remove(worker_id);
                            continue;
                        }
                        Some(PodHealth::Unhealthy(detail)) => detail.clone(),
                        None => "pod missing".to_string(),
                    };
                    let ticks = unhealthy_ticks.entry(worker_id.clone()).or_insert(0);
                    *ticks = ticks.saturating_add(1);
                    if *ticks < WORKER_HEALTH_UNHEALTHY_GRACE_TICKS {
                        continue;
                    }
                    emitted.insert(worker_id.clone());
                    println!("[MASTER] kube worker unhealthy worker={worker_id} ({detail})");
                    if failure_tx
                        .send(FailureEvent {
                            worker_id: worker_id.clone(),
                            kind: FailureKind::PodUnhealthy,
                            detail,
                        })
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
        });
        WorkerHealthWatchHandle::new(join)
    }

    /// Env overrides annotation. Annotation absent → enabled (default).
    async fn worker_health_poll_enabled(&self) -> bool {
        if let Ok(v) = env::var("VOLGA_KUBE_WORKER_HEALTH_POLL") {
            if let Some(enabled) = parse_boolish(&v) {
                return enabled;
            }
        }
        match self.get_crd().await {
            Ok(crd) => {
                let annotation = json_get_path(
                    &crd,
                    &["metadata", "annotations", KUBE_WORKER_HEALTH_POLL_ANNOTATION],
                )
                .and_then(|v| v.as_str());
                match annotation.and_then(parse_boolish) {
                    Some(enabled) => enabled,
                    None => true,
                }
            }
            Err(e) => {
                println!(
                    "[MASTER] failed reading health-poll annotation, defaulting to enabled: {e}"
                );
                true
            }
        }
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
                if let Some(worker_id) = self.worker_id_from_pod(item) {
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

    fn run_health_poll(
        &self,
        worker_ids: HashSet<String>,
        failure_tx: mpsc::Sender<FailureEvent>,
    ) -> Option<WorkerHealthWatchHandle> {
        // Task checks env / pipeline annotation and no-ops when disabled.
        Some(self.spawn_health_poll_task(
            worker_ids,
            failure_tx,
            worker_health_poll_interval(),
        ))
    }

    async fn bootstrap(&self) -> Result<()> {
        // Env wins if already set on the pod; else pipeline annotation.
        if env::var("VOLGA_USE_TEST_CONSTS")
            .ok()
            .as_deref()
            .and_then(parse_boolish)
            == Some(true)
        {
            crate::runtime::consts::init_test_runtime_consts();
            return Ok(());
        }
        let crd = self.get_crd().await?;
        let enabled = json_get_path(
            &crd,
            &["metadata", "annotations", USE_TEST_CONSTS_ANNOTATION],
        )
        .and_then(|v| v.as_str())
        .and_then(parse_boolish)
        .unwrap_or(false);
        if enabled {
            crate::runtime::consts::init_test_runtime_consts();
        }
        Ok(())
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
