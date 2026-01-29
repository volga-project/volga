use std::collections::HashMap;
use std::time::Duration;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use base64::{engine::general_purpose, Engine as _};
use kube::{Api, Client, ResourceExt};
use kube::api::{DeleteParams, PostParams};
use k8s_openapi::api::apps::v1::{Deployment, StatefulSet};
use k8s_openapi::api::core::v1::Service;
use serde_json::json;
use tokio::time::sleep;

use crate::executor::placement::{
    build_task_placement_mapping,
    strategy_from_name,
    worker_to_task_ids,
    TaskPlacementStrategyName,
    WorkerEndpoint,
    WorkerTaskPlacement,
};
use crate::api::spec::resources::ResourceProfile;
use crate::executor::resource_planner::ResourcePlanner;
use crate::executor::runtime_adapter::{AttemptHandle, RuntimeAdapter, StartAttemptRequest};
use crate::runtime::bootstrap::WorkerBootstrapPayload;
use crate::api::spec::runtime_adapter::RuntimeAdapterKind;

pub struct K8sRuntimeConfig {
    pub namespace: String,
    pub master_image: String,
    pub worker_image: String,
    pub master_port: u16,
    pub worker_port: u16,
    pub master_labels: HashMap<String, String>,
    pub worker_labels: HashMap<String, String>,
    pub master_annotations: HashMap<String, String>,
    pub worker_annotations: HashMap<String, String>,
    pub master_node_selector: HashMap<String, String>,
    pub worker_node_selector: HashMap<String, String>,
    pub master_tolerations: Option<serde_json::Value>,
    pub worker_tolerations: Option<serde_json::Value>,
    pub master_affinity: Option<serde_json::Value>,
    pub worker_affinity: Option<serde_json::Value>,
}

impl Default for K8sRuntimeConfig {
    fn default() -> Self {
        Self {
            namespace: "default".to_string(),
            master_image: "volga_stream:latest".to_string(),
            worker_image: "volga_stream:latest".to_string(),
            master_port: 7000,
            worker_port: 7001,
            master_labels: HashMap::new(),
            worker_labels: HashMap::new(),
            master_annotations: HashMap::new(),
            worker_annotations: HashMap::new(),
            master_node_selector: HashMap::new(),
            worker_node_selector: HashMap::new(),
            master_tolerations: None,
            worker_tolerations: None,
            master_affinity: None,
            worker_affinity: None,
        }
    }
}

pub struct K8sRuntimeAdapter {
    client: Client,
    config: K8sRuntimeConfig,
}

impl K8sRuntimeAdapter {
    pub async fn new(config: K8sRuntimeConfig) -> Result<Self> {
        let client = Client::try_default().await?;
        Ok(Self { client, config })
    }

    fn encode_b64<T: serde::Serialize>(value: &T) -> Result<String> {
        let bytes = bincode::serialize(value)?;
        Ok(general_purpose::STANDARD.encode(bytes))
    }

    fn make_worker_nodes(
        worker_statefulset: &str,
        headless_service: &str,
        namespace: &str,
        worker_port: u16,
        replicas: usize,
    ) -> Vec<WorkerEndpoint> {
        (0..replicas)
            .map(|i| {
                let pod_name = format!("{worker_statefulset}-{i}");
                let addr = format!(
                    "{}.{}.{}.svc.cluster.local",
                    pod_name, headless_service, namespace
                );
                WorkerEndpoint::new(pod_name, addr, worker_port)
            })
            .collect()
    }

    fn resource_requirements(profile: &ResourceProfile) -> Option<serde_json::Value> {
        let mut limits = serde_json::Map::new();
        let mut requests = serde_json::Map::new();

        if let Some(cpu) = &profile.cpu_limit {
            limits.insert("cpu".to_string(), json!(cpu));
        }
        if let Some(mem) = &profile.mem_limit {
            limits.insert("memory".to_string(), json!(mem));
        }
        if let Some(cpu) = &profile.cpu_request {
            requests.insert("cpu".to_string(), json!(cpu));
        }
        if let Some(mem) = &profile.mem_request {
            requests.insert("memory".to_string(), json!(mem));
        }

        if limits.is_empty() && requests.is_empty() {
            return None;
        }

        let mut out = serde_json::Map::new();
        if !limits.is_empty() {
            out.insert("limits".to_string(), serde_json::Value::Object(limits));
        }
        if !requests.is_empty() {
            out.insert("requests".to_string(), serde_json::Value::Object(requests));
        }
        Some(serde_json::Value::Object(out))
    }

    fn master_service_name(base: &str) -> String {
        format!("{base}-master")
    }

    fn worker_service_name(base: &str) -> String {
        format!("{base}-worker")
    }

    fn worker_statefulset_name(base: &str) -> String {
        format!("{base}-workers")
    }

    async fn wait_for_statefulset_ready(
        &self,
        api: &Api<StatefulSet>,
        name: &str,
        replicas: usize,
    ) -> Result<()> {
        for _ in 0..60 {
            if let Ok(ss) = api.get(name).await {
                let ready = ss.status.and_then(|s| s.ready_replicas).unwrap_or(0) as usize;
                if ready >= replicas {
                    return Ok(());
                }
            }
            sleep(Duration::from_secs(1)).await;
        }
        Err(anyhow!("statefulset {name} did not become ready"))
    }

    async fn wait_for_deployment_ready(&self, api: &Api<Deployment>, name: &str) -> Result<()> {
        for _ in 0..60 {
            if let Ok(dep) = api.get(name).await {
                let ready = dep.status.and_then(|s| s.ready_replicas).unwrap_or(0);
                if ready > 0 {
                    return Ok(());
                }
            }
            sleep(Duration::from_secs(1)).await;
        }
        Err(anyhow!("deployment {name} did not become ready"))
    }

    fn base_name(execution_ids: &crate::control_plane::types::ExecutionIds) -> String {
        format!(
            "volga-{}-{}",
            execution_ids.pipeline_id.0.to_string(),
            execution_ids.attempt_id.0
        )
        .to_lowercase()
    }

    fn merge_labels(
        base: serde_json::Map<String, serde_json::Value>,
        extra: &HashMap<String, String>,
    ) -> serde_json::Map<String, serde_json::Value> {
        let mut labels = base;
        for (k, v) in extra {
            labels.insert(k.clone(), json!(v));
        }
        labels
    }
}

#[async_trait]
impl RuntimeAdapter for K8sRuntimeAdapter {
    async fn start_attempt(&self, mut req: StartAttemptRequest) -> Result<AttemptHandle> {
        let task_placement_strategy = req
            .pipeline_spec
            .execution_profile
            .task_placement_strategy()
            .cloned()
            .unwrap_or_else(|| panic!("execution profile does not define task placement strategy"));
        let resource_strategy = req
            .pipeline_spec
            .execution_profile
            .resource_strategy()
            .cloned()
            .unwrap_or_else(|| panic!("execution profile does not define resource strategy"));
        if !self
            .supported_task_placement_strategies()
            .iter()
            .any(|s| s == &task_placement_strategy)
        {
            panic!(
                "K8sRuntimeAdapter does not support task placement strategy {:?}",
                task_placement_strategy
            );
        }

        let base = Self::base_name(&req.execution_ids);
        let master_service_name = Self::master_service_name(&base);
        let worker_service_name = Self::worker_service_name(&base);
        let worker_statefulset = Self::worker_statefulset_name(&base);

        let placement_strategy = strategy_from_name(&task_placement_strategy);
        let mut placements = placement_strategy.place_tasks(&req.execution_graph);
        if placements.is_empty() {
            placements.push(WorkerTaskPlacement::new(Vec::new()));
        }

        let resource_plan = ResourcePlanner::plan(
            &placements,
            &resource_strategy,
            &req.pipeline_spec.resource_profiles,
        );
        let replicas = resource_plan.worker_resources.len().max(1);
        let worker_endpoints = Self::make_worker_nodes(
            &worker_statefulset,
            &worker_service_name,
            &self.config.namespace,
            self.config.worker_port,
            replicas,
        );
        let task_placement_mapping =
            build_task_placement_mapping(&placements, &worker_endpoints);
        let worker_task_ids = worker_to_task_ids(&task_placement_mapping);

        let worker_resource = resource_plan
            .worker_resources
            .get(0)
            .cloned()
            .unwrap_or_default();
        let worker_resources_json = Self::resource_requirements(&worker_resource);

        let bootstrap_payload = WorkerBootstrapPayload {
            execution_ids: req.execution_ids.clone(),
            pipeline_spec: req.pipeline_spec.clone(),
            worker_endpoints: worker_endpoints.clone(),
            worker_task_ids: worker_task_ids.clone(),
            transport_overrides_queue_records: req.transport_overrides_queue_records.clone(),
            worker_runtime: req.worker_runtime.clone(),
            operator_type_storage_overrides: req.operator_type_storage_overrides.clone(),
        };

        let bootstrap_b64 = Self::encode_b64(&bootstrap_payload)?;
        let worker_addrs = worker_endpoints
            .iter()
            .map(|n| format!("{}:{}", n.host, n.port))
            .collect::<Vec<_>>();

        let master_addr = format!("{}.{}:{}", master_service_name, self.config.namespace, self.config.master_port);

        let master_env = vec![
            json!({"name": "VOLGA_MASTER_BIND_ADDR", "value": format!("0.0.0.0:{}", self.config.master_port)}),
            json!({"name": "VOLGA_WORKER_ADDRS", "value": worker_addrs.join(",")}),
            json!({"name": "VOLGA_BOOTSTRAP_B64", "value": bootstrap_b64}),
        ];

        let base_labels = json!({
            "app": "volga",
            "pipeline_id": req.execution_ids.pipeline_id.0.to_string(),
            "attempt_id": format!("{}", req.execution_ids.attempt_id.0),
        })
        .as_object()
        .expect("labels map")
        .clone();
        let master_labels = Self::merge_labels(base_labels.clone(), &self.config.master_labels);
        let worker_labels = Self::merge_labels(base_labels, &self.config.worker_labels);

        let mut master_pod_spec = json!({
            "containers": [{
                "name": "master",
                "image": self.config.master_image,
                "command": ["/volga_master"],
                "env": master_env,
                "ports": [{
                    "containerPort": self.config.master_port,
                    "name": "grpc"
                }]
            }]
        });
        if !self.config.master_node_selector.is_empty() {
            master_pod_spec["nodeSelector"] = json!(self.config.master_node_selector);
        }
        if let Some(tolerations) = &self.config.master_tolerations {
            master_pod_spec["tolerations"] = tolerations.clone();
        }
        if let Some(affinity) = &self.config.master_affinity {
            master_pod_spec["affinity"] = affinity.clone();
        }

        let master_deployment: Deployment = serde_json::from_value(json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": master_service_name,
                "namespace": self.config.namespace,
                "labels": master_labels,
                "annotations": self.config.master_annotations,
            },
            "spec": {
                "replicas": 1,
                "selector": { "matchLabels": master_labels },
                "template": {
                    "metadata": { "labels": master_labels, "annotations": self.config.master_annotations },
                    "spec": master_pod_spec
                }
            }
        }))?;

        let master_service: Service = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": master_service_name,
                "namespace": self.config.namespace,
                "labels": master_labels,
                "annotations": self.config.master_annotations,
            },
            "spec": {
                "selector": master_labels,
                "ports": [{
                    "port": self.config.master_port,
                    "targetPort": self.config.master_port,
                    "name": "grpc"
                }]
            }
        }))?;

        let worker_service: Service = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": worker_service_name,
                "namespace": self.config.namespace,
                "labels": worker_labels,
                "annotations": self.config.worker_annotations,
            },
            "spec": {
                "clusterIP": "None",
                "selector": worker_labels,
                "ports": [{
                    "port": self.config.worker_port,
                    "targetPort": self.config.worker_port,
                    "name": "grpc"
                }]
            }
        }))?;

        let mut worker_container = json!({
            "name": "worker",
            "image": self.config.worker_image,
            "command": ["/volga_worker"],
            "env": [
                {"name": "VOLGA_MASTER_ADDR", "value": master_addr},
                {"name": "VOLGA_WORKER_PORT", "value": format!("{}", self.config.worker_port)},
                {"name": "VOLGA_WORKER_BIND_ADDR", "value": format!("0.0.0.0:{}", self.config.worker_port)},
                {
                    "name": "VOLGA_WORKER_ID",
                    "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}
                }
            ],
            "ports": [{
                "containerPort": self.config.worker_port,
                "name": "grpc"
            }]
        });
        if let Some(resources) = worker_resources_json {
            worker_container["resources"] = resources;
        }

        let mut worker_pod_spec = json!({
            "containers": [worker_container]
        });
        if !self.config.worker_node_selector.is_empty() {
            worker_pod_spec["nodeSelector"] = json!(self.config.worker_node_selector);
        }
        if let Some(tolerations) = &self.config.worker_tolerations {
            worker_pod_spec["tolerations"] = tolerations.clone();
        }
        if let Some(affinity) = &self.config.worker_affinity {
            worker_pod_spec["affinity"] = affinity.clone();
        }

        let worker_statefulset_obj: StatefulSet = serde_json::from_value(json!({
            "apiVersion": "apps/v1",
            "kind": "StatefulSet",
            "metadata": {
                "name": worker_statefulset,
                "namespace": self.config.namespace,
                "labels": worker_labels,
                "annotations": self.config.worker_annotations,
            },
            "spec": {
                "serviceName": worker_service_name,
                "replicas": replicas,
                "selector": { "matchLabels": worker_labels },
                "template": {
                    "metadata": { "labels": worker_labels, "annotations": self.config.worker_annotations },
                    "spec": worker_pod_spec
                }
            }
        }))?;

        let deployments: Api<Deployment> = Api::namespaced(self.client.clone(), &self.config.namespace);
        let services: Api<Service> = Api::namespaced(self.client.clone(), &self.config.namespace);
        let statefulsets: Api<StatefulSet> = Api::namespaced(self.client.clone(), &self.config.namespace);

        services.create(&PostParams::default(), &master_service).await?;
        deployments.create(&PostParams::default(), &master_deployment).await?;
        services.create(&PostParams::default(), &worker_service).await?;
        statefulsets.create(&PostParams::default(), &worker_statefulset_obj).await?;

        self.wait_for_deployment_ready(&deployments, &master_service_name).await?;
        self.wait_for_statefulset_ready(&statefulsets, &worker_statefulset, replicas).await?;

        let execution_ids = req.execution_ids.clone();
        let (stop_sender, stop_receiver) = tokio::sync::oneshot::channel();
        let join = tokio::spawn(async move {
            let _ = stop_receiver.await;
            Ok(crate::runtime::observability::PipelineSnapshot::new(HashMap::new()))
        });

        Ok(AttemptHandle {
            execution_ids,
            master_addr,
            worker_addrs,
            join,
            stop_sender: Some(stop_sender),
        })
    }

    async fn stop_attempt(&self, handle: AttemptHandle) -> Result<()> {
        let base = Self::base_name(&handle.execution_ids);
        let master_service_name = Self::master_service_name(&base);
        let worker_service_name = Self::worker_service_name(&base);
        let worker_statefulset = Self::worker_statefulset_name(&base);

        let deployments: Api<Deployment> = Api::namespaced(self.client.clone(), &self.config.namespace);
        let services: Api<Service> = Api::namespaced(self.client.clone(), &self.config.namespace);
        let statefulsets: Api<StatefulSet> = Api::namespaced(self.client.clone(), &self.config.namespace);

        let _ = deployments.delete(&master_service_name, &DeleteParams::default()).await;
        let _ = services.delete(&master_service_name, &DeleteParams::default()).await;
        let _ = statefulsets.delete(&worker_statefulset, &DeleteParams::default()).await;
        let _ = services.delete(&worker_service_name, &DeleteParams::default()).await;

        if let Some(stop_sender) = handle.stop_sender {
            let _ = stop_sender.send(());
        } else {
            handle.abort();
        }
        Ok(())
    }

    fn supported_task_placement_strategies(&self) -> &[TaskPlacementStrategyName] {
        static SUPPORTED: [TaskPlacementStrategyName; 2] = [
            TaskPlacementStrategyName::SingleNode,
            TaskPlacementStrategyName::OperatorPerNode,
        ];
        &SUPPORTED
    }

    fn runtime_kind(&self) -> RuntimeAdapterKind {
        RuntimeAdapterKind::K8s
    }
}
