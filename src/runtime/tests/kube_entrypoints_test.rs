use std::fs;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use arrow::array::StringArray;
use uuid::Uuid;

use crate::api::{KubePipelineSpec, PipelineSpec};
use crate::common::test_utils::gen_unique_grpc_port;
use crate::storage::InMemoryStorageClient;

const NUM_WORKERS: usize = 3;
const SLOTS_PER_NODE: usize = 2;
const PARALLELISM: usize = NUM_WORKERS * SLOTS_PER_NODE;
const NUM_BATCHES: usize = 2;
const BATCH_SIZE: usize = 5;
const EXPECTED_RECORDS_PER_TASK: usize = NUM_BATCHES * BATCH_SIZE;
const EXPECTED_RECORDS: usize = EXPECTED_RECORDS_PER_TASK * PARALLELISM;
const TEST_NAMESPACE: &str = "default";
const TEST_STORAGE_SERVICE: &str = "volga-test-storage";
const TEST_STORAGE_PORT: u16 = 50071;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn run_kubectl(args: &[&str]) -> Result<String> {
    println!("[KUBE_SMOKE] kubectl {:?}", args);
    let output = Command::new("kubectl")
        .args(args)
        .output()
        .with_context(|| format!("failed to execute kubectl {:?}", args))?;
    if !output.status.success() {
        bail!(
            "kubectl {:?} failed: status={} stdout={} stderr={}",
            args,
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    if !stdout.trim().is_empty() {
        println!("[KUBE_SMOKE] kubectl stdout: {}", stdout.trim());
    }
    Ok(stdout)
}

fn apply_test_storage_manifest() -> Result<()> {
    println!("[KUBE_SMOKE] applying test storage manifests");
    let manifest_dir = repo_root().join("kubevolga/config/test-storage");
    println!("[KUBE_SMOKE] deleting existing test storage deployment (if present)");
    run_kubectl(&[
        "-n",
        TEST_NAMESPACE,
        "delete",
        "deployment/volga-test-storage",
        "--ignore-not-found=true",
    ])?;
    run_kubectl(&["apply", "-k", manifest_dir.to_str().unwrap()])?;
    println!("[KUBE_SMOKE] waiting for test storage rollout");
    run_kubectl(&[
        "-n",
        TEST_NAMESPACE,
        "rollout",
        "status",
        "deployment/volga-test-storage",
        "--timeout=120s",
    ])?;
    Ok(())
}

fn cleanup_pipeline(name: &str) {
    println!("[KUBE_SMOKE] cleanup pipeline {}", name);
    let _ = run_kubectl(&[
        "-n",
        TEST_NAMESPACE,
        "delete",
        "volgapipeline",
        name,
        "--ignore-not-found=true",
    ]);
}

async fn wait_for_pipeline_phase(name: &str, expected_phase: &str, timeout: Duration) -> Result<()> {
    let start = tokio::time::Instant::now();
    let mut last_log = tokio::time::Instant::now();
    loop {
        let phase = run_kubectl(&[
            "-n",
            TEST_NAMESPACE,
            "get",
            "volgapipeline",
            name,
            "-o",
            "jsonpath={.status.phase}",
        ])?
        .trim()
        .to_string();
        if phase == expected_phase {
            println!(
                "[KUBE_SMOKE] pipeline {} reached phase {}",
                name, expected_phase
            );
            return Ok(());
        }
        if phase == "InvalidSpec" {
            bail!("pipeline entered InvalidSpec phase");
        }
        if start.elapsed() > timeout {
            bail!(
                "timed out waiting for phase {}, got {}",
                expected_phase,
                phase
            );
        }
        if last_log.elapsed() >= Duration::from_secs(5) {
            println!(
                "[KUBE_SMOKE] waiting for phase {}: current={}",
                expected_phase, phase
            );
            last_log = tokio::time::Instant::now();
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

fn start_port_forward(local_port: u16) -> Result<Child> {
    println!(
        "[KUBE_SMOKE] starting port-forward {} -> svc/{}:{}",
        local_port, TEST_STORAGE_SERVICE, TEST_STORAGE_PORT
    );
    let child = Command::new("kubectl")
        .args([
            "-n",
            TEST_NAMESPACE,
            "port-forward",
            &format!("svc/{}", TEST_STORAGE_SERVICE),
            &format!("{}:{}", local_port, TEST_STORAGE_PORT),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .context("failed to start kubectl port-forward")?;
    Ok(child)
}

fn configure_pipeline_crd(name: &str, sink_addr: &str) -> Result<serde_json::Value> {
    println!(
        "[KUBE_SMOKE] configuring pipeline CRD name={} sink_addr={}",
        name, sink_addr
    );
    let sample_path = repo_root().join("kubevolga/config/samples/volga_v1alpha1_pipeline.yaml");
    let sample = fs::read_to_string(&sample_path)
        .with_context(|| format!("read sample pipeline manifest at {}", sample_path.display()))?;
    let mut doc: serde_json::Value =
        serde_yaml::from_str(&sample).context("parse sample pipeline manifest YAML")?;

    doc["metadata"]["name"] = serde_json::Value::String(name.to_string());
    doc["metadata"]["namespace"] = serde_json::Value::String(TEST_NAMESPACE.to_string());
    doc["spec"]["pipelineSpec"]["parallelism"] = serde_json::Value::Number(PARALLELISM.into());
    doc["spec"]["pipelineSpec"]["task_assignment_strategy"] = serde_json::json!({
        "Pipelined": { "slots_per_node": SLOTS_PER_NODE }
    });

    let sink = doc["spec"]["pipelineSpec"]["sink"]["InMemoryStorageGrpc"]
        .as_str()
        .context("expected sink.InMemoryStorageGrpc to be JSON string")?;
    let mut sink_json: serde_json::Value =
        serde_json::from_str(sink).context("parse embedded sink JSON")?;
    sink_json["server_addr"] = serde_json::Value::String(sink_addr.to_string());
    doc["spec"]["pipelineSpec"]["sink"]["InMemoryStorageGrpc"] = serde_json::Value::String(
        serde_json::to_string(&sink_json).context("serialize embedded sink JSON")?,
    );
    let datagen = doc["spec"]["pipelineSpec"]["sources"][0]["source"]["Datagen"]
        .as_str()
        .context("expected sources[0].source.Datagen to be JSON string")?;
    let mut datagen_json: serde_json::Value =
        serde_json::from_str(datagen).context("parse embedded Datagen JSON")?;
    datagen_json["rate"] = serde_json::Value::Null;
    datagen_json["limit"] = serde_json::Value::Number(EXPECTED_RECORDS.into());
    datagen_json["batch_size"] = serde_json::Value::Number(BATCH_SIZE.into());
    datagen_json["fields"]["value"]["Key"]["num_unique"] =
        serde_json::Value::Number(PARALLELISM.into());
    doc["spec"]["pipelineSpec"]["sources"][0]["source"]["Datagen"] = serde_json::Value::String(
        serde_json::to_string(&datagen_json).context("serialize embedded Datagen JSON")?,
    );

    let kube_spec: KubePipelineSpec = serde_json::from_value(doc["spec"]["pipelineSpec"].clone())
        .context("deserialize sample pipelineSpec as KubePipelineSpec")?;
    let spec: PipelineSpec = kube_spec
        .try_into()
        .context("convert KubePipelineSpec into runtime PipelineSpec")?;

    let expected_workers = NUM_WORKERS;
    println!("[KUBE_SMOKE] expected worker replicas for pipelined test: {}", expected_workers);
    doc["spec"]["workers"]["replicas"] = serde_json::Value::Number(expected_workers.into());
    Ok(doc)
}

fn write_pipeline_cr(name: &str, doc: &serde_json::Value) -> Result<PathBuf> {
    let path = std::env::temp_dir().join(format!("{}-pipeline.json", name));
    fs::write(&path, serde_json::to_vec_pretty(&doc)?).context("write pipeline CR file")?;
    println!("[KUBE_SMOKE] wrote pipeline CR to {}", path.display());
    Ok(path)
}

// Manual smoke test: requires a running kind cluster + deployed kubevolga operator.
#[tokio::test]
#[ignore]
async fn test_kube_master_and_workers_smoke() -> Result<()> {
    println!("[KUBE_SMOKE] test start");
    apply_test_storage_manifest()?;

    let local_storage_port = gen_unique_grpc_port();
    let mut port_forward = start_port_forward(local_storage_port)?;
    let storage_addr_local = format!("http://127.0.0.1:{}", local_storage_port);
    let sink_addr_in_cluster = format!(
        "http://{}.{}.svc.cluster.local:{}",
        TEST_STORAGE_SERVICE, TEST_NAMESPACE, TEST_STORAGE_PORT
    );

    let pipeline_name = format!("kube-smoke-{}", Uuid::new_v4().simple());
    let pipeline_doc = configure_pipeline_crd(&pipeline_name, &sink_addr_in_cluster)?;
    let cr_path = write_pipeline_cr(&pipeline_name, &pipeline_doc)?;

    cleanup_pipeline(&pipeline_name);
    println!(
        "[KUBE_SMOKE] applying pipeline CR {} from {}",
        pipeline_name,
        cr_path.display()
    );
    run_kubectl(&["apply", "-f", cr_path.to_str().unwrap()])?;
    wait_for_pipeline_phase(&pipeline_name, "Running", Duration::from_secs(180)).await?;

    println!(
        "[KUBE_SMOKE] connecting to in-memory storage via {}",
        storage_addr_local
    );
    let storage_start = tokio::time::Instant::now();
    let storage_timeout = Duration::from_secs(60);
    let mut storage_client;
    loop {
        match InMemoryStorageClient::new(storage_addr_local.clone()).await {
            Ok(client) => {
                storage_client = client;
                println!("[KUBE_SMOKE] connected to storage");
                break;
            }
            Err(e) => {
                if storage_start.elapsed() > storage_timeout {
                    bail!("failed to connect to test storage: {}", e);
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }

    let records_start = tokio::time::Instant::now();
    let records_timeout = Duration::from_secs(120);
    let mut last_rows_log = tokio::time::Instant::now();
    let stored_messages = loop {
        let messages = storage_client.get_vector().await?;
        let total_rows: usize = messages.iter().map(|m| m.record_batch().num_rows()).sum();
        if total_rows >= EXPECTED_RECORDS {
            println!(
                "[KUBE_SMOKE] storage reached expected rows: {} in {} batches",
                total_rows,
                messages.len()
            );
            break messages;
        }
        if records_start.elapsed() > records_timeout {
            bail!(
                "timed out waiting for storage records: got {} rows in {} batches, expected {} rows",
                total_rows,
                messages.len(),
                EXPECTED_RECORDS
            );
        }
        if last_rows_log.elapsed() >= Duration::from_secs(5) {
            println!(
                "[KUBE_SMOKE] waiting for storage rows: got {} rows in {} batches (expected {})",
                total_rows,
                messages.len(),
                EXPECTED_RECORDS
            );
            last_rows_log = tokio::time::Instant::now();
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    };

    let total_rows: usize = stored_messages
        .iter()
        .map(|m| m.record_batch().num_rows())
        .sum();
    assert_eq!(
        total_rows, EXPECTED_RECORDS,
        "expected all records to be written to sink"
    );
    for message in stored_messages {
        let batch = message.record_batch();
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .context("expected Utf8 output column in sink batch")?;
        assert!(
            values.value(0).starts_with("key-"),
            "expected deterministic Key generator output"
        );
    }
    println!("[KUBE_SMOKE] validation passed");

    cleanup_pipeline(&pipeline_name);
    let _ = fs::remove_file(cr_path);
    let _ = port_forward.kill();
    println!("[KUBE_SMOKE] test finished");
    Ok(())
}
