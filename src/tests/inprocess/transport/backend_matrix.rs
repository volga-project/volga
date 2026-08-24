use std::sync::Arc;
use std::time::Duration;

use crate::runtime::health::WorkerHealth;
use crate::test_utils::many_to_many_harness::{
    run_many_to_many_case, MeshChannelMode, MeshTestConfig,
};
use crate::transport::{TransportBackend, TransportBackendTrait};

fn grpc_backend() -> Box<dyn TransportBackendTrait> {
    Box::new(TransportBackend::new(Arc::new(WorkerHealth::new())))
}

fn happy_path(mode: MeshChannelMode) -> MeshTestConfig {
    let remote = !matches!(mode, MeshChannelMode::LocalOnly);
    MeshTestConfig {
        num_writer_nodes: if remote { 2 } else { 1 },
        num_reader_nodes: if remote { 2 } else { 1 },
        num_writers_per_node: if remote { 3 } else { 8 },
        num_readers_per_node: if remote { 3 } else { 8 },
        messages_per_writer: if remote { 5 } else { 8 },
        mode,
        startup_wait: Duration::from_millis(if remote { 1000 } else { 50 }),
        processing_wait: Duration::from_millis(if remote { 2000 } else { 50 }),
        read_timeout: Duration::from_secs(if remote { 10 } else { 5 }),
        queue_size_records: None,
        paused_reader_indices: Vec::new(),
    }
}

fn slow_consumer(mode: MeshChannelMode) -> MeshTestConfig {
    let remote = !matches!(mode, MeshChannelMode::LocalOnly);
    MeshTestConfig {
        num_writer_nodes: 1,
        num_reader_nodes: 1,
        num_writers_per_node: 2,
        num_readers_per_node: 2,
        messages_per_writer: 8,
        mode,
        startup_wait: Duration::from_millis(if remote { 1000 } else { 50 }),
        processing_wait: Duration::from_millis(if remote { 2000 } else { 50 }),
        read_timeout: Duration::from_secs(if remote { 10 } else { 5 }),
        queue_size_records: Some(2),
        paused_reader_indices: vec![0],
    }
}

#[tokio::test]
async fn test_many_to_many_local_only_harness() {
    run_many_to_many_case(happy_path(MeshChannelMode::LocalOnly), grpc_backend).await;
}

#[tokio::test]
async fn test_many_to_many_remote_only_harness() {
    run_many_to_many_case(happy_path(MeshChannelMode::RemoteOnly), grpc_backend).await;
}

#[tokio::test]
async fn test_many_to_many_mixed_local_remote_harness() {
    run_many_to_many_case(happy_path(MeshChannelMode::MixedLocalRemote), grpc_backend).await;
}

#[tokio::test]
async fn test_many_to_many_local_slow_consumer() {
    run_many_to_many_case(slow_consumer(MeshChannelMode::LocalOnly), grpc_backend).await;
}

/// Shared gRPC ingress demux: one full dest pins the hop. Un-ignore in #229 (per-edge TCP).
#[tokio::test]
#[ignore = "shared gRPC HOL; un-ignore in #229"]
async fn test_many_to_many_remote_slow_consumer() {
    run_many_to_many_case(slow_consumer(MeshChannelMode::RemoteOnly), grpc_backend).await;
}
