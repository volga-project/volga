use std::time::Duration;
use std::sync::Arc;

use crate::runtime::health::WorkerHealth;
use crate::transport::{TransportBackend, TransportBackendTrait};

use super::many_to_many_harness::{run_many_to_many_case, MeshChannelMode, MeshTestConfig};

#[tokio::test]
async fn test_many_to_many_local_only_harness() {
    let config = MeshTestConfig {
        num_writer_nodes: 1,
        num_reader_nodes: 1,
        num_writers_per_node: 8,
        num_readers_per_node: 8,
        messages_per_writer: 8,
        mode: MeshChannelMode::LocalOnly,
        startup_wait: Duration::from_millis(50),
        processing_wait: Duration::from_millis(50),
        read_timeout: Duration::from_secs(5),
    };

    run_many_to_many_case(config, || -> Box<dyn TransportBackendTrait> {
        Box::new(TransportBackend::new(Arc::new(WorkerHealth::new())))
    })
    .await;
}

#[tokio::test]
async fn test_many_to_many_remote_only_harness() {
    let config = MeshTestConfig {
        num_writer_nodes: 2,
        num_reader_nodes: 2,
        num_writers_per_node: 3,
        num_readers_per_node: 3,
        messages_per_writer: 5,
        mode: MeshChannelMode::RemoteOnly,
        startup_wait: Duration::from_millis(1000),
        processing_wait: Duration::from_millis(2000),
        read_timeout: Duration::from_secs(10),
    };

    run_many_to_many_case(config, || -> Box<dyn TransportBackendTrait> {
        Box::new(TransportBackend::new(Arc::new(WorkerHealth::new())))
    })
    .await;
}

#[tokio::test]
async fn test_many_to_many_mixed_local_remote_harness() {
    let config = MeshTestConfig {
        num_writer_nodes: 2,
        num_reader_nodes: 2,
        num_writers_per_node: 3,
        num_readers_per_node: 3,
        messages_per_writer: 5,
        mode: MeshChannelMode::MixedLocalRemote,
        startup_wait: Duration::from_millis(1000),
        processing_wait: Duration::from_millis(2000),
        read_timeout: Duration::from_secs(10),
    };

    run_many_to_many_case(config, || -> Box<dyn TransportBackendTrait> {
        Box::new(TransportBackend::new(Arc::new(WorkerHealth::new())))
    })
    .await;
}

// TODO test ordered delivery
