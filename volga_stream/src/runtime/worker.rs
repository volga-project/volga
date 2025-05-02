use crate::runtime::{execution_graph::ExecutionGraph, task::{Task, StreamTask}, runtime_context::RuntimeContext};
use anyhow::Result;
use std::sync::Arc;
use tokio::runtime::{Builder, Runtime};
use tokio_rayon::rayon::{ThreadPool, ThreadPoolBuilder};
use std::collections::HashMap;
use crate::transport::{TransportBackend, InMemoryTransportBackend};

pub struct Worker {
    graph: ExecutionGraph,
    vertex_ids: Vec<String>,
    runtimes: HashMap<String, Runtime>,
    compute_pools: HashMap<String, Arc<ThreadPool>>,
    num_io_threads: usize,
    num_compute_threads: usize,
    transport_backend: InMemoryTransportBackend,
}

impl Worker {
    pub fn new(graph: ExecutionGraph, vertex_ids: Vec<String>, num_io_threads: usize, num_compute_threads: usize) -> Self {
        Self {
            graph,
            vertex_ids,
            runtimes: HashMap::new(),
            compute_pools: HashMap::new(),
            num_io_threads,
            num_compute_threads,
            transport_backend: InMemoryTransportBackend::new(),
        }
    }

    pub async fn start(&mut self) -> Result<()> {
        // Start the transport backend
        self.transport_backend.start().await?;

        for vertex_id in &self.vertex_ids {
            let vertex = self.graph.get_vertex(vertex_id).expect("Vertex should exist");
            
            // Create a new Tokio runtime for each vertex
            let runtime = Builder::new_multi_thread()
                .worker_threads(self.num_io_threads)
                .enable_all()
                .build()?;

            // Create a compute pool for each vertex
            let compute_pool = Arc::new(
                ThreadPoolBuilder::new()
                    .num_threads(self.num_compute_threads)
                    .build()?
            );

            // Store runtime and compute pool
            self.runtimes.insert(vertex_id.clone(), runtime);
            self.compute_pools.insert(vertex_id.clone(), compute_pool.clone());

            // Create runtime context for the vertex
            let runtime_context = RuntimeContext::new(
                0, // task_id
                0, // task_index
                0, // parallelism
                0, // operator_id
                vertex_id.clone(), // operator_name
                None, // job_config
                Some(compute_pool), // compute_pool
            );

            // Create and start the task
            let mut task = StreamTask::new(
                vertex_id.clone(),
                vertex.operator_config.clone(),
                runtime_context,
            ).await?;

            // Register the task's transport client with the backend
            self.transport_backend.register_client(vertex_id.clone(), task.transport_client()).await?;

            // Register input channels
            if let Some((input_edges, _)) = self.graph.get_edges_for_vertex(vertex_id) {
                for edge in input_edges {
                    let channel = edge.channel.clone();
                    self.transport_backend.register_channel(vertex_id.clone(), channel, true).await?;
                }
            }

            // Register output channels
            if let Some((_, output_edges)) = self.graph.get_edges_for_vertex(vertex_id) {
                for edge in output_edges {
                    let channel = edge.channel.clone();
                    self.transport_backend.register_channel(vertex_id.clone(), channel, false).await?;
                }
            }

            // Get the runtime for this vertex
            let runtime = self.runtimes.get(vertex_id).unwrap();
            
            // Spawn the task in its own runtime
            runtime.spawn(async move {
                if let Err(e) = task.open().await {
                    eprintln!("Error opening task: {}", e);
                    return;
                }
                if let Err(e) = task.run().await {
                    eprintln!("Error running task: {}", e);
                    return;
                }
                if let Err(e) = task.close().await {
                    eprintln!("Error closing task: {}", e);
                    return;
                }
            });
        }

        Ok(())
    }

    pub async fn close(&mut self) -> Result<()> {
        // Close the transport backend
        self.transport_backend.close().await?;

        for (_, runtime) in self.runtimes.drain() {
            runtime.shutdown_background();
        }
        Ok(())
    }
}
