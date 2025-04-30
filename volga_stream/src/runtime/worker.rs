use crate::runtime::{execution_graph::{ExecutionGraph, ExecutionVertex}, task::{Task, StreamTask}, runtime_context::RuntimeContext};
use anyhow::Result;
use std::sync::Arc;
use tokio::runtime::{Builder, Runtime};
use tokio_rayon::rayon::{ThreadPool, ThreadPoolBuilder};
use std::collections::HashMap;
use crate::runtime::operator::Operator;

pub struct Worker {
    graph: ExecutionGraph,
    vertices: Vec<ExecutionVertex>,
    runtimes: HashMap<String, Runtime>,
    compute_pools: HashMap<String, Arc<ThreadPool>>,
    num_threads: usize,
}

impl Worker {
    pub fn new(graph: ExecutionGraph, vertices: Vec<ExecutionVertex>, num_threads: usize) -> Self {
        Self {
            graph,
            vertices,
            runtimes: HashMap::new(),
            compute_pools: HashMap::new(),
            num_threads,
        }
    }

    pub async fn start(&mut self) -> Result<()> {
        for vertex in &mut self.vertices {
            // Create a new Tokio runtime for each vertex
            let runtime = Builder::new_multi_thread()
                .worker_threads(self.num_threads)
                .enable_all()
                .build()?;

            // Create a compute pool for each vertex
            let compute_pool = Arc::new(
                ThreadPoolBuilder::new()
                    .num_threads(self.num_threads)
                    .build()?
            );

            // Store runtime and compute pool
            self.runtimes.insert(vertex.vertex_id.clone(), runtime);
            self.compute_pools.insert(vertex.vertex_id.clone(), compute_pool.clone());

            // Create runtime context for the vertex
            let runtime_context = RuntimeContext::new(
                0, // task_id
                0, // task_index
                self.num_threads as i32, // parallelism
                0, // operator_id
                vertex.vertex_id.clone(), // operator_name
                None, // job_config
                Some(compute_pool), // compute_pool
            );

            // Create and start the task
            if let Some(operator) = vertex.take_operator() {
                let mut task = StreamTask::new(
                    vertex.vertex_id.clone(),
                    operator,
                    Default::default(),
                    runtime_context,
                    None,
                )?;

                // Get the runtime for this vertex
                let runtime = self.runtimes.get(&vertex.vertex_id).unwrap();
                
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
        }

        Ok(())
    }

    pub fn get_compute_pool(&self, vertex_id: &str) -> Option<&Arc<ThreadPool>> {
        self.compute_pools.get(vertex_id)
    }

    pub async fn shutdown(&mut self) -> Result<()> {
        for (_, runtime) in self.runtimes.drain() {
            runtime.shutdown_background();
        }
        Ok(())
    }
}
