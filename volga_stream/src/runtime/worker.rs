use crate::runtime::{
    execution_graph::ExecutionGraph,
    stream_task::StreamTask,
    runtime_context::RuntimeContext,
    stream_task_actor::StreamTaskActor,
};
use crate::transport::{
    transport_backend_actor::{TransportBackendActor, TransportBackendActorMessage},
    transport_client_actor::TransportClientActorType,
};
use anyhow::Result;
use std::collections::HashMap;
use kameo::{spawn, prelude::ActorRef};
use tokio::runtime::{Builder, Runtime};

pub struct Worker {
    graph: ExecutionGraph,
    vertex_ids: Vec<String>,
    task_actors: HashMap<String, ActorRef<StreamTaskActor>>,
    backend_actor: Option<ActorRef<TransportBackendActor>>,
    task_runtimes: HashMap<String, Runtime>,
    backend_runtime: Option<Runtime>,
    num_io_threads: usize,
}

impl Worker {
    pub fn new(
        graph: ExecutionGraph, 
        vertex_ids: Vec<String>,
        num_io_threads: usize,
    ) -> Self {
        Self {
            graph,
            vertex_ids,
            task_actors: HashMap::new(),
            backend_actor: None,
            task_runtimes: HashMap::new(),
            backend_runtime: None,
            num_io_threads,
        }
    }

    pub fn start(&mut self) -> Result<()> {
        println!("Starting worker");

        // Create runtime for transport backend
        let backend_runtime = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .thread_name("transport-backend-runtime")
            .build()?;

        // Create backend actor in its runtime
        let backend_actor = backend_runtime.block_on(async {
            spawn(TransportBackendActor::new())
        });

        self.backend_runtime = Some(backend_runtime);
        self.backend_actor = Some(backend_actor.clone());

        // Create and register task actors
        for vertex_id in &self.vertex_ids {
            let vertex = self.graph.get_vertex(vertex_id).expect("Vertex should exist");
            
            // Create runtime for this task
            let runtime = Builder::new_multi_thread()
                .worker_threads(self.num_io_threads)
                .enable_all()
                .thread_name(format!("task-runtime-{}", vertex_id))
                .build()?;

            // Create runtime context for the vertex
            let runtime_context = RuntimeContext::new(
                vertex_id.clone(), // vertex_id
                vertex.task_index, // task_index
                vertex.parallelism, // parallelism
                None, // job_config
            );

            // Create the task and its actor in the task's runtime
            let task = StreamTask::new(
                vertex_id.clone(),
                vertex.operator_config.clone(),
                runtime_context,
            )?;
            let task_actor = StreamTaskActor::new(task);
            let task_ref = runtime.block_on(async {
                spawn(task_actor)
            });
            self.task_actors.insert(vertex_id.clone(), task_ref.clone());

            // Get edges for this vertex
            let (input_edges, output_edges) = self.graph.get_edges_for_vertex(vertex_id).unwrap();

            // Register everything in a single block_on call
            runtime.block_on(async {
                // Register task actor with backend
                self.backend_actor.as_ref().unwrap().ask(TransportBackendActorMessage::RegisterActor {
                    vertex_id: vertex_id.clone(),
                    actor: TransportClientActorType::StreamTask(task_ref.clone()),
                }).await?;

                // Register input channels
                for edge in input_edges {
                    self.backend_actor.as_ref().unwrap().ask(TransportBackendActorMessage::RegisterChannel {
                        vertex_id: vertex_id.clone(),
                        channel: edge.channel.clone(),
                        is_input: true,
                    }).await?;
                }

                // Register output channels
                for edge in output_edges {
                    self.backend_actor.as_ref().unwrap().ask(TransportBackendActorMessage::RegisterChannel {
                        vertex_id: vertex_id.clone(),
                        channel: edge.channel.clone(),
                        is_input: false,
                    }).await?;

                    // Create collector for output channel
                    task_ref.ask(crate::runtime::stream_task_actor::StreamTaskMessage::CreateCollector {
                        channel_id: edge.channel.get_channel_id().clone(),
                        partition_type: edge.partition_type.clone(),
                        target_operator_id: edge.job_edge_id.clone(),
                    }).await?;
                }

                Ok::<(), anyhow::Error>(())
            })?;

            // Store the runtime after all async operations are done
            self.task_runtimes.insert(vertex_id.clone(), runtime);
        }

        // Start the backend
        self.backend_runtime.as_ref().unwrap().block_on(async {
            self.backend_actor.as_ref().unwrap().ask(TransportBackendActorMessage::Start).await
        })?;

        // Start all tasks
        let mut start_futures = Vec::new();
        for (vertex_id, runtime) in &self.task_runtimes {
            let vertex_id = vertex_id.clone();
            let task_ref = self.task_actors.get(&vertex_id).unwrap().clone();

            // Run task asynchronously without blocking
            let task_ref = task_ref.clone();
            let fut = runtime.spawn(async move {
                if let Err(e) = task_ref.ask(crate::runtime::stream_task_actor::StreamTaskMessage::Run).await {
                    eprintln!("Error running task {}: {}", vertex_id, e);
                }
            });
            start_futures.push(fut);
        }
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            for f in start_futures {
                let _ = f.await?;
            }
            Ok::<(), anyhow::Error>(())
        })?;

        println!("Started worker");
        Ok(())
    }

    pub fn close(&mut self) -> Result<()> {
        println!("Closing worker");

        // Close all tasks in parallel
        let mut close_futures = Vec::new();
        for (vertex_id, runtime) in &self.task_runtimes {
            let task_ref = self.task_actors.get(&vertex_id.clone()).unwrap().clone();
            let fut = runtime.spawn(async move {
                task_ref.ask(crate::runtime::stream_task_actor::StreamTaskMessage::Close).await?;
                Ok::<(), anyhow::Error>(())
            });
            close_futures.push(fut);
        }

        tokio::runtime::Runtime::new().unwrap().block_on(async {
            for f in close_futures {
                let _ = f.await?;
            }
            Ok::<(), anyhow::Error>(())
        })?;

        // Close the backend
        if let Some(backend_actor) = &self.backend_actor {
            self.backend_runtime.as_ref().unwrap().block_on(async {
                backend_actor.ask(TransportBackendActorMessage::Close).await
            })?;
        }

        // Shutdown all runtimes
        for (vertex_id, runtime) in self.task_runtimes.drain() {
            println!("Shutting down runtime for task {}", vertex_id);
            runtime.shutdown_timeout(std::time::Duration::from_secs(1));
        }

        // Shutdown backend runtime
        if let Some(runtime) = self.backend_runtime.take() {
            println!("Shutting down transport backend runtime");
            runtime.shutdown_timeout(std::time::Duration::from_secs(1));
        }

        println!("Closed worker");
        Ok(())
    }
}
