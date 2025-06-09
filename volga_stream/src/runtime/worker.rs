use crate::{runtime::{
    execution_graph::ExecutionGraph, runtime_context::RuntimeContext, stream_task::{StreamTask, StreamTaskStatus}, stream_task_actor::{StreamTaskActor, StreamTaskMessage}
}, transport::{InMemoryTransportBackend, TransportBackend}};
use crate::transport::transport_backend_actor::{TransportBackendActor, TransportBackendActorMessage};
use std::collections::HashMap;
use kameo::{spawn, prelude::ActorRef};
use tokio::runtime::{Builder, Runtime};
use tokio::time::{sleep, Duration};
use futures::future::join_all;

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

    pub fn start(&mut self) {
        println!("Starting worker");

        let backend_runtime = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .thread_name("transport-backend-runtime")
            .build().unwrap();

        let mut backend = InMemoryTransportBackend::new();
        let mut transport_client_configs = backend.init_channels(&self.graph, self.vertex_ids.clone());

        let backend_actor = backend_runtime.block_on(async {
            spawn(TransportBackendActor::new(backend))
        });

        self.backend_runtime = Some(backend_runtime);
        self.backend_actor = Some(backend_actor.clone());

        for vertex_id in &self.vertex_ids {
            let vertex = self.graph.get_vertex(vertex_id).expect("Vertex should exist");
            
            // Create runtime for this task
            let runtime = Builder::new_multi_thread()
                .worker_threads(self.num_io_threads)
                .enable_all()
                .thread_name(format!("task-runtime-{}", vertex_id))
                .build().unwrap();

            // Create runtime context for the vertex
            let runtime_context = RuntimeContext::new(
                vertex_id.clone(),
                vertex.task_index,
                vertex.parallelism,
                None,
            );

            // Create the task and its actor in the task's runtime
            let task = StreamTask::new(
                vertex_id.clone(),
                vertex.operator_config.clone(),
                transport_client_configs.remove(&vertex_id.clone()).unwrap(),
                runtime_context,
                self.graph.clone(),
            );
            let task_actor = StreamTaskActor::new(task);
            let task_ref = runtime.block_on(async {
                spawn(task_actor)
            });
            self.task_actors.insert(vertex_id.clone(), task_ref.clone());
            self.task_runtimes.insert(vertex_id.clone(), runtime);
        }

        // Start the backend
        self.backend_runtime.as_ref().unwrap().block_on(async {
            self.backend_actor.as_ref().unwrap().ask(TransportBackendActorMessage::Start).await
        }).unwrap();

        // Start all tasks
        let mut start_futures = Vec::new();
        for (vertex_id, runtime) in &self.task_runtimes {
            let vertex_id = vertex_id.clone();
            let task_ref = self.task_actors.get(&vertex_id).unwrap().clone();

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
        }).unwrap();

        println!("Started worker");
    }

    pub fn close(&mut self) {
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
        }).unwrap();

        // Close the backend
        if let Some(backend_actor) = &self.backend_actor {
            self.backend_runtime.as_ref().unwrap().block_on(async {
                backend_actor.ask(TransportBackendActorMessage::Close).await
            }).unwrap();
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
    }

    async fn wait_for_completion(&self) {
        let mut all_closed = false;
        while !all_closed {
            // Create futures for all task state checks
            let state_futures: Vec<_> = self.task_actors.iter()
                .map(|(vertex_id, task_ref)| {
                    let task_ref = task_ref.clone();
                    let vertex_id = vertex_id.clone();
                    async move {
                        let state = task_ref.ask(StreamTaskMessage::GetState).await?;
                        Ok::<_, anyhow::Error>((vertex_id, state))
                    }
                })
                .collect();

            // Wait for all state checks to complete in parallel
            let states = join_all(state_futures).await;
            
            // Check if any task is not closed
            all_closed = true;
            for result in states {
                match result {
                    Ok((vertex_id, state)) => {
                        println!("Task {} is in state {:?}", vertex_id, state.status);
                        if state.status != StreamTaskStatus::Closed {
                            all_closed = false;
                        }
                    }
                    Err(e) => {
                        println!("Error getting task state: {}", e);
                        all_closed = false;
                    }
                }
            }

            if !all_closed {
                sleep(Duration::from_millis(1000)).await;
            }
        }
        println!("All tasks have completed");
    }

    pub fn execute(&mut self) {
        println!("Starting worker execution");
        self.start();
        let runtime = Runtime::new().unwrap();
        runtime.block_on(async {
            self.wait_for_completion().await;
            println!("Worker execution completed");
        });
    }
}
