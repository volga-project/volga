// use crate::runtime::{execution_graph::ExecutionGraph, task::StreamTask, runtime_context::RuntimeContext, partition::{create_partition, PartitionType}};
// use anyhow::Result;
// use std::{sync::Arc, collections::HashMap};
// use tokio::runtime::{Builder, Runtime};
// use tokio_rayon::rayon::{ThreadPool, ThreadPoolBuilder};
// use crate::transport::{TransportBackend, InMemoryTransportBackend};
// use tokio::sync::Mutex;

// pub struct Worker {
//     graph: ExecutionGraph,
//     vertex_ids: Vec<String>,
//     task_runtimes: HashMap<String, Runtime>,
//     compute_pools: HashMap<String, Arc<ThreadPool>>,
//     tasks: HashMap<String, Arc<Mutex<StreamTask>>>,
//     num_io_threads: usize,
//     num_compute_threads: usize,
//     transport_backend: Arc<Mutex<InMemoryTransportBackend>>,
//     transport_runtime: Runtime,
// }

// impl Worker {
//     pub fn new(
//         graph: ExecutionGraph, 
//         vertex_ids: Vec<String>, 
//         num_io_threads: usize, 
//         num_compute_threads: usize,
//     ) -> Self {
//         Self {
//             graph,
//             vertex_ids,
//             task_runtimes: HashMap::new(),
//             compute_pools: HashMap::new(),
//             tasks: HashMap::new(),
//             num_io_threads,
//             num_compute_threads,
//             transport_backend: Arc::new(Mutex::new(InMemoryTransportBackend::new())),
//             transport_runtime: Builder::new_multi_thread()
//                 .worker_threads(1)
//                 .enable_all()
//                 .thread_name("transport-runtime")
//                 .build()
//                 .unwrap(),
//         }
//     }

//     pub fn start(&mut self) -> Result<()> {
//         println!("Starting worker");
//         // Create runtimes, tasks and register input/output channels
//         for vertex_id in &self.vertex_ids {
//             let vertex = self.graph.get_vertex(vertex_id).expect("Vertex should exist");
            
//             // Create a new Tokio runtime for each vertex
//             let runtime = Builder::new_multi_thread()
//                 .worker_threads(self.num_io_threads)
//                 .enable_all()
//                 .thread_name(format!("tokio-runtime-vertex-{}", vertex_id))
//                 .build()?;

//             // Create a compute pool for each vertex
//             let compute_pool = Arc::new(
//                 ThreadPoolBuilder::new()
//                     .num_threads(self.num_compute_threads)
//                     .build()?
//             );

//             // Store runtime and compute pool
//             self.task_runtimes.insert(vertex_id.clone(), runtime);
//             self.compute_pools.insert(vertex_id.clone(), compute_pool.clone());

//             // Create runtime context for the vertex
//             let runtime_context = RuntimeContext::new(
//                 0, // task_id
//                 0, // task_index
//                 0, // parallelism
//                 0, // operator_id
//                 vertex_id.clone(), // operator_name
//                 None, // job_config
//                 Some(compute_pool), // compute_pool
//             );

            
//             // Create the task
//             let task = Arc::new(Mutex::new(StreamTask::new(
//                 vertex_id.clone(),
//                 vertex.operator_config.clone(),
//                 runtime_context,
//             )?));
//             self.tasks.insert(vertex_id.clone(), task.clone());
//             let task = task.clone();
            
//             let (input_edges, output_edges) = self.graph.get_edges_for_vertex(vertex_id).unwrap();
//             let transport_backend = self.transport_backend.clone();
//             self.transport_runtime.block_on(async move {
//                 let mut task = task.lock().await;
//                 let mut backend = transport_backend.lock().await;
//                 backend.register_client(vertex_id.clone(), task.transport_client()).await?;
//                 for edge in input_edges {
//                     backend.register_channel(
//                         vertex_id.clone(),
//                         edge.channel.clone(),
//                         true,
//                     ).await?;
//                 }
//                 for edge in output_edges.clone() {
//                     backend.register_channel(
//                         vertex_id.clone(),
//                         edge.channel.clone(),
//                         false,
//                     ).await?;
//                 }
//                 for edge in output_edges.clone() {
//                     task.create_or_update_collector(
//                         edge.channel.get_channel_id().clone(),
//                         create_partition(edge.partition_type.clone()),
//                         edge.job_edge_id.clone(),
//                     )?;
//                 }
//                 Ok::<(), anyhow::Error>(())
//             })?;
//         }
        
//         // Start the transport backend
//         let transport_backend = self.transport_backend.clone();
//         self.transport_runtime.block_on(async move {
//             let mut backend = transport_backend.lock().await;
//             backend.start().await
//         })?;

//         // Start the tasks
//         for vertex_id in &self.vertex_ids {
//             // Spawn the task on the vertex's runtime
//             let runtime = self.task_runtimes.get(vertex_id).unwrap();
//             let task = self.tasks.get(vertex_id).unwrap().clone();
//             runtime.spawn(async move {
//                 let mut task = task.lock().await;
//                 task.open().await?;
//                 task.run().await?;
//                 Ok::<(), anyhow::Error>(())
//             });
//         }

//         println!("Started worker");
//         Ok(())
//     }

//     pub fn close(&mut self) -> Result<()> {
//         println!("Closing worker");
//         // Close all tasks in their respective runtimes
//         for vertex_id in &self.vertex_ids {
//             if let Some(task) = self.tasks.get(vertex_id) {
//                 let task = task.clone();
//                 if let Some(runtime) = self.task_runtimes.get(vertex_id) {
//                     runtime.block_on(async move {
//                         let mut task = task.lock().await;
//                         task.close().await
//                     })?;
//                 }
//             }
//         }

//         // Close the transport backend in its runtime
//         let transport_backend = self.transport_backend.clone();
//         self.transport_runtime.block_on(async move {
//             let mut backend = transport_backend.lock().await;
//             backend.close().await
//         })?;

//         // Shutdown all runtimes
//         for vertex_id in &self.vertex_ids {
//             if let Some(runtime) = self.task_runtimes.remove(vertex_id) {
//                 runtime.shutdown_timeout(std::time::Duration::from_secs(1));
//             }
//         }

//         // TODO: Shutdown the transport runtime
//         // self.transport_runtime.shutdown_timeout(std::time::Duration::from_secs(1));

//         Ok(())
//     }
// }
