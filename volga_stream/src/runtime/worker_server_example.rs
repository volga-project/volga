use crate::runtime::worker_server::WorkerServer;
use crate::runtime::execution_graph::ExecutionGraph;

/// Example of how to use the WorkerServer
pub async fn run_worker_server_example() -> Result<(), Box<dyn std::error::Error>> {
    // Create a new worker server
    let mut server = WorkerServer::new();
    
    // Example execution graph and vertex IDs (you would create these based on your application)
    let graph = ExecutionGraph::new(); // You would populate this with your actual graph
    let vertex_ids = vec!["vertex1".to_string(), "vertex2".to_string()];
    let num_io_threads = 4;
    
    // Initialize the worker with the execution graph
    server.initialize_worker(graph, vertex_ids, num_io_threads).await;
    
    // Start the gRPC server
    server.start("127.0.0.1:50051").await?;
    
    // Keep the server running
    tokio::signal::ctrl_c().await?;
    
    // Stop the server gracefully
    server.stop().await;
    
    Ok(())
}

/// Example of how to create a worker server with an existing worker
pub async fn run_worker_server_with_existing_worker() -> Result<(), Box<dyn std::error::Error>> {
    // Create your worker instance
    let graph = ExecutionGraph::new(); // You would populate this with your actual graph
    let vertex_ids = vec!["vertex1".to_string(), "vertex2".to_string()];
    let num_io_threads = 4;
    let worker = crate::runtime::worker::Worker::new(graph, vertex_ids, num_io_threads);
    
    // Create worker server with the existing worker
    let mut server = WorkerServer::with_worker(worker);
    
    // Start the gRPC server
    server.start("127.0.0.1:50051").await?;
    
    // Keep the server running
    tokio::signal::ctrl_c().await?;
    
    // Stop the server gracefully
    server.stop().await;
    
    Ok(())
} 