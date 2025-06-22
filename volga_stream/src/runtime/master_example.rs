use crate::runtime::master::Master;

/// Example of how to use the Master to orchestrate multiple worker servers
pub async fn run_master_example() -> Result<(), Box<dyn std::error::Error>> {
    // Create a new master
    let mut master = Master::new();
    
    // Define worker IPs (these would be the actual IPs of your worker servers)
    let worker_ips = vec![
        "127.0.0.1:50051".to_string(),
        "127.0.0.1:50052".to_string(),
        "127.0.0.1:50053".to_string(),
    ];
    
    println!("[MASTER_EXAMPLE] Starting master with {} workers", worker_ips.len());
    
    // Execute the complete lifecycle
    master.execute(worker_ips).await?;
    
    println!("[MASTER_EXAMPLE] Master execution completed");
    Ok(())
}

/// Example of how to use the Master step by step
pub async fn run_master_step_by_step() -> Result<(), Box<dyn std::error::Error>> {
    // Create a new master
    let mut master = Master::new();
    
    // Define worker IPs
    let worker_ips = vec![
        "127.0.0.1:50051".to_string(),
        "127.0.0.1:50052".to_string(),
    ];
    
    println!("[MASTER_EXAMPLE] Starting master step by step with {} workers", worker_ips.len());
    
    // Step 1: Create clients for all workers
    master.connect_to_workers(worker_ips).await?;
    
    // Step 2: Start all workers
    master.start_all_workers().await?;
    
    // Step 3: Start state polling
    master.start_state_polling();
    
    // Step 4: Wait for all tasks to be opened
    master.wait_for_all_tasks_status(crate::runtime::stream_task::StreamTaskStatus::Opened).await?;
    
    // Step 5: Start all tasks
    master.run_all_tasks().await?;
    
    // Step 6: Wait for all tasks to be closing
    master.wait_for_all_tasks_status(crate::runtime::stream_task::StreamTaskStatus::Finished).await?;
    
    // Step 7: Close all tasks
    master.close_all_tasks().await?;
    
    // Step 8: Wait for all tasks to be closed
    master.wait_for_all_tasks_status(crate::runtime::stream_task::StreamTaskStatus::Closed).await?;
    
    // Step 9: Close all workers
    master.close_all_workers().await?;
    
    println!("[MASTER_EXAMPLE] Step-by-step execution completed");
    Ok(())
}

/// Example of how to monitor worker states
pub async fn run_master_with_monitoring() -> Result<(), Box<dyn std::error::Error>> {
    // Create a new master
    let mut master = Master::new();
    
    // Define worker IPs
    let worker_ips = vec![
        "127.0.0.1:50051".to_string(),
        "127.0.0.1:50052".to_string(),
    ];
    
    println!("[MASTER_EXAMPLE] Starting master with monitoring");
    
    // Create clients and start workers
    master.connect_to_workers(worker_ips).await?;
    master.start_all_workers().await?;
    master.start_state_polling();
    
    // Monitor states for a while
    for i in 0..10 {
        let states = master.get_worker_states().await;
        println!("[MASTER_EXAMPLE] Iteration {} - Worker states:", i);
        for (worker_ip, state) in states {
            println!("  Worker {}: {} tasks, statuses: {:?}", 
                    worker_ip, 
                    state.task_statuses.len(),
                    state.task_statuses.values().collect::<Vec<_>>());
        }
        
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    }
    
    println!("[MASTER_EXAMPLE] Monitoring completed");
    Ok(())
} 