use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};
use crate::runtime::worker::WorkerState;
use crate::runtime::stream_task::StreamTaskStatus;

// Include the generated protobuf client code
pub mod worker_service {
    tonic::include_proto!("worker_service");
}

use worker_service::{
    worker_service_client::WorkerServiceClient,
    GetWorkerStateRequest, StartWorkerRequest, RunWorkerTasksRequest, CloseWorkerTasksRequest, CloseWorkerRequest,
};

/// Client for communicating with a worker server
pub struct WorkerClient {
    client: WorkerServiceClient<tonic::transport::Channel>,
    worker_ip: String,
}

impl WorkerClient {
    pub async fn connect(worker_ip: String) -> anyhow::Result<Self> {
        const MAX_RETRIES: u32 = 5;
        const RETRY_DELAY_MS: u64 = 1000;
        
        let addr = format!("http://{}", worker_ip);
        let mut last_error = None;
        
        for attempt in 0..MAX_RETRIES {
            match WorkerServiceClient::connect(addr.clone()).await {
                Ok(client) => {
                    println!("[MASTER] Successfully connected to worker {} on attempt {}", worker_ip, attempt + 1);
                    return Ok(Self {
                        client,
                        worker_ip,
                    });
                }
                Err(e) => {
                    last_error = Some(e.to_string());
                    println!("[MASTER] Connection attempt {} to worker {} failed: {}", attempt + 1, worker_ip, e);
                    
                    // Don't sleep on the last attempt
                    if attempt < MAX_RETRIES - 1 {
                        sleep(Duration::from_millis(RETRY_DELAY_MS * (attempt + 1) as u64)).await;
                    }
                }
            }
        }
        
        Err(anyhow::anyhow!(
            "Failed to connect to worker {} after {} attempts. Last error: {:?}", 
            worker_ip, 
            MAX_RETRIES, 
            last_error
        ))
    }

    pub async fn start_worker(&mut self) -> anyhow::Result<bool> {
        let request = tonic::Request::new(StartWorkerRequest {});
        let response = self.client.start_worker(request).await?;
        
        let success = response.get_ref().success;
        if !success {
            let error_msg = &response.get_ref().error_message;
            println!("[MASTER] Failed to start worker {}: {}", self.worker_ip, error_msg);
        } else {
            println!("[MASTER] Successfully started worker {}", self.worker_ip);
        }
        
        Ok(success)
    }

    pub async fn run_worker_tasks(&mut self) -> anyhow::Result<bool> {
        let request = tonic::Request::new(RunWorkerTasksRequest {});
        let response = self.client.run_worker_tasks(request).await?;
        let success = response.get_ref().success;
        if !success {
            let error_msg = &response.get_ref().error_message;
            println!("[MASTER] Failed to run tasks on worker {}: {}", self.worker_ip, error_msg);
        } else {
            println!("[MASTER] Successfully ran tasks on worker {}", self.worker_ip);
        }
        Ok(success)
    }

    pub async fn close_worker_tasks(&mut self) -> anyhow::Result<bool> {
        let request = tonic::Request::new(CloseWorkerTasksRequest {});
        let response = self.client.close_worker_tasks(request).await?;
        let success = response.get_ref().success;
        if !success {
            let error_msg = &response.get_ref().error_message;
            println!("[MASTER] Failed to close tasks on worker {}: {}", self.worker_ip, error_msg);
        } else {
            println!("[MASTER] Successfully closed tasks on worker {}", self.worker_ip);
        }
        Ok(success)
    }

    pub async fn close_worker(&mut self) -> anyhow::Result<bool> {
        let request = tonic::Request::new(CloseWorkerRequest {});
        let response = self.client.close_worker(request).await?;
        
        let success = response.get_ref().success;
        if !success {
            let error_msg = &response.get_ref().error_message;
            println!("[MASTER] Failed to close worker {}: {}", self.worker_ip, error_msg);
        } else {
            println!("[MASTER] Successfully closed worker {}", self.worker_ip);
        }
        
        Ok(success)
    }

    pub async fn get_worker_state(&mut self) -> anyhow::Result<WorkerState> {
        let request = tonic::Request::new(GetWorkerStateRequest {});
        let response = self.client.get_worker_state(request).await?;
        
        let state_bytes = &response.get_ref().worker_state_bytes;
        if state_bytes.is_empty() {
            return Err(anyhow::anyhow!("Worker state is empty"));
        }
        
        let worker_state = WorkerState::from_bytes(state_bytes)?;
        Ok(worker_state)
    }
}

/// Master that orchestrates multiple worker servers
pub struct Master {
    worker_clients: Arc<Mutex<HashMap<String, WorkerClient>>>,
    worker_states: Arc<Mutex<HashMap<String, WorkerState>>>,
    state_polling_handle: Option<tokio::task::JoinHandle<()>>,
    running: Arc<std::sync::atomic::AtomicBool>,
}

impl Master {
    pub fn new() -> Self {
        Self {
            worker_clients: Arc::new(Mutex::new(HashMap::new())),
            worker_states: Arc::new(Mutex::new(HashMap::new())),
            state_polling_handle: None,
            running: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Connect to all workers
    pub async fn connect_to_workers(&mut self, worker_ips: Vec<String>) -> anyhow::Result<()> {
        println!("[MASTER] Connecting to {} workers", worker_ips.len());
        
        let mut clients_guard = self.worker_clients.lock().await;
        
        for worker_ip in worker_ips {
            match WorkerClient::connect(worker_ip.clone()).await {
                Ok(client) => {
                    clients_guard.insert(worker_ip.clone(), client);
                    println!("[MASTER] Connected to worker {}", worker_ip);
                }
                Err(e) => {
                    println!("[MASTER] Failed to connect to worker {}: {}", worker_ip, e);
                    return Err(e);
                }
            }
        }
        
        println!("[MASTER] Successfully created clients for all workers");
        Ok(())
    }

    /// Start all workers
    pub async fn start_all_workers(&mut self) -> anyhow::Result<()> {
        println!("[MASTER] Starting all workers");
        
        // Collect worker IPs first
        let worker_ips: Vec<String> = {
            let clients_guard = self.worker_clients.lock().await;
            clients_guard.keys().cloned().collect()
        };
        
        let mut start_futures = Vec::new();
        for worker_ip in worker_ips {
            let worker_clients = self.worker_clients.clone();
            let future = async move {
                let mut clients_guard = worker_clients.lock().await;
                if let Some(client) = clients_guard.get_mut(&worker_ip) {
                    let result = client.start_worker().await;
                    (worker_ip, result)
                } else {
                    (worker_ip, Err(anyhow::anyhow!("Worker client not found")))
                }
            };
            start_futures.push(future);
        }
        
        let results = futures::future::join_all(start_futures).await;
        
        for (worker_ip, result) in results {
            match result {
                Ok(success) => {
                    if !success {
                        return Err(anyhow::anyhow!("Failed to start worker {}", worker_ip));
                    }
                }
                Err(e) => {
                    return Err(anyhow::anyhow!("Error starting worker {}: {}", worker_ip, e));
                }
            }
        }
        
        println!("[MASTER] All workers started successfully");
        Ok(())
    }

    /// Start periodic state polling
    pub fn start_state_polling(&mut self) {
        println!("[MASTER] Starting state polling task");
        
        let worker_clients = self.worker_clients.clone();
        let worker_states = self.worker_states.clone();
        let running = self.running.clone();
        
        let handle = tokio::spawn(async move {
            while running.load(std::sync::atomic::Ordering::Relaxed) {
                let mut new_states = HashMap::new();
                
                let mut clients_guard = worker_clients.lock().await;
                for (worker_ip, client) in clients_guard.iter_mut() {
                    match client.get_worker_state().await {
                        Ok(state) => {
                            new_states.insert(worker_ip.clone(), state);
                        }
                        Err(e) => {
                            println!("[MASTER] Failed to get state from worker {}: {}", worker_ip, e);
                        }
                    }
                }
                drop(clients_guard);
                
                // Update shared state
                {
                    let mut states_guard = worker_states.lock().await;
                    *states_guard = new_states;
                }
                
                sleep(Duration::from_millis(100)).await;
            }
        });
        
        self.state_polling_handle = Some(handle);
    }

    /// Wait for all workers to have tasks in the specified status
    pub async fn wait_for_all_tasks_status(&self, target_status: StreamTaskStatus) -> anyhow::Result<()> {
        println!("[MASTER] Waiting for all workers to have tasks in status: {:?}", target_status);
        
        let start_time = std::time::Instant::now();
        let timeout_duration = Duration::from_secs(5);
        
        loop {
            // Check timeout
            if start_time.elapsed() > timeout_duration {
                return Err(anyhow::anyhow!("Timeout waiting for all tasks to be {:?} after {:?}", target_status, timeout_duration));
            }
            
            let all_ready = {
                let states_guard = self.worker_states.lock().await;
                if states_guard.is_empty() {
                    false
                } else {
                    states_guard.values().all(|state| state.all_tasks_have_status(target_status))
                }
            };
            
            if all_ready {
                println!("[MASTER] All workers have tasks in status: {:?}", target_status);
                break;
            }
            
            sleep(Duration::from_millis(100)).await;
        }
        
        Ok(())
    }

    /// Start all tasks on all workers
    pub async fn run_all_tasks(&mut self) -> anyhow::Result<()> {
        println!("[MASTER] Running all tasks on all workers");
        let worker_ips: Vec<String> = {
            let clients_guard = self.worker_clients.lock().await;
            clients_guard.keys().cloned().collect()
        };
        let mut run_futures = Vec::new();
        for worker_ip in worker_ips {
            let worker_clients = self.worker_clients.clone();
            let future = async move {
                let mut clients_guard = worker_clients.lock().await;
                if let Some(client) = clients_guard.get_mut(&worker_ip) {
                    let result = client.run_worker_tasks().await;
                    (worker_ip, result)
                } else {
                    (worker_ip, Err(anyhow::anyhow!("Worker client not found")))
                }
            };
            run_futures.push(future);
        }
        let results = futures::future::join_all(run_futures).await;
        for (worker_ip, result) in results {
            match result {
                Ok(success) => {
                    if !success {
                        return Err(anyhow::anyhow!("Failed to run tasks on worker {}", worker_ip));
                    }
                }
                Err(e) => {
                    return Err(anyhow::anyhow!("Error running tasks on worker {}: {}", worker_ip, e));
                }
            }
        }
        println!("[MASTER] All tasks started successfully");
        Ok(())
    }

    /// Close all tasks on all workers
    pub async fn close_all_tasks(&mut self) -> anyhow::Result<()> {
        println!("[MASTER] Closing all tasks on all workers");
        
        // Collect worker IPs first
        let worker_ips: Vec<String> = {
            let clients_guard = self.worker_clients.lock().await;
            clients_guard.keys().cloned().collect()
        };
        
        let mut close_futures = Vec::new();
        for worker_ip in worker_ips {
            let worker_clients = self.worker_clients.clone();
            let future = async move {
                let mut clients_guard = worker_clients.lock().await;
                if let Some(client) = clients_guard.get_mut(&worker_ip) {
                    let result = client.close_worker_tasks().await;
                    (worker_ip, result)
                } else {
                    (worker_ip, Err(anyhow::anyhow!("Worker client not found")))
                }
            };
            close_futures.push(future);
        }
        
        let results = futures::future::join_all(close_futures).await;
        
        for (worker_ip, result) in results {
            match result {
                Ok(success) => {
                    if !success {
                        return Err(anyhow::anyhow!("Failed to close tasks on worker {}", worker_ip));
                    }
                }
                Err(e) => {
                    return Err(anyhow::anyhow!("Error closing tasks on worker {}: {}", worker_ip, e));
                }
            }
        }
        
        println!("[MASTER] All tasks closed successfully");
        Ok(())
    }

    /// Close all workers
    pub async fn close_all_workers(&mut self) -> anyhow::Result<()> {
        println!("[MASTER] Closing all workers");
        
        // Collect worker IPs first
        let worker_ips: Vec<String> = {
            let clients_guard = self.worker_clients.lock().await;
            clients_guard.keys().cloned().collect()
        };
        
        let mut close_futures = Vec::new();
        for worker_ip in worker_ips {
            let worker_clients = self.worker_clients.clone();
            let future = async move {
                let mut clients_guard = worker_clients.lock().await;
                if let Some(client) = clients_guard.get_mut(&worker_ip) {
                    let result = client.close_worker().await;
                    (worker_ip, result)
                } else {
                    (worker_ip, Err(anyhow::anyhow!("Worker client not found")))
                }
            };
            close_futures.push(future);
        }
        
        let results = futures::future::join_all(close_futures).await;
        
        for (worker_ip, result) in results {
            match result {
                Ok(success) => {
                    if !success {
                        return Err(anyhow::anyhow!("Failed to close worker {}", worker_ip));
                    }
                }
                Err(e) => {
                    return Err(anyhow::anyhow!("Error closing worker {}: {}", worker_ip, e));
                }
            }
        }
        
        println!("[MASTER] All workers closed successfully");
        Ok(())
    }

    /// Execute the complete job lifecycle according to the new protocol
    pub async fn execute(&mut self, worker_ips: Vec<String>) -> anyhow::Result<()> {
        println!("[MASTER] Starting execution with {} workers", worker_ips.len());
        
        self.running.store(true, std::sync::atomic::Ordering::Relaxed);
        
        // 1. Master connects to all workers
        self.connect_to_workers(worker_ips).await?;
        
        // 2. Master starts all workers
        self.start_all_workers().await?;
        
        // 3. Start state polling to monitor worker states
        self.start_state_polling();
        
        // 4. Master waits for all tasks on all workers to be opened
        self.wait_for_all_tasks_status(StreamTaskStatus::Opened).await?;
        
        // 5. Master runs all tasks on all workers
        self.run_all_tasks().await?;
        
        // 6. Master waits for all tasks on all workers to be finished
        self.wait_for_all_tasks_status(StreamTaskStatus::Finished).await?;
        
        // 7. Master closes all tasks on all workers
        self.close_all_tasks().await?;
        
        // 8. Master waits for all tasks on all workers to be closed
        self.wait_for_all_tasks_status(StreamTaskStatus::Closed).await?;
        
        // 9. Master closes workers
        self.close_all_workers().await?;
        
        // Stop state polling
        self.running.store(false, std::sync::atomic::Ordering::Relaxed);
        if let Some(handle) = self.state_polling_handle.take() {
            let _ = handle.await;
        }
        
        println!("[MASTER] Execution completed successfully");
        Ok(())
    }

    /// Get current worker states
    pub async fn get_worker_states(&self) -> HashMap<String, WorkerState> {
        let states_guard = self.worker_states.lock().await;
        states_guard.clone()
    }
} 