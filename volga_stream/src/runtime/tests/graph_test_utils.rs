use crate::runtime::execution_graph::{ExecutionEdge, ExecutionGraph, ExecutionVertex};
use crate::runtime::operators::operator::OperatorConfig;
use crate::runtime::partition::PartitionType;
use crate::transport::channel::Channel;
use crate::common::test_utils::gen_unique_grpc_port;
use std::collections::HashMap;

/// Configuration for generating test execution graphs
#[derive(Debug, Clone)]
pub struct TestGraphConfig {
    /// List of (vertex_name, operator_config) tuples defining the operator chain
    pub operators: Vec<(String, OperatorConfig)>,
    /// Parallelism level for each operator
    pub parallelism: usize,
    /// Whether to enable chaining
    pub chained: bool,
    /// Whether to simulate remote connections
    pub is_remote: bool,
    /// For remote case
    pub num_workers_per_operator: Option<usize>
}

/// Generates a test execution graph based on the provided configuration
pub fn create_test_execution_graph(config: TestGraphConfig) -> (ExecutionGraph, Option<HashMap<String, Vec<String>>>) {
    // Validate configuration
    validate_configs(&config.operators);

    let mut graph = ExecutionGraph::new();
    
    // Group operators based on chaining configuration
    let grouped_operators = if config.chained {
        group_operators_for_chaining(&config.operators)
    } else {
        // If no chaining, each operator becomes its own group
        config.operators.clone()
    };

    // Create vertices for each operator with parallelism
    for (op_name, op_config) in &grouped_operators {
        for i in 0..config.parallelism {
            let vertex_id = if config.parallelism == 1 {
                op_name.clone()
            } else {
                format!("{}_{}", op_name, i)
            };
            
            let vertex = ExecutionVertex::new(
                vertex_id,
                op_config.clone(),
                config.parallelism as i32,
                i as i32,
            );
            graph.add_vertex(vertex);
        }
    }

    let mut worker_to_port = HashMap::new();
    let mut worker_distribution = None;
    // Assign ports for remote channels if needed
    if config.is_remote {
        let num_workers_per_operator = config.num_workers_per_operator.unwrap();
        let parallelism_per_worker = config.parallelism / num_workers_per_operator;
        worker_distribution = Some(create_operator_based_worker_distribution(num_workers_per_operator, &grouped_operators, parallelism_per_worker));
        for worker_id in worker_distribution.clone().unwrap().keys() {
            worker_to_port.insert(worker_id.clone(), gen_unique_grpc_port() as i32);
        }
    }


    // Create edges between operators
    for i in 0..grouped_operators.len() - 1 {
        let (source_name, source_config) = &grouped_operators[i];
        let (target_name, target_config) = &grouped_operators[i + 1];
        
        // Determine partition type based on operator types
        let partition_type = determine_partition_type(source_config, target_config);
        
        // Create edges between all parallel instances
        for source_idx in 0..config.parallelism {
            let source_id = if config.parallelism == 1 {
                source_name.clone()
            } else {
                format!("{}_{}", source_name, source_idx)
            };
            
            for target_idx in 0..config.parallelism {
                let target_id = if config.parallelism == 1 {
                    target_name.clone()
                } else {
                    format!("{}_{}", target_name, target_idx)
                };
                let wd = worker_distribution.clone();
                let channel = create_channel(
                    &source_id,
                    &target_id,
                    config.is_remote,
                    &wd,
                    &worker_to_port,
                );
                
                let edge = ExecutionEdge::new(
                    source_id.clone(),
                    target_id.clone(),
                    target_name.clone(),
                    partition_type.clone(),
                    channel,
                );
                graph.add_edge(edge);
            }
        }
    }

    (graph, worker_distribution.clone())
}

/// Validates operator configurations
fn validate_configs(operators: &[(String, OperatorConfig)]) {
    for (i, (op_name, op_config)) in operators.iter().enumerate() {
        match op_config {
            OperatorConfig::ReduceConfig(_, _) => {
                // Check if there's a KeyBy operator right before this reduce
                if i == 0 {
                    panic!("Reduce operator '{}' requires a KeyBy operator before it", op_name);
                }
                
                let prev_config = &operators[i - 1].1;
                match prev_config {
                    OperatorConfig::KeyByConfig(_) => {
                        // This is valid - reduce has keyby right before it
                    }
                    _ => {
                        panic!("Reduce operator '{}' requires a KeyBy operator immediately before it", op_name);
                    }
                }
            }
            _ => {}
        }
    }
}

/// Groups operators into chains based on partition types
fn group_operators_for_chaining(operators: &[(String, OperatorConfig)]) -> Vec<(String, OperatorConfig)> {
    let mut grouped_operators = Vec::new();
    let mut current_chain = Vec::new();
    
    for (op_name, op_config) in operators {
        current_chain.push((op_name.clone(), op_config.clone()));
        
        // If this is the last operator or the next operator requires hash partitioning,
        // end the current chain
        if current_chain.len() > 1 {
            let last_op = &current_chain[current_chain.len() - 2];
            let current_op = &current_chain[current_chain.len() - 1];
            
            if determine_partition_type(&last_op.1, &current_op.1) == PartitionType::Hash {
                // Remove the last operator from current chain and start a new one
                let last_op = current_chain.pop().unwrap();
                grouped_operators.push(create_operator_from_chain(&current_chain));
                current_chain = vec![last_op];
            }
        }
    }
    
    // Add the last chain
    if !current_chain.is_empty() {
        grouped_operators.push(create_operator_from_chain(&current_chain));
    }
    
    grouped_operators
}

/// Creates an operator from a chain of operators
fn create_operator_from_chain(chain: &[(String, OperatorConfig)]) -> (String, OperatorConfig) {
    if chain.len() == 1 {
        // Single operator - no chaining needed
        chain[0].clone()
    } else {
        // Multiple operators - create chained config
        let chained_config = OperatorConfig::ChainedConfig(
            chain.iter().map(|(_, config)| config.clone()).collect()
        );
        
        // Create name with all operator names joined by ->
        let chain_name = chain.iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>()
            .join("->");
        
        (format!("chain_{}", chain_name), chained_config)
    }
}

/// Determines the appropriate partition type between two operators
fn determine_partition_type(source_config: &OperatorConfig, target_config: &OperatorConfig) -> PartitionType {
    match (source_config, target_config) {
        // Hash partitioning when source is KeyBy
        (OperatorConfig::KeyByConfig(_), _) => PartitionType::Hash,
        // Hash partitioning when source is ChainedConfig and the last operator in the chain is KeyBy
        (OperatorConfig::ChainedConfig(configs), _) => {
            let mut has_key_by = false;
            for config in configs {
                match config {
                    OperatorConfig::KeyByConfig(_) => {
                        has_key_by = true;
                        break;
                    }
                    _ => {}
                }
            }
            if has_key_by {
                PartitionType::Hash
            } else {
                PartitionType::RoundRobin
            }
        }
        // All other cases use round-robin
        _ => PartitionType::RoundRobin,
    }
}

/// Creates a channel based on configuration
fn create_channel(
    source_id: &str,
    target_id: &str,
    is_remote: bool,
    worker_distribution: &Option<HashMap<String, Vec<String>>>,
    worker_to_port: &HashMap<String, i32>,
) -> Channel {
    if !is_remote {
        return Channel::Local {
            channel_id: format!("{}_to_{}", source_id, target_id),
        };
    }
    
    // For remote channels, we need worker distribution
    if let Some(ref worker_dist) = worker_distribution {
        let source_worker_id = find_worker_for_vertex(source_id, worker_dist);
        let target_worker_id = find_worker_for_vertex(target_id, worker_dist);
        let target_port = worker_to_port.get(&target_worker_id).unwrap_or(&50051);
        
        Channel::Remote {
            channel_id: format!("{}_to_{}", source_id, target_id),
            source_node_ip: "127.0.0.1".to_string(),
            source_node_id: source_worker_id,
            target_node_ip: "127.0.0.1".to_string(),
            target_node_id: target_worker_id,
            target_port: *target_port,
        }
    } else {
        panic!("No worker distribution provided");
    }
}

/// Helper function to find which worker a vertex is assigned to
fn find_worker_for_vertex(vertex_id: &str, worker_distribution: &HashMap<String, Vec<String>>) -> String {
    for (worker_id, vertex_ids) in worker_distribution {
        if vertex_ids.contains(&vertex_id.to_string()) {
            return worker_id.clone();
        }
    }
    panic!("No worker found for vertex: {}", vertex_id);
}

/// Helper function to create a worker distribution where each worker handles one operator type
pub fn create_operator_based_worker_distribution(
    num_workers_per_operator: usize,
    operators: &[(String, OperatorConfig)],
    parallelism_per_worker: usize,
) -> HashMap<String, Vec<String>> {
    let mut distribution = HashMap::new();
    let mut worker_id = 0;
    
    for (op_name, _) in operators {
        for worker_idx in 0..num_workers_per_operator {
            let worker_id_str = format!("worker_{}", worker_id);
            
            // Assign vertices for this worker (only vertices of the specific operator type)
            let mut vertex_ids = Vec::new();
            for vertex_idx in 0..parallelism_per_worker {
                let global_vertex_idx = worker_idx * parallelism_per_worker + vertex_idx;
                let vertex_id = if parallelism_per_worker == 1 {
                    op_name.clone()
                } else {
                    format!("{}_{}", op_name, global_vertex_idx)
                };
                vertex_ids.push(vertex_id);
            }
            
            distribution.insert(worker_id_str, vertex_ids);
            worker_id += 1;
        }
    }
    
    distribution
} 