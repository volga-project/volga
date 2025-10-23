use std::sync::Arc;

use crate::{api::logical_graph::determine_partition_type, common::Message, runtime::{operators::operator::{create_operator, MessageStream, Operator, OperatorBase, OperatorConfig, OperatorPollResult, OperatorTrait, OperatorType}, partition::PartitionType, runtime_context::RuntimeContext}, storage::storage::Storage};
use anyhow::Result;
use async_trait::async_trait;
use futures::future::try_join_all;
use tokio::sync::mpsc::{self, Sender};
use tokio_stream::wrappers::ReceiverStream;

#[derive(Debug)]
pub struct ChainedOperator {
    base: OperatorBase,
    chain_senders: Vec<Option<Sender<Message>>>,
    operators: Vec<Operator>,
}

impl ChainedOperator {
    pub fn new(config: OperatorConfig, storage: Arc<Storage>) -> Self {
        let configs = match config.clone() {
            OperatorConfig::ChainedConfig(configs) => configs,
            _ => panic!("Expected ChainedConfig, got {:?}", config),
        };

        if configs.is_empty() {
            panic!("ChainedOperator cannot be empty");
        }

        let mut operators = Vec::new();
        let mut chain_senders = Vec::new();
        for i in 0..configs.len() {
            let mut operator = create_operator(configs[i].clone(), storage.clone());
            if i > 0 {
                let (tx, rx) = mpsc::channel(10);
                chain_senders.push(Some(tx));
                operator.set_input(Some(Box::pin(ReceiverStream::new(rx))));
                operators.push(operator);
            } else {
                chain_senders.push(None);
                operators.push(operator);
            }
        }
        
        Self { 
            base: OperatorBase::new(config, storage.clone()), 
            chain_senders,
            operators,
        }
    }
}

#[async_trait]
impl OperatorTrait for ChainedOperator {

    async fn open(&mut self, context: &RuntimeContext) -> Result<()> {
        // TODO open base?

        let open_futures: Vec<_> = self.operators
            .iter_mut()
            .map(|operator| operator.open(context))
            .collect();
        
        try_join_all(open_futures).await?;
        Ok(())
    }

    fn operator_type(&self) -> OperatorType {
        self.base.operator_type()
    }

    async fn close(&mut self) -> Result<()> {

        // TODO close base?
        let close_futures: Vec<_> = self.operators
            .iter_mut()
            .map(|operator| operator.close())
            .collect();
        
        try_join_all(close_futures).await?;
        Ok(())
    }

    fn set_input(&mut self, input: Option<MessageStream>) {
        if !self.operators.is_empty() {
            let first_op = &mut self.operators[0];
            first_op.set_input(input);
        }
    }

    // TODO implement
    async fn poll_next(&mut self) -> OperatorPollResult {
        panic!("Chaining not implemented")
    }
}

/// Groups operators into chains based on partition types
pub fn group_operators_for_chaining(operators: &[OperatorConfig]) -> Vec<OperatorConfig> {
    let mut grouped_operators = Vec::new();
    let mut current_chain = Vec::new();
    
    for op_config in operators {
        current_chain.push(op_config.clone());
        
        // If this is the last operator or the next operator requires hash partitioning,
        // end the current chain
        if current_chain.len() > 1 {
            let last_op = &current_chain[current_chain.len() - 2];
            let current_op = &current_chain[current_chain.len() - 1];
            
            if determine_partition_type(&last_op, &current_op) == PartitionType::Hash {
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
pub fn create_operator_from_chain(chain: &[OperatorConfig]) -> OperatorConfig {
    if chain.len() == 1 {
        // Single operator - no chaining needed
        chain[0].clone()
    } else {
        // Multiple operators - create chained config
        let chained_config = OperatorConfig::ChainedConfig(
            chain.iter().map(|config| config.clone()).collect()
        );
        
        chained_config
    }
}