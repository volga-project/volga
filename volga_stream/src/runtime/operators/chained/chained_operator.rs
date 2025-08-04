use crate::{api::logical_graph::determine_partition_type, common::Message, runtime::{functions::map::MapFunction, operators::operator::{create_operator_from_config, Operator, OperatorBase, OperatorConfig, OperatorTrait, OperatorType}, partition::PartitionType, runtime_context::RuntimeContext}};
use anyhow::Result;
use async_trait::async_trait;
use tokio_rayon::AsyncThreadPool;
use futures::future::try_join_all;

#[derive(Debug)]
pub struct ChainedOperator {
    base: OperatorBase,
    operators: Vec<Operator>,
}

impl ChainedOperator {
    pub fn new(config: OperatorConfig) -> Self {
        let configs = match config.clone() {
            OperatorConfig::ChainedConfig(configs) => configs,
            _ => panic!("Expected ChainedConfig, got {:?}", config),
        };
        Self { 
            base: OperatorBase::new(config),
            operators: configs.iter().map(|config| create_operator_from_config(config.clone())).collect(),
        }
    }

    async fn process_message_with_operator_chain(message: Message, operators: &mut [Operator]) -> Option<Vec<Message>> {
        let mut cur_messages = vec![message];
        for operator in operators.iter_mut() {
            let mut res = vec![];
            for message in cur_messages {
                let operator_messages = operator.process_message(message).await;
                if operator_messages.is_none() {
                    return None
                }
                res.extend(operator_messages.unwrap());
            }
            cur_messages = res;
        }
        Some(cur_messages)
    }
}

#[async_trait]
impl OperatorTrait for ChainedOperator {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()> {
        let open_futures: Vec<_> = self.operators
            .iter_mut()
            .map(|operator| operator.open(context))
            .collect();
        
        try_join_all(open_futures).await?;
        Ok(())
    }

    // when we have no source
    async fn process_message(&mut self, message: Message) -> Option<Vec<Message>> {
        match message {
            Message::Watermark(watermark) => {
                Some(vec![Message::Watermark(watermark)])
            }
            _ => {
                Self::process_message_with_operator_chain(message, &mut self.operators).await
            }
        }
    }

    // when chained operator starts with source
    async fn fetch(&mut self) -> Option<Vec<Message>> {
        let first_op = self.operators.get_mut(0).expect("chained operator can not be empty");
        if first_op.operator_type() != OperatorType::Source {
            panic!("Can not fetch chained operator without source operator being first");
        }

        let messages = first_op.fetch().await;
        if messages.is_none() {
            return None;
        }
        let mut res = vec![];

        for message in messages.unwrap() {
            // skip processing watermark
            match message {
                Message::Watermark(watermark) => {
                    res.push(Message::Watermark(watermark));
                }
                _ => {
                    // process by all operators except the first one
                    let processed = Self::process_message_with_operator_chain(message, &mut self.operators[1..]).await;
                    if processed.is_some() {
                        res.extend(processed.unwrap());
                    }
                }
            }

        }
        if res.len() != 0 {
            Some(res)
        } else {
            None
        }
    }

    fn operator_type(&self) -> OperatorType {
        self.base.operator_type()
    }

    async fn close(&mut self) -> Result<()> {
        let close_futures: Vec<_> = self.operators
            .iter_mut()
            .map(|operator| operator.close())
            .collect();
        
        try_join_all(close_futures).await?;
        Ok(())
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