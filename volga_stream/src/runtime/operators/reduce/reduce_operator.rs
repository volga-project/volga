use std::{collections::HashMap, sync::{Arc, Mutex}};

use anyhow::Result;
use async_trait::async_trait;

use tokio_rayon::AsyncThreadPool;

use crate::{common::{Key, Message}, runtime::{functions::reduce::{Accumulator, AggregationResultExtractor, AggregationResultExtractorTrait, ReduceFunction, ReduceFunctionTrait}, operators::operator::{OperatorBase, OperatorTrait, OperatorType, OperatorConfig}, runtime_context::RuntimeContext}};


#[derive(Debug)]
pub struct ReduceOperator {
    base: OperatorBase,
    accumulators: HashMap<Key, Arc<Mutex<Accumulator>>>,
    result_extractor: AggregationResultExtractor,
}

impl ReduceOperator {
    pub fn new(config: OperatorConfig) -> Self {
        let (reduce_function, extractor) = match config.clone() {
            OperatorConfig::ReduceConfig(reduce_function, extractor) => (reduce_function, extractor),
            _ => panic!("Expected ReduceConfig, got {:?}", config),
        };
        Self {
            base: OperatorBase::new_with_function(reduce_function, config),
            accumulators: HashMap::new(),
            result_extractor: extractor.unwrap_or_else(AggregationResultExtractor::all_aggregations),
        }
    }
}

#[async_trait]
impl OperatorTrait for ReduceOperator {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()> {
        self.base.open(context).await
    }

    async fn close(&mut self) -> Result<()> {
        let vertex_id = self.base.runtime_context.as_ref().unwrap().vertex_id();
        self.base.close().await
    }

    async fn process_message(&mut self, message: Message) -> Option<Vec<Message>> {
        let upstream_vertex_id = message.upstream_vertex_id();
        let ingest_ts = message.ingest_timestamp();
        // Explicitly check and handle only KeyedMessage
        match message {
            Message::Keyed(keyed_message) => {
                let key = keyed_message.key().clone();
                
                // Check if we need to create a new accumulator or update an existing one
                let acc_exists = self.accumulators.contains_key(&key);
                
                let function = self.base.get_function_mut::<ReduceFunction>().unwrap();

                if !acc_exists {
                    // Create a new accumulator
                    self.accumulators.insert(key.clone(), Arc::new(Mutex::new(function.create_accumulator())));
                }
                
                // Now that we've updated the accumulator, get the result
                let acc = self.accumulators.get_mut(&key)
                    .unwrap();
                
                let keyed_message = keyed_message.clone();
                let function = function.clone();
                let result_extractor = self.result_extractor.clone();
                let acc = acc.clone();
  
                // make sure we use fifo to maintain order 
                // let result = self.base.thread_pool.spawn_fifo_async(move || {
                //     let mut acc = acc.lock().unwrap();
                //     function.update_accumulator(&mut acc, &keyed_message);
                //     let agg_result = function.get_result(&acc);
                //     let result_message = result_extractor.extract_result(&key, &agg_result, upstream_vertex_id, ingest_ts);
                //     Some(vec![result_message])
                // }).await;


                let mut acc = acc.lock().unwrap();
                function.update_accumulator(&mut acc, &keyed_message);
                let agg_result = function.get_result(&acc);
                let result_message = result_extractor.extract_result(&key, &agg_result, upstream_vertex_id, ingest_ts);
                let result = Some(vec![result_message]);

                return result;
            },
            _ => {
                panic!("ReduceOperator requires KeyedMessage input")
            }
        }
    }

    fn operator_type(&self) -> OperatorType {
        self.base.operator_type()
    }
}