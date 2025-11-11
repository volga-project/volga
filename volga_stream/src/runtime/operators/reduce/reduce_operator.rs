use std::{collections::HashMap, sync::{Arc, Mutex}, fmt};

use anyhow::Result;
use async_trait::async_trait;
use futures::StreamExt;

use crate::{common::{Key, Message}, runtime::{functions::reduce::{Accumulator, AggregationResultExtractor, AggregationResultExtractorTrait, ReduceFunction, ReduceFunctionTrait}, operators::operator::{MessageStream, OperatorBase, OperatorConfig, OperatorPollResult, OperatorTrait, OperatorType}, runtime_context::RuntimeContext}};


pub struct ReduceOperator {
    base: OperatorBase,
    accumulators: HashMap<Key, Arc<Mutex<Accumulator>>>,
    result_extractor: AggregationResultExtractor,
}

impl fmt::Debug for ReduceOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReduceOperator")
            .field("base", &self.base)
            .field("accumulators", &self.accumulators)
            .field("result_extractor", &self.result_extractor)
            .finish()
    }
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

    fn set_input(&mut self, input: Option<MessageStream>) {
        self.base.set_input(input);
    }

    async fn poll_next(&mut self) -> OperatorPollResult {
        let input_stream = self.base.input.as_mut().expect("input stream not set");
        match input_stream.next().await {
            Some(Message::Watermark(watermark)) => {
                return OperatorPollResult::Ready(Message::Watermark(watermark));
            }
            Some(message) => {
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
                        //     Some(result_message)
                        // }).await;

                        let mut acc = acc.lock().unwrap();
                        function.update_accumulator(&mut acc, &keyed_message);
                        let agg_result = function.get_result(&acc);
                        let result_message = result_extractor.extract_result(&key, &agg_result, upstream_vertex_id, ingest_ts);
                        OperatorPollResult::Ready(result_message)
                    },
                    _ => {
                        panic!("ReduceOperator requires KeyedMessage input")
                    }
                }
            }
            None => OperatorPollResult::None,
        }
    }

    fn operator_type(&self) -> OperatorType {
        self.base.operator_type()
    }
}
