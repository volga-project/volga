use async_trait::async_trait;
use anyhow::Result;
use std::fmt;
use crate::common::message::{Message, MAX_WATERMARK_VALUE};
use crate::runtime::runtime_context::RuntimeContext;
use crate::runtime::functions::function_trait::FunctionTrait;
use std::any::Any;
use tokio::time::{timeout, Duration, Instant};
use rand::{thread_rng, Rng, distributions::Alphanumeric, SeedableRng, rngs::StdRng};
use std::time::SystemTime;
use arrow::array::{StringArray, Int64Array};
use arrow::datatypes::{Field, Schema};
use std::sync::Arc;
use arrow::array::Array;
use super::source_function::SourceFunctionTrait;
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchingMode {
    SameWord,  // Batch identical words together
    RoundRobin, // Batch words in round robin order
}

#[derive(Debug)]
pub struct WordCountSourceFunction {
    word_length: usize,
    dictionary_size: usize,
    num_to_send_per_word: Option<usize>,
    run_for_s: Option<u64>,
    batch_size: usize,
    batching_mode: BatchingMode,
    dictionary: Vec<String>,
    current_index: usize,
    start_time: Option<Instant>,
    copies_sent_per_word: HashMap<String, usize>,
    runtime_context: Option<RuntimeContext>,
    max_watermark_sent: bool,
}

impl WordCountSourceFunction {
    pub fn new(
        word_length: usize,
        dictionary_size: usize,
        num_to_send_per_word: Option<usize>,
        run_for_s: Option<u64>,
        batch_size: usize,
        batching_mode: BatchingMode,
    ) -> Self {
        Self {
            word_length,
            dictionary_size,
            num_to_send_per_word,
            run_for_s,
            batch_size,
            batching_mode,
            dictionary: Vec::new(),
            current_index: 0,
            start_time: None,
            copies_sent_per_word: HashMap::new(),
            runtime_context: None,
            max_watermark_sent: false,
        }
    }

    fn generate_random_word(&self, seed_offset: usize) -> String {
        // Use vertex_id as base seed, add offset for different words
        let base_seed = if let Some(ctx) = &self.runtime_context {
            // Hash the full vertex_id to get a unique seed per worker
            let vertex_id = ctx.vertex_id();
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut hasher = DefaultHasher::new();
            vertex_id.hash(&mut hasher);
            hasher.finish()
        } else {
            0
        };
        
        let seed = base_seed.wrapping_add(seed_offset as u64);
        let mut rng = StdRng::seed_from_u64(seed);
        
        std::iter::repeat(())
            .map(|_| rng.sample(Alphanumeric) as char)
            .filter(|c| c.is_alphabetic())
            .take(self.word_length)
            .collect()
    }

    fn create_batch(&self, words: &[String]) -> Message {
        let schema = Arc::new(Schema::new(vec![
            Field::new("word", arrow::datatypes::DataType::Utf8, false),
            Field::new("timestamp", arrow::datatypes::DataType::Int64, false),
        ]));

        // Convert Vec<String> to Vec<&str> for StringArray
        let word_refs: Vec<&str> = words.iter().map(|s| s.as_str()).collect();
        let word_array = StringArray::from(word_refs);
        
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let timestamp_array = Int64Array::from(vec![timestamp; words.len()]);

        let batch = arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![Arc::new(word_array), Arc::new(timestamp_array)],
        ).unwrap();

        Message::new(None, batch, None)
    }

    fn collect_words_for_batch(&mut self) -> Option<Vec<String>> {
        
        match self.batching_mode {
            BatchingMode::SameWord => {
                
                if let Some(num_to_send) = self.num_to_send_per_word {
                    // case when we send a fixed num of words
                    let start_index = self.current_index;
                    loop {
                        let word = &self.dictionary[self.current_index];
                        let sent_count = *self.copies_sent_per_word.get(word).unwrap_or(&0);
                
                        if sent_count < num_to_send {
                            // Calculate how many more copies we can send of this word
                            let remaining = num_to_send - sent_count;
                            let to_add = std::cmp::min(remaining, self.batch_size);
                            
                            // Create batch_words with exact capacity needed
                            let mut batch_words = Vec::with_capacity(to_add);
                            
                            // Add copies of this word to the batch
                            for _ in 0..to_add {
                                batch_words.push(word.clone());
                            }
                            
                            // Update sent count for this word
                            *self.copies_sent_per_word.entry(word.clone()).or_insert(0) += to_add;
                            
                            // If we've sent all copies of this word, move to next word
                            if sent_count + to_add >= num_to_send {
                                self.current_index = (self.current_index + 1) % self.dictionary.len();
                            }
                            
                            return Some(batch_words);
                        }
                        // Move to next word
                        self.current_index = (self.current_index + 1) % self.dictionary.len();

                        // If we've checked all words, break
                        if self.current_index == start_index {
                            break;
                        }
                    }
                } else {
                    // case when we send for a time period
                    let word = &self.dictionary[self.current_index];
                    if self.start_time.is_none() {
                        self.start_time = Some(Instant::now());
                    }
                    let run_for_s = self.run_for_s.unwrap();
                    let start_time = self.start_time.unwrap();
                    if start_time.elapsed().as_secs() >= run_for_s {
                        return None;
                    }

                    let batch_words = vec![word.clone(); self.batch_size];

                    self.current_index = (self.current_index + 1) % self.dictionary.len();

                    return Some(batch_words);
                }
            },
            BatchingMode::RoundRobin => {
                let mut batch_words = vec![];
                if let Some(num_to_send) = self.num_to_send_per_word {
                    // case when we send a fixed num of words
                    loop {
                        let word = &self.dictionary[self.current_index];
                        let sent_count = *self.copies_sent_per_word.get(word).unwrap_or(&0);
                        if sent_count < num_to_send {
                            batch_words.push(word.clone());
                            *self.copies_sent_per_word.entry(word.clone()).or_insert(0) += 1;
                        }
                        self.current_index = (self.current_index + 1) % self.dictionary.len();
                        if batch_words.len() == self.batch_size {
                            return Some(batch_words);
                        }

                        // check if we've sent all words
                        let all_sent = self.copies_sent_per_word.values().all(|count| *count >= num_to_send);
                        if all_sent {
                            if batch_words.len() > 0 {
                                return Some(batch_words);
                            } else {
                                return None;
                            }
                        }
                    }
                } else {
                    // case when we send for a time period
                    loop {
                        let word = &self.dictionary[self.current_index];
                        batch_words.push(word.clone());
                        self.current_index = (self.current_index + 1) % self.dictionary.len();

                        if batch_words.len() == self.batch_size {
                            return Some(batch_words);
                        }
                        if self.start_time.is_none() {
                            self.start_time = Some(Instant::now());
                        }
                        let run_for_s = self.run_for_s.unwrap();
                        let start_time = self.start_time.unwrap();
                        if start_time.elapsed().as_secs() >= run_for_s {
                            return None;
                        }
                    }
                }
            }
        }
        None
    }

    fn send_max_watermark_if_needed(&mut self) -> Option<Message> {
        if !self.max_watermark_sent {
            self.max_watermark_sent = true;
            let vertex_id = self.runtime_context.as_ref().unwrap().vertex_id().to_string();
            let watermark = Message::Watermark(crate::common::message::WatermarkMessage::new(
                vertex_id,
                MAX_WATERMARK_VALUE,
                Some(SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64)
            ));
            return Some(watermark);
        }
        None
    }   
}

#[async_trait]
impl FunctionTrait for WordCountSourceFunction {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()> {
        self.runtime_context = Some(context.clone());
        // Generate the pool of words using deterministic seeds
        self.dictionary = (0..self.dictionary_size)
            .map(|i| self.generate_random_word(i))
            .collect();
        Ok(())
    }
    
    async fn close(&mut self) -> Result<()> {
        let n = self.copies_sent_per_word.clone();
        // println!("WordCountSourceFunction {:?} close, words_sent_per_word {:?}", self.runtime_context.as_ref().unwrap().vertex_id(), n);
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
    
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[async_trait]
impl SourceFunctionTrait for WordCountSourceFunction {
    async fn fetch(&mut self) -> Option<Message> {

        // Collect words for this batch
        match self.collect_words_for_batch() {
            Some(words) => {
                Some(self.create_batch(&words))
            },
            None => {
                self.send_max_watermark_if_needed()
            }
        }
    }
}

async fn test_word_count_source(
    word_length: usize,
    dictionary_size: usize,
    num_to_send_per_word: Option<usize>,
    run_for_s: Option<u64>,
    batch_size: usize,
    batching_mode: BatchingMode,
) {
    let is_fixed_size = num_to_send_per_word.is_some();
    
    let mut source = WordCountSourceFunction::new(
        word_length,
        dictionary_size,
        num_to_send_per_word,
        run_for_s,
        batch_size,
        batching_mode,
    );

    source.open(&RuntimeContext::new("test".to_string(), 0, 1, None)).await.unwrap();

    let mut word_counts = HashMap::new();
    let mut watermark_received = false;
    
    let start_time = std::time::Instant::now();
    while let Some(message) = source.fetch().await {
        match message {
            Message::Regular(_) | Message::Keyed(_) => {
                let record_batch = message.record_batch();
                let word_array = record_batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
                
                if batching_mode == BatchingMode::SameWord {
                    // Verify all words in batch are the same
                    let first_word = word_array.value(0).to_string();
                    for i in 1..word_array.len() {
                        assert_eq!(word_array.value(i), first_word, "All words in batch should be identical");
                    }
                }

                // Count words
                for i in 0..word_array.len() {
                    let word = word_array.value(i).to_string();
                    *word_counts.entry(word).or_insert(0) += 1;
                }
            }
            Message::Watermark(watermark) => {
                assert_eq!(watermark.watermark_value, MAX_WATERMARK_VALUE, "Watermark should have max value");
                watermark_received = true;
            }
        }
    }

    // Verify each word was sent exactly num_to_send_per_word times
    if is_fixed_size {
        assert_eq!(word_counts.len(), dictionary_size);
        for (_, count) in word_counts {
            assert_eq!(count, num_to_send_per_word.unwrap());
        }
    } else {
        assert!(word_counts.len() > 0, "Should have sent some words");
        assert!(word_counts.len() <= dictionary_size, "Should not have sent more unique words than dictionary size");

        let elapsed_time = start_time.elapsed().as_secs();
        
        // Verify the source ran for approximately the specified time
        assert!(elapsed_time >= run_for_s.unwrap(), "Source should have run for at least {} seconds, but ran for {}", run_for_s.unwrap(), elapsed_time);
        assert!(elapsed_time <= run_for_s.unwrap() + 1, "Source should not have run much longer than {} seconds, but ran for {}", run_for_s.unwrap(), elapsed_time);
    }

    // Verify we received a watermark at the end
    assert!(watermark_received, "Should have received a watermark message at the end");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_word_count_source_same_word_fixed_size() {
        test_word_count_source(5, 100, Some(100), None, 10, BatchingMode::SameWord).await;
    }

    #[tokio::test]
    async fn test_word_count_source_same_word_fixed_time() {
        test_word_count_source(5, 100, None, Some(2), 10, BatchingMode::SameWord).await;
    }

    #[tokio::test]
    async fn test_word_count_source_round_robin_fixed_size() {
        test_word_count_source(5, 100, Some(100), None, 10, BatchingMode::RoundRobin).await;
    }

    #[tokio::test]
    async fn test_word_count_source_round_robin_fixed_time() {
        test_word_count_source(5, 100, None, Some(2), 10, BatchingMode::RoundRobin).await;
    }
} 