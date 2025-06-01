use async_trait::async_trait;
use anyhow::Result;
use std::fmt;
use crate::common::message::Message;
use crate::runtime::runtime_context::RuntimeContext;
use crate::runtime::functions::function_trait::FunctionTrait;
use std::any::Any;
use tokio::time::{timeout, Duration, Instant};
use rand::{thread_rng, Rng, distributions::Alphanumeric};
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
}

#[derive(Debug)]
pub struct WordCountSourceFunction {
    word_length: usize,
    num_words: usize,
    num_to_send_per_word: Option<usize>,
    run_for_s: Option<u64>,
    batch_size: usize,
    batching_mode: BatchingMode,
    words: Vec<String>,
    current_index: usize,
    start_time: Option<Instant>,
    words_sent_per_word: HashMap<String, usize>,
}

impl WordCountSourceFunction {
    pub fn new(
        word_length: usize,
        num_words: usize,
        num_to_send_per_word: Option<usize>,
        run_for_s: Option<u64>,
        batch_size: usize,
        batching_mode: BatchingMode,
    ) -> Self {
        Self {
            word_length,
            num_words,
            num_to_send_per_word,
            run_for_s,
            batch_size,
            batching_mode,
            words: Vec::new(),
            current_index: 0,
            start_time: None,
            words_sent_per_word: HashMap::new(),
        }
    }

    fn generate_random_word(&self) -> String {
        let mut rng = thread_rng();
        std::iter::repeat(())
            .map(|_| rng.sample(Alphanumeric) as char)
            .filter(|c| c.is_alphabetic())
            .take(self.word_length)
            .collect()
    }

    fn create_batch(&self, words: &[String]) -> Result<Message> {
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
        )?;

        Ok(Message::new(None, batch))
    }

    fn collect_words_for_batch(&mut self) -> Option<Vec<String>> {
        let start_index = self.current_index;
        
        match self.batching_mode {
            BatchingMode::SameWord => {
                loop {
                    let word = &self.words[self.current_index];
                    let sent_count = *self.words_sent_per_word.get(word).unwrap_or(&0);
                    
                    if let Some(num_to_send) = self.num_to_send_per_word {
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
                            *self.words_sent_per_word.entry(word.clone()).or_insert(0) += to_add;
                            
                            // If we've sent all copies of this word, move to next word
                            if sent_count + to_add >= num_to_send {
                                self.current_index = (self.current_index + 1) % self.words.len();
                            }
                            
                            return Some(batch_words);
                        }
                    }
                    
                    // Move to next word
                    self.current_index = (self.current_index + 1) % self.words.len();
                    
                    // If we've checked all words, break
                    if self.current_index == start_index {
                        break;
                    }
                }
            }
        }
        None
    }
}

#[async_trait]
impl FunctionTrait for WordCountSourceFunction {
    async fn open(&mut self, _context: &RuntimeContext) -> Result<()> {
        // Generate the pool of words
        self.words = (0..self.num_words)
            .map(|_| self.generate_random_word())
            .collect();
        self.start_time = Some(Instant::now());
        Ok(())
    }
    
    async fn close(&mut self) -> Result<()> {
        Ok(())
    }
    
    async fn finish(&mut self) -> Result<()> {
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
    async fn fetch(&mut self) -> Result<Option<Message>> {
        // Check if we should stop based on time
        if let Some(run_for_s) = self.run_for_s {
            if let Some(start_time) = self.start_time {
                if start_time.elapsed().as_secs() >= run_for_s {
                    tokio::task::yield_now().await;
                    return Ok(None);
                }
            }
        }

        // Collect words for this batch
        let batch_words = match self.collect_words_for_batch() {
            Some(words) => words,
            None => {
                tokio::task::yield_now().await;
                return Ok(None);
            }
        };

        // Create and return batch
        let message = self.create_batch(&batch_words)?;
        
        // Yield to other tasks
        tokio::task::yield_now().await;
        
        Ok(Some(message))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_word_count_source() {
        let word_length = 5;
        let num_words = 1000;
        let num_to_send_per_word = 100;
        let batch_size = 50;

        let mut source = WordCountSourceFunction::new(
            word_length,
            num_words,
            Some(num_to_send_per_word),
            None,
            batch_size,
            BatchingMode::SameWord,
        );

        source.open(&RuntimeContext::new("test".to_string(), 0, 1, None)).await.unwrap();

        let mut messages = Vec::new();
        let mut word_counts = HashMap::new();
        
        while let Some(message) = source.fetch().await.unwrap() {
            let record_batch = message.record_batch();
            let word_array = record_batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            
            // Verify all words in batch are the same
            let first_word = word_array.value(0).to_string();
            for i in 1..word_array.len() {
                assert_eq!(word_array.value(i), first_word, "All words in batch should be identical");
            }

            // Count words
            for i in 0..word_array.len() {
                let word = word_array.value(i).to_string();
                *word_counts.entry(word).or_insert(0) += 1;
            }
            
            messages.push(message);
        }

        // Verify each word was sent exactly num_to_send_per_word times
        assert_eq!(word_counts.len(), num_words);
        for (_, count) in word_counts {
            assert_eq!(count, num_to_send_per_word);
        }
    }
} 