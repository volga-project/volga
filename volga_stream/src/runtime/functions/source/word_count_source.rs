use async_trait::async_trait;
use anyhow::Result;
use std::fmt;
use crate::common::data_batch::DataBatch;
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
use crate::runtime::execution_graph::BatchingMode;

#[derive(Debug)]
pub struct WordCountSourceFunction {
    word_length: usize,
    num_words: usize,        // Total pool of words
    num_to_send_per_word: Option<usize>, // Optional: how many copies of each word to send
    run_for_s: Option<u64>,
    batch_size: usize,
    batching_mode: BatchingMode,
    words: Vec<String>,
    current_index: usize,
    start_time: Option<Instant>,
    words_sent_per_word: HashMap<String, usize>, // Track how many times each word has been sent
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

    fn create_batch(&self, words: &[String]) -> Result<DataBatch> {
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

        Ok(DataBatch::new(None, batch))
    }

    fn collect_words_for_batch(&mut self) -> Vec<String> {
        let mut batch_words = Vec::with_capacity(self.batch_size);
        
        match self.batching_mode {
            BatchingMode::SameWord => {
                // Find a word that hasn't been sent enough times
                let mut attempts = 0;
                while attempts < self.words.len() {
                    let word = &self.words[self.current_index];
                    let sent_count = self.words_sent_per_word.get(word).unwrap_or(&0);
                    
                    if let Some(num_to_send) = self.num_to_send_per_word {
                        if sent_count < &num_to_send {
                            // Calculate how many more copies we can send of this word
                            let remaining = num_to_send - sent_count;
                            // Don't try to fill beyond num_to_send_per_word
                            let to_add = std::cmp::min(remaining, std::cmp::min(self.batch_size, num_to_send));
                            
                            // Add copies of this word to the batch
                            for _ in 0..to_add {
                                batch_words.push(word.clone());
                                *self.words_sent_per_word.entry(word.clone()).or_insert(0) += 1;
                            }
                            // Always move to next word after adding what we can
                            self.current_index = (self.current_index + 1) % self.words.len();
                            break;
                        }
                    }
                    
                    self.current_index = (self.current_index + 1) % self.words.len();
                    attempts += 1;
                }
            },
            BatchingMode::MixedWord => {
                let mut words_collected = 0;
                let mut attempts = 0;
                let max_attempts = self.words.len(); // Prevent infinite loop

                while words_collected < self.batch_size && attempts < max_attempts {
                    let word = &self.words[self.current_index];
                    
                    let sent_count = self.words_sent_per_word.get(word).unwrap_or(&0);
                    if let Some(num_to_send) = self.num_to_send_per_word {
                        if sent_count >= &num_to_send {
                            self.current_index = (self.current_index + 1) % self.words.len();
                            attempts += 1;
                            continue;
                        }
                    }
                    
                    batch_words.push(word.clone());
                    *self.words_sent_per_word.entry(word.clone()).or_insert(0) += 1;
                    self.current_index = (self.current_index + 1) % self.words.len();
                    words_collected += 1;
                    attempts = 0; // Reset attempts when we successfully add a word
                }

                // If we couldn't collect any words after trying all words, return empty batch
                if words_collected == 0 {
                    return Vec::new();
                }
            }
        }
        
        batch_words
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
    async fn fetch(&mut self) -> Result<Option<DataBatch>> {
        // Check if we should stop based on time
        if let Some(run_for_s) = self.run_for_s {
            if let Some(start_time) = self.start_time {
                if start_time.elapsed().as_secs() >= run_for_s {
                    tokio::task::yield_now().await;
                    return Ok(None);
                }
            }
        }

        // Check if we should stop based on number of words to send per word
        if let Some(num_to_send_per_word) = self.num_to_send_per_word {
            let all_words_sent = self.words.iter().all(|word| {
                self.words_sent_per_word.get(word).unwrap_or(&0) >= &num_to_send_per_word
            });
            if all_words_sent {
                tokio::task::yield_now().await;
                return Ok(None);
            }
        }

        // Collect words for this batch
        let batch_words = self.collect_words_for_batch();

        // If we couldn't collect any words, return None
        if batch_words.is_empty() {
            tokio::task::yield_now().await;
            return Ok(None);
        }

        // Create and return batch
        let batch = self.create_batch(&batch_words)?;
        
        // Yield to other tasks
        tokio::task::yield_now().await;
        
        Ok(Some(batch))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::sleep;
    use std::time::Duration;
    use crate::runtime::execution_graph::BatchingMode;

    #[tokio::test]
    async fn test_word_count_source_timed() {
        // Test configuration
        let word_length = 5;
        let num_words = 100;      // Pool of words
        let run_for_s = 2;        // Run for 2 seconds
        let batch_size = 10;      // Batch size

        let mut source = WordCountSourceFunction::new(
            word_length,
            num_words,
            None, // No limit on number of words to send
            Some(run_for_s),
            batch_size,
            BatchingMode::MixedWord,
        );

        // Open the source
        source.open(&RuntimeContext::new("test".to_string(), 0, 1, None)).await.unwrap();

        // Collect all batches
        let mut total_words = 0;
        let mut batches = Vec::new();
        let mut all_words = Vec::new();
        let start_time = Instant::now();
        
        while let Some(batch) = source.fetch().await.unwrap() {
            let record_batch = batch.record_batch();
            let word_array = record_batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            let timestamp_array = record_batch.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
            
            // Verify batch structure
            assert_eq!(record_batch.num_columns(), 2);
            assert_eq!(word_array.len(), timestamp_array.len());
            
            // Collect words and verify their properties
            for i in 0..word_array.len() {
                let word = word_array.value(i).to_string();
                assert_eq!(word.len(), word_length);
                assert!(word.chars().all(|c| c.is_alphabetic()));
                all_words.push(word);
            }
            
            total_words += word_array.len();
            batches.push(batch);
            
            // Small sleep to simulate real-world processing
            sleep(Duration::from_millis(10)).await;
        }

        let elapsed = start_time.elapsed();
        
        // Verify that we ran for approximately the specified duration
        assert!(elapsed >= Duration::from_secs(run_for_s), "Should run for at least run_for_s seconds");
        assert!(elapsed <= Duration::from_secs(run_for_s + 1), "Should not run much longer than run_for_s seconds");
        
        // Verify that we got some words
        assert!(total_words > 0, "Should have sent some words");
        
        // Verify batch sizes
        for batch in batches.iter().take(batches.len() - 1) {
            assert_eq!(batch.record_batch().num_rows(), batch_size, "Full batches should have batch_size rows");
        }
        
        // Verify no duplicate words in a single batch
        for batch in &batches {
            let word_array = batch.record_batch().column(0).as_any().downcast_ref::<StringArray>().unwrap();
            let mut words_in_batch = std::collections::HashSet::new();
            for i in 0..word_array.len() {
                let word = word_array.value(i).to_string();
                assert!(words_in_batch.insert(word), "Duplicate word found in batch");
            }
        }
    }

    #[tokio::test]
    async fn test_word_count_source_same_word_batching() {
        let word_length = 5;
        let num_words = 3;
        let num_to_send_per_word = 4;
        let batch_size = 100; // Test with batch size larger than num_to_send_per_word

        let mut source = WordCountSourceFunction::new(
            word_length,
            num_words,
            Some(num_to_send_per_word),
            None,
            batch_size,
            BatchingMode::SameWord,
        );

        source.open(&RuntimeContext::new("test".to_string(), 0, 1, None)).await.unwrap();

        let mut batches = Vec::new();
        let mut word_counts = HashMap::new();
        
        while let Some(batch) = source.fetch().await.unwrap() {
            let record_batch = batch.record_batch();
            let word_array = record_batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            
            // Verify all words in batch are the same
            let first_word = word_array.value(0).to_string();
            for i in 1..word_array.len() {
                assert_eq!(word_array.value(i), first_word, "All words in batch should be identical");
            }
            
            // Verify batch size is correct
            assert!(word_array.len() <= num_to_send_per_word, 
                "Batch size should not exceed num_to_send_per_word in SameWord mode");
            
            // Count words
            for i in 0..word_array.len() {
                let word = word_array.value(i).to_string();
                *word_counts.entry(word).or_insert(0) += 1;
            }
            
            batches.push(batch);
        }

        // Verify each word was sent exactly num_to_send_per_word times
        assert_eq!(word_counts.len(), num_words);
        for (_, count) in word_counts {
            assert_eq!(count, num_to_send_per_word);
        }
        
        // Verify we have the expected number of batches
        // In SameWord mode, we should have exactly num_words batches
        // since we send all copies of a word in one batch
        assert_eq!(batches.len(), num_words);
    }

    #[tokio::test]
    async fn test_word_count_source_mixed_word_batching() {
        let word_length = 5;
        let num_words = 3;
        let num_to_send_per_word = 4;
        let batch_size = 100; // Test with batch size larger than total words to send

        let mut source = WordCountSourceFunction::new(
            word_length,
            num_words,
            Some(num_to_send_per_word),
            None,
            batch_size,
            BatchingMode::MixedWord,
        );

        source.open(&RuntimeContext::new("test".to_string(), 0, 1, None)).await.unwrap();

        let mut batches = Vec::new();
        let mut word_counts = HashMap::new();
        
        while let Some(batch) = source.fetch().await.unwrap() {
            let record_batch = batch.record_batch();
            let word_array = record_batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            
            // Verify batch is not empty
            assert!(!word_array.is_empty(), "Batch should not be empty");
            
            // Count words
            for i in 0..word_array.len() {
                let word = word_array.value(i).to_string();
                *word_counts.entry(word).or_insert(0) += 1;
            }
            
            batches.push(batch);
        }

        // Verify each word was sent exactly num_to_send_per_word times
        assert_eq!(word_counts.len(), num_words);
        for (_, count) in word_counts {
            assert_eq!(count, num_to_send_per_word);
        }
        
        // Calculate expected number of batches
        let total_words = num_words * num_to_send_per_word;
        let expected_batches = (total_words + batch_size - 1) / batch_size;
        assert_eq!(batches.len(), expected_batches, 
            "Expected {} batches for {} total words with batch size {}", 
            expected_batches, total_words, batch_size);
        
        // Verify all batches except possibly the last one are full
        for (i, batch) in batches.iter().enumerate() {
            let rows = batch.record_batch().num_rows();
            if i < batches.len() - 1 {
                assert_eq!(rows, batch_size, "All batches except the last one should be full");
            } else {
                assert!(rows <= batch_size, "Last batch should not exceed batch size");
                assert!(rows > 0, "Last batch should not be empty");
            }
        }
    }
} 