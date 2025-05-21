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

#[derive(Debug)]
pub struct WordCountSourceFunction {
    word_length: usize,
    num_words: usize,        // Total pool of words
    num_to_send: Option<usize>, // Optional: how many words to actually send
    run_for_s: Option<u64>,
    batch_size: usize,
    words: Vec<String>,
    current_index: usize,
    start_time: Option<Instant>,
    words_sent: usize,
}

impl WordCountSourceFunction {
    pub fn new(
        word_length: usize,
        num_words: usize,
        num_to_send: Option<usize>,
        run_for_s: Option<u64>,
        batch_size: usize,
    ) -> Self {
        Self {
            word_length,
            num_words,
            num_to_send,
            run_for_s,
            batch_size,
            words: Vec::new(),
            current_index: 0,
            start_time: None,
            words_sent: 0,
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

        // Check if we should stop based on number of words to send
        if let Some(num_to_send) = self.num_to_send {
            if self.words_sent >= num_to_send {
                tokio::task::yield_now().await;
                return Ok(None);
            }
        }

        // Calculate how many words we can send in this batch
        let remaining_words = if let Some(num_to_send) = self.num_to_send {
            num_to_send.saturating_sub(self.words_sent)
        } else {
            usize::MAX // No limit on words to send
        };
        let batch_size = self.batch_size.min(remaining_words);

        // If we can't send any more words, return None
        if batch_size == 0 {
            tokio::task::yield_now().await;
            return Ok(None);
        }

        // Collect words for this batch
        let mut batch_words = Vec::with_capacity(batch_size);
        let mut words_collected = 0;
        
        while words_collected < batch_size {
            // Check if we've reached the remaining words limit
            if words_collected >= remaining_words {
                break;
            }
            
            let word = &self.words[self.current_index];
            batch_words.push(word.clone());
            self.current_index = (self.current_index + 1) % self.words.len();
            self.words_sent += 1;
            words_collected += 1;
        }

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

    #[tokio::test]
    async fn test_word_count_source() {
        // Test configuration
        let word_length = 5;
        let num_words = 100;      // Pool of words
        let num_to_send = 1009;   // Number of words to send
        let batch_size = 10;      // Batch size

        let mut source = WordCountSourceFunction::new(
            word_length,
            num_words,
            Some(num_to_send),
            None, // run_for_s no-op
            batch_size,
        );

        // Open the source
        source.open(&RuntimeContext::new("test".to_string(), 0, 1, None)).await.unwrap();

        // Collect all batches
        let mut total_words = 0;
        let mut batches = Vec::new();
        let mut all_words = Vec::new();
        
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

        // Verify total words sent
        assert_eq!(total_words, num_to_send, "Should send exactly num_to_send words");
        
        // Verify batch sizes
        let expected_full_batches = num_to_send / batch_size;
        let expected_partial_batch_size = num_to_send % batch_size;
        
        assert_eq!(batches.len(), expected_full_batches + (if expected_partial_batch_size > 0 { 1 } else { 0 }));
        
        // Verify full batches
        for batch in batches.iter().take(expected_full_batches) {
            assert_eq!(batch.record_batch().num_rows(), batch_size);
        }
        
        // Verify partial batch if it exists
        if expected_partial_batch_size > 0 {
            assert_eq!(batches.last().unwrap().record_batch().num_rows(), expected_partial_batch_size);
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
} 