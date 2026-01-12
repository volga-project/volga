use crate::{
    api::pipeline_context::PipelineContextBuilder,
    common::test_utils::{gen_unique_grpc_port, print_pipeline_state},
    executor::local_executor::LocalExecutor,
    runtime::{
        functions::{
            source::word_count_source::BatchingMode,
        },
        operators::{
            sink::sink_operator::SinkConfig,
            source::source_operator::{SourceConfig, WordCountSourceConfig},
        },
    },
    storage::{InMemoryStorageClient, InMemoryStorageServer}
};
use anyhow::Result;
use std::{collections::HashMap, sync::Arc};
use tokio::runtime::Runtime;
use arrow::{array::StringArray, datatypes::{Field, Schema}};

// TODO: we may have colliding words since each source worker generates it's own
#[test]
fn test_word_count() -> Result<()> {
    // Create runtime for async operations
    let runtime = Runtime::new()?;

    let storage_server_addr = format!("127.0.0.1:{}", gen_unique_grpc_port());
    let parallelism = 2;
    let word_length = 10;
    let dictionary_size_per_source = 10; // Number of unique words per source vertex
    let num_to_send_per_word = 1000; // Number of copies of each word to send
    let batch_size = 10;

    // Create streaming context with the logical graph and executor
    let context = PipelineContextBuilder::new()
        .with_parallelism(parallelism)
        .with_source(
            "word_count_source".to_string(), 
            SourceConfig::WordCountSourceConfig(WordCountSourceConfig::new(
                word_length,
                dictionary_size_per_source,
                Some(num_to_send_per_word),
                None, // No time limit
                batch_size,
                BatchingMode::SameWord,
            )), 
            Arc::new(Schema::new(vec![
                Field::new("word", arrow::datatypes::DataType::Utf8, false),
                Field::new("timestamp", arrow::datatypes::DataType::Int64, false),
            ]))
        )
        .with_sink(SinkConfig::InMemoryStorageGrpcSinkConfig(format!("http://{}", storage_server_addr)))
        .sql("SELECT word, COUNT(*) as count FROM word_count_source GROUP BY word")
        .with_executor(Box::new(LocalExecutor::new()))
        .build();

    let (result_vec, pipeline_state) = runtime.block_on(async {
        let mut storage_server = InMemoryStorageServer::new();
        storage_server.start(&storage_server_addr).await.unwrap();
        
        let pipeline_state = context.execute().await.unwrap();
        
        let mut client = InMemoryStorageClient::new(format!("http://{}", storage_server_addr)).await.unwrap();
        let result_vec = client.get_vector().await.unwrap();
        storage_server.stop().await;
        (result_vec, pipeline_state)
    });

    println!("{:?}", result_vec);
    print_pipeline_state(&pipeline_state, None, false, false, None, None);

    assert_eq!(result_vec.len(), parallelism, "Should have exactly one message per parallelism instance");

    // Collect word counts from all batches
    let mut word_counts = HashMap::new();
    
    for (batch_idx, message) in result_vec.iter().enumerate() {
        let batch = message.record_batch();
        
        // Verify we have the expected number of columns
        assert_eq!(batch.num_columns(), 2, "Batch {} should have 2 columns: word and count", batch_idx);
        
        // Extract columns from the batch
        let word_column = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let count_column = batch.column(1).as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
        
        // Process each row in this batch
        for i in 0..batch.num_rows() {
            let word = word_column.value(i).to_string();
            let count = count_column.value(i);
            
            // Accumulate counts (in case the same word appears in multiple batches)
            *word_counts.entry(word).or_insert(0) += count;
        }
        
        println!("Batch {}: {} rows", batch_idx, batch.num_rows());
    }
    
    // Verify total unique words
    assert_eq!(word_counts.len(), dictionary_size_per_source * parallelism, 
               "Should have exactly {} unique words (dictionary_size_per_source * parallelism)", 
               dictionary_size_per_source * parallelism);
    
    let mut failed_words = Vec::new();
    for (word, count) in &word_counts {
        if *count != num_to_send_per_word as i64 {
            failed_words.push((word.clone(), *count));
        }
    }
    
    if !failed_words.is_empty() {
        println!("Words that don't match expected count of {}:", num_to_send_per_word);
        for (word, count) in &failed_words {
            println!("  Word: '{}', Expected: {}, Actual: {}", word, num_to_send_per_word, count);
        }
        panic!("Found {} words with incorrect counts", failed_words.len());
    }
    
    println!("Successfully verified {} unique words, each with count {}", word_counts.len(), num_to_send_per_word);

    Ok(())
} 