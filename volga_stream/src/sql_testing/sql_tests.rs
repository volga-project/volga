use crate::{
    api::streaming_context::StreamingContext,
    common::{message::Message, test_utils::{gen_unique_grpc_port, verify_message_records_match}, WatermarkMessage, MAX_WATERMARK_VALUE},
    executor::local_executor::LocalExecutor,
    runtime::operators::{sink::sink_operator::SinkConfig, source::source_operator::{SourceConfig, VectorSourceConfig}},
    storage::{InMemoryStorageClient, InMemoryStorageServer}
};
use anyhow::Result;

use arrow::{
    datatypes::{Schema, Field, DataType},
    array::{StringArray, Int32Array, Int64Array, Float64Array},
    record_batch::RecordBatch
};
use std::sync::Arc;

/// Test case definition for SQL queries
#[derive(Debug)]
struct SqlTestCase {
    name: &'static str,
    sql: &'static str,
    input_batches: Vec<RecordBatch>,
    expected_batch: RecordBatch,
    schema: Arc<Schema>,
}

/// Helper function to create a record batch with mixed data types
fn create_mixed_batch(
    ids: Vec<i32>,
    names: Vec<&str>,
    values: Vec<f64>
) -> RecordBatch {
    let id_array = Int32Array::from(ids);
    let name_array = StringArray::from(names);
    let value_array = Float64Array::from(values);
    
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(id_array),
            Arc::new(name_array),
            Arc::new(value_array),
        ]
    ).unwrap()
}

/// Helper function to create a record batch with just names and values
fn create_name_value_batch(
    names: Vec<&str>,
    values: Vec<f64>
) -> RecordBatch {
    let name_array = StringArray::from(names);
    let value_array = Float64Array::from(values);
    
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(name_array),
            Arc::new(value_array),
        ]
    ).unwrap()
}

/// Helper function to create a record batch with aggregated results
fn create_agg_batch(
    names: Vec<&str>,
    counts: Vec<i64>
) -> RecordBatch {
    let name_array = StringArray::from(names);
    let count_array = Int64Array::from(counts);
    
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("count", DataType::Int64, false),
    ]));
    
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(name_array),
            Arc::new(count_array),
        ]
    ).unwrap()
}

fn get_test_cases() -> Vec<SqlTestCase> {
    let base_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));

    // Input data for all tests
    let input_batches = vec![
        create_mixed_batch(
            vec![1, 2, 3],
            vec!["alice", "bob", "charlie"],
            vec![10.0, 20.0, 30.0]
        ),
        create_mixed_batch(
            vec![4, 5, 6],
            vec!["alice", "bob", "alice"],
            vec![15.0, 25.0, 35.0]
        ),
    ];

    vec![
        // Test 1: Simple SELECT *
        SqlTestCase {
            name: "select_all",
            sql: "SELECT * FROM test_table",
            input_batches: input_batches.clone(),
            expected_batch: create_mixed_batch(
                vec![1, 2, 3, 4, 5, 6],
                vec!["alice", "bob", "charlie", "alice", "bob", "alice"],
                vec![10.0, 20.0, 30.0, 15.0, 25.0, 35.0]
            ),
            schema: base_schema.clone(),
        },

        // Test 2: SELECT specific columns
        SqlTestCase {
            name: "select_columns",
            sql: "SELECT name, value FROM test_table",
            input_batches: input_batches.clone(),
            expected_batch: create_name_value_batch(
                vec!["alice", "bob", "charlie", "alice", "bob", "alice"],
                vec![10.0, 20.0, 30.0, 15.0, 25.0, 35.0]
            ),
            schema: base_schema.clone(),
        },

        // Test 3: WHERE clause with string comparison
        SqlTestCase {
            name: "where_string",
            sql: "SELECT * FROM test_table WHERE name = 'alice'",
            input_batches: input_batches.clone(),
            expected_batch: create_mixed_batch(
                vec![1, 4, 6],
                vec!["alice", "alice", "alice"],
                vec![10.0, 15.0, 35.0]
            ),
            schema: base_schema.clone(),
        },

        // Test 4: WHERE clause with numeric comparison
        SqlTestCase {
            name: "where_numeric",
            sql: "SELECT * FROM test_table WHERE value > 20.0",
            input_batches: input_batches.clone(),
            expected_batch: create_mixed_batch(
                vec![3, 5, 6],
                vec!["charlie", "bob", "alice"],
                vec![30.0, 25.0, 35.0]
            ),
            schema: base_schema.clone(),
        },

        // Test 5: GROUP BY with COUNT
        SqlTestCase {
            name: "group_by_count",
            sql: "SELECT name, COUNT(*) as count FROM test_table GROUP BY name",
            input_batches: input_batches.clone(),
            expected_batch: create_agg_batch(
                vec!["alice", "bob", "charlie"],
                vec![3, 2, 1]
            ),
            schema: base_schema.clone(),
        },
    ]
}

/// Run a single SQL test case
async fn run_sql_test_case(test_case: &SqlTestCase) -> Result<()> {
    // Convert input batches to messages
    let mut test_messages: Vec<Message> = test_case.input_batches
        .iter()
        .map(|batch| Message::new(None, batch.clone(), None))
        .collect();
    
    test_messages.push(Message::Watermark(WatermarkMessage::new(
        "source".to_string(),
        MAX_WATERMARK_VALUE,
        None,
    )));

    let storage_server_addr = format!("127.0.0.1:{}", gen_unique_grpc_port());
    
    // Create streaming context
    let context = StreamingContext::new()
        .with_parallelism(1)
        .with_source(
            "test_table".to_string(),
            SourceConfig::VectorSourceConfig(VectorSourceConfig::new(test_messages)),
            test_case.schema.clone()
        )
        .with_sink(SinkConfig::InMemoryStorageGrpcSinkConfig(format!("http://{}", storage_server_addr)))
        .sql(test_case.sql)
        .with_executor(Box::new(LocalExecutor::new()));

    // Start storage server and execute
    let mut storage_server = InMemoryStorageServer::new();
    storage_server.start(&storage_server_addr).await.unwrap();
    
    context.execute().await.unwrap();
    
    let mut client = InMemoryStorageClient::new(format!("http://{}", storage_server_addr)).await.unwrap();
    let actual_messages = client.get_vector().await.unwrap();
    storage_server.stop().await;

    // Convert expected batch to message for comparison
    let expected_messages = vec![Message::new(None, test_case.expected_batch.clone(), None)];

    // Verify results
    if test_case.name == "group_by_count" {
        verify_message_records_match(&expected_messages, &actual_messages, test_case.name, false);
    } else {
        verify_message_records_match(&expected_messages, &actual_messages, test_case.name, true);
    }
    
    println!("✓ SQL test '{}' passed", test_case.name);
    Ok(())
}

#[tokio::test]
async fn test_sql_queries() -> Result<()> {
    let test_cases = get_test_cases();
    
    println!("Running {} SQL test cases...", test_cases.len());
    
    for test_case in &test_cases {
        println!("Running test: {} - SQL: {}", test_case.name, test_case.sql);
        run_sql_test_case(test_case).await?;
    }
    
    println!("All SQL tests passed! ✓");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_select_all() -> Result<()> {
        let test_cases = get_test_cases();
        let test_case = test_cases.iter().find(|tc| tc.name == "select_all").unwrap();
        run_sql_test_case(test_case).await
    }

    #[tokio::test]
    async fn test_select_columns() -> Result<()> {
        let test_cases = get_test_cases();
        let test_case = test_cases.iter().find(|tc| tc.name == "select_columns").unwrap();
        run_sql_test_case(test_case).await
    }

    #[tokio::test]
    async fn test_where_string() -> Result<()> {
        let test_cases = get_test_cases();
        let test_case = test_cases.iter().find(|tc| tc.name == "where_string").unwrap();
        run_sql_test_case(test_case).await
    }

    #[tokio::test]
    async fn test_where_numeric() -> Result<()> {
        let test_cases = get_test_cases();
        let test_case = test_cases.iter().find(|tc| tc.name == "where_numeric").unwrap();
        run_sql_test_case(test_case).await
    }

    #[tokio::test]
    async fn test_group_by_count() -> Result<()> {
        let test_cases = get_test_cases();
        let test_case = test_cases.iter().find(|tc| tc.name == "group_by_count").unwrap();
        run_sql_test_case(test_case).await
    }
}