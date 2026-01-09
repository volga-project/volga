use crate::{
    api::pipeline_context::{ExecutionProfile, PipelineContext, PipelineContextBuilder},
    common::{message::Message, test_utils::{gen_unique_grpc_port, verify_message_records_match}, WatermarkMessage, MAX_WATERMARK_VALUE},
    runtime::operators::{sink::sink_operator::SinkConfig, source::source_operator::{SourceConfig, VectorSourceConfig}},
    storage::{InMemoryStorageClient, InMemoryStorageServer}
};
use anyhow::Result;

use arrow::{
    datatypes::{Schema, Field, DataType},
    array::{StringArray, Int32Array, Int64Array, Float64Array, BooleanArray},
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

/// Helper function to create a record batch with sum aggregation
fn create_sum_batch(
    names: Vec<&str>,
    sums: Vec<f64>
) -> RecordBatch {
    let name_array = StringArray::from(names);
    let sum_array = Float64Array::from(sums);
    
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("sum", DataType::Float64, true),  // SUM is nullable in DataFusion
    ]));
    
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(name_array),
            Arc::new(sum_array),
        ]
    ).unwrap()
}

/// Helper function to create a record batch with average aggregation
fn create_avg_batch(
    names: Vec<&str>,
    avgs: Vec<f64>
) -> RecordBatch {
    let name_array = StringArray::from(names);
    let avg_array = Float64Array::from(avgs);
    
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("avg", DataType::Float64, true),  // AVG is nullable in DataFusion
    ]));
    
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(name_array),
            Arc::new(avg_array),
        ]
    ).unwrap()
}

/// Helper function to create a record batch with min/max aggregation
fn create_min_max_batch(
    names: Vec<&str>,
    mins: Vec<f64>,
    maxs: Vec<f64>
) -> RecordBatch {
    let name_array = StringArray::from(names);
    let min_array = Float64Array::from(mins);
    let max_array = Float64Array::from(maxs);
    
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("min", DataType::Float64, true),  // MIN is nullable in DataFusion
        Field::new("max", DataType::Float64, true),  // MAX is nullable in DataFusion
    ]));
    
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(name_array),
            Arc::new(min_array),
            Arc::new(max_array),
        ]
    ).unwrap()
}

/// Helper function to create a record batch with boolean values
fn create_bool_batch(
    ids: Vec<i32>,
    names: Vec<&str>,
    is_active: Vec<bool>
) -> RecordBatch {
    let id_array = Int32Array::from(ids);
    let name_array = StringArray::from(names);
    let bool_array = BooleanArray::from(is_active);
    
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("is_active", DataType::Boolean, false),
    ]));
    
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(id_array),
            Arc::new(name_array),
            Arc::new(bool_array),
        ]
    ).unwrap()
}

/// Helper function to create a record batch with computed values
fn create_computed_batch(
    ids: Vec<i32>,
    names: Vec<&str>,
    computed_values: Vec<f64>
) -> RecordBatch {
    let id_array = Int32Array::from(ids);
    let name_array = StringArray::from(names);
    let computed_array = Float64Array::from(computed_values);
    
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("computed_value", DataType::Float64, false),
    ]));
    
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(id_array),
            Arc::new(name_array),
            Arc::new(computed_array),
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

        // Test 6: GROUP BY with SUM
        SqlTestCase {
            name: "group_by_sum",
            sql: "SELECT name, SUM(value) as sum FROM test_table GROUP BY name",
            input_batches: input_batches.clone(),
            expected_batch: create_sum_batch(
                vec!["alice", "bob", "charlie"],
                vec![60.0, 45.0, 30.0]  // alice: 10+15+35, bob: 20+25, charlie: 30
            ),
            schema: base_schema.clone(),
        },

        // Test 7: GROUP BY with AVG
        SqlTestCase {
            name: "group_by_avg",
            sql: "SELECT name, AVG(value) as avg FROM test_table GROUP BY name",
            input_batches: input_batches.clone(),
            expected_batch: create_avg_batch(
                vec!["alice", "bob", "charlie"],
                vec![20.0, 22.5, 30.0]  // alice: 60/3, bob: 45/2, charlie: 30/1
            ),
            schema: base_schema.clone(),
        },

        // Test 8: GROUP BY with MIN and MAX
        SqlTestCase {
            name: "group_by_min_max",
            sql: "SELECT name, MIN(value) as min, MAX(value) as max FROM test_table GROUP BY name",
            input_batches: input_batches.clone(),
            expected_batch: create_min_max_batch(
                vec!["alice", "bob", "charlie"],
                vec![10.0, 20.0, 30.0],  // min values
                vec![35.0, 25.0, 30.0]   // max values
            ),
            schema: base_schema.clone(),
        },

        // Test 9: WHERE with AND condition
        SqlTestCase {
            name: "where_and",
            sql: "SELECT * FROM test_table WHERE value > 15.0 AND name != 'charlie'",
            input_batches: input_batches.clone(),
            expected_batch: create_mixed_batch(
                vec![2, 5, 6],
                vec!["bob", "bob", "alice"],
                vec![20.0, 25.0, 35.0]
            ),
            schema: base_schema.clone(),
        },

        // Test 10: WHERE with OR condition
        SqlTestCase {
            name: "where_or",
            sql: "SELECT * FROM test_table WHERE name = 'alice' OR value < 15.0",
            input_batches: input_batches.clone(),
            expected_batch: create_mixed_batch(
                vec![1, 4, 6],
                vec!["alice", "alice", "alice"],
                vec![10.0, 15.0, 35.0]
            ),
            schema: base_schema.clone(),
        },

        // Test 11: WHERE with IN clause
        SqlTestCase {
            name: "where_in",
            sql: "SELECT * FROM test_table WHERE name IN ('alice', 'charlie')",
            input_batches: input_batches.clone(),
            expected_batch: create_mixed_batch(
                vec![1, 3, 4, 6],
                vec!["alice", "charlie", "alice", "alice"],
                vec![10.0, 30.0, 15.0, 35.0]
            ),
            schema: base_schema.clone(),
        },

        // Test 12: WHERE with BETWEEN
        SqlTestCase {
            name: "where_between",
            sql: "SELECT * FROM test_table WHERE value BETWEEN 15.0 AND 25.0",
            input_batches: input_batches.clone(),
            expected_batch: create_mixed_batch(
                vec![2, 4, 5],
                vec!["bob", "alice", "bob"],
                vec![20.0, 15.0, 25.0]
            ),
            schema: base_schema.clone(),
        },

        // Test 13: WHERE with LIKE pattern matching
        SqlTestCase {
            name: "where_like",
            sql: "SELECT * FROM test_table WHERE name LIKE 'a%'",
            input_batches: input_batches.clone(),
            expected_batch: create_mixed_batch(
                vec![1, 4, 6],
                vec!["alice", "alice", "alice"],
                vec![10.0, 15.0, 35.0]
            ),
            schema: base_schema.clone(),
        },

        // Test 14: SELECT with computed columns
        SqlTestCase {
            name: "select_computed",
            sql: "SELECT id, name, value * 2 as computed_value FROM test_table",
            input_batches: input_batches.clone(),
            expected_batch: create_computed_batch(
                vec![1, 2, 3, 4, 5, 6],
                vec!["alice", "bob", "charlie", "alice", "bob", "alice"],
                vec![20.0, 40.0, 60.0, 30.0, 50.0, 70.0]
            ),
            schema: base_schema.clone(),
        },

        // Test 15: SELECT with CASE WHEN
        SqlTestCase {
            name: "select_case_when",
            sql: "SELECT id, name, CASE WHEN value > 20.0 THEN true ELSE false END as is_active FROM test_table",
            input_batches: input_batches.clone(),
            expected_batch: create_bool_batch(
                vec![1, 2, 3, 4, 5, 6],
                vec!["alice", "bob", "charlie", "alice", "bob", "alice"],
                vec![false, false, true, false, true, true]
            ),
            schema: base_schema.clone(),
        },

        // Test 18: GROUP BY with HAVING
        SqlTestCase {
            name: "group_by_having",
            sql: "SELECT name, COUNT(*) as count FROM test_table GROUP BY name HAVING COUNT(*) > 1",
            input_batches: input_batches.clone(),
            expected_batch: create_agg_batch(
                vec!["alice", "bob"],
                vec![3, 2]
            ),
            schema: base_schema.clone(),
        },

        // Test 20: Complex WHERE with multiple conditions
        SqlTestCase {
            name: "where_complex",
            sql: "SELECT * FROM test_table WHERE (name = 'alice' AND value > 10.0) OR (name = 'bob' AND value < 25.0)",
            input_batches: input_batches.clone(),
            expected_batch: create_mixed_batch(
                vec![2, 4, 6],
                vec!["bob", "alice", "alice"],
                vec![20.0, 15.0, 35.0]
            ),
            schema: base_schema.clone(),
        },

        // Test 21: Column and table aliases
        SqlTestCase {
            name: "aliases",
            sql: "SELECT t.id as user_id, t.name as user_name, t.value as score FROM test_table as t WHERE t.value > 15.0",
            input_batches: input_batches.clone(),
            expected_batch: RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("user_id", DataType::Int32, false),
                    Field::new("user_name", DataType::Utf8, false),
                    Field::new("score", DataType::Float64, false),
                ])),
                vec![
                    Arc::new(Int32Array::from(vec![2, 3, 5, 6])),
                    Arc::new(StringArray::from(vec!["bob", "charlie", "bob", "alice"])),
                    Arc::new(Float64Array::from(vec![20.0, 30.0, 25.0, 35.0])),
                ]
            ).unwrap(),
            schema: base_schema.clone(),
        },


    ]
}

/// Run a single SQL test case
async fn run_sql_test_case(test_case: &SqlTestCase) -> Result<()> {
    // Convert input batches to messages
    let mut test_messages: Vec<Message> = test_case.input_batches
        .iter()
        .map(|batch| Message::new(None, batch.clone(), None, None))
        .collect();
    
    test_messages.push(Message::Watermark(WatermarkMessage::new(
        "source".to_string(),
        MAX_WATERMARK_VALUE,
        None,
    )));

    let storage_server_addr = format!("127.0.0.1:{}", gen_unique_grpc_port());
    
    // Create streaming context
    let context = PipelineContextBuilder::new()
        .with_parallelism(1)
        .with_source(
            "test_table".to_string(),
            SourceConfig::VectorSourceConfig(VectorSourceConfig::new(test_messages)),
            test_case.schema.clone()
        )
        .with_sink(SinkConfig::InMemoryStorageGrpcSinkConfig(format!("http://{}", storage_server_addr)))
        .sql(test_case.sql)
        .with_execution_profile(ExecutionProfile::SingleWorkerNoMaster { num_threads_per_task: 4 })
        .build();

    // Start storage server and execute
    let mut storage_server = InMemoryStorageServer::new();
    storage_server.start(&storage_server_addr).await.unwrap();
    
    context.execute().await.unwrap();
    
    let mut client = InMemoryStorageClient::new(format!("http://{}", storage_server_addr)).await.unwrap();
    let actual_messages = client.get_vector().await.unwrap();
    storage_server.stop().await;

    // Convert expected batch to message for comparison
    let expected_messages = vec![Message::new(None, test_case.expected_batch.clone(), None, None)];

    // Verify results - use unordered comparison for group by operations
    let preserve_order = !matches!(test_case.name, 
        "group_by_count" | "group_by_sum" | "group_by_avg" | "group_by_min_max" | 
        "group_by_having"
    );
    
    verify_message_records_match(&expected_messages, &actual_messages, test_case.name, preserve_order);
    
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

    #[tokio::test]
    async fn test_group_by_sum() -> Result<()> {
        let test_cases = get_test_cases();
        let test_case = test_cases.iter().find(|tc| tc.name == "group_by_sum").unwrap();
        run_sql_test_case(test_case).await
    }

    #[tokio::test]
    async fn test_group_by_avg() -> Result<()> {
        let test_cases = get_test_cases();
        let test_case = test_cases.iter().find(|tc| tc.name == "group_by_avg").unwrap();
        run_sql_test_case(test_case).await
    }

    #[tokio::test]
    async fn test_group_by_min_max() -> Result<()> {
        let test_cases = get_test_cases();
        let test_case = test_cases.iter().find(|tc| tc.name == "group_by_min_max").unwrap();
        run_sql_test_case(test_case).await
    }

    #[tokio::test]
    async fn test_where_and() -> Result<()> {
        let test_cases = get_test_cases();
        let test_case = test_cases.iter().find(|tc| tc.name == "where_and").unwrap();
        run_sql_test_case(test_case).await
    }

    #[tokio::test]
    async fn test_where_or() -> Result<()> {
        let test_cases = get_test_cases();
        let test_case = test_cases.iter().find(|tc| tc.name == "where_or").unwrap();
        run_sql_test_case(test_case).await
    }

    #[tokio::test]
    async fn test_where_in() -> Result<()> {
        let test_cases = get_test_cases();
        let test_case = test_cases.iter().find(|tc| tc.name == "where_in").unwrap();
        run_sql_test_case(test_case).await
    }

    #[tokio::test]
    async fn test_where_between() -> Result<()> {
        let test_cases = get_test_cases();
        let test_case = test_cases.iter().find(|tc| tc.name == "where_between").unwrap();
        run_sql_test_case(test_case).await
    }

    #[tokio::test]
    async fn test_where_like() -> Result<()> {
        let test_cases = get_test_cases();
        let test_case = test_cases.iter().find(|tc| tc.name == "where_like").unwrap();
        run_sql_test_case(test_case).await
    }

    #[tokio::test]
    async fn test_select_computed() -> Result<()> {
        let test_cases = get_test_cases();
        let test_case = test_cases.iter().find(|tc| tc.name == "select_computed").unwrap();
        run_sql_test_case(test_case).await
    }

    #[tokio::test]
    async fn test_select_case_when() -> Result<()> {
        let test_cases = get_test_cases();
        let test_case = test_cases.iter().find(|tc| tc.name == "select_case_when").unwrap();
        run_sql_test_case(test_case).await
    }

    #[tokio::test]
    async fn test_group_by_having() -> Result<()> {
        let test_cases = get_test_cases();
        let test_case = test_cases.iter().find(|tc| tc.name == "group_by_having").unwrap();
        run_sql_test_case(test_case).await
    }

    #[tokio::test]
    async fn test_where_complex() -> Result<()> {
        let test_cases = get_test_cases();
        let test_case = test_cases.iter().find(|tc| tc.name == "where_complex").unwrap();
        run_sql_test_case(test_case).await
    }

    #[tokio::test]
    async fn test_aliases() -> Result<()> {
        let test_cases = get_test_cases();
        let test_case = test_cases.iter().find(|tc| tc.name == "aliases").unwrap();
        run_sql_test_case(test_case).await
    }
}