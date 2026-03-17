use std::sync::Arc;
use datafusion::functions_aggregate::count::count;
use datafusion::functions_aggregate::min_max::max;
use datafusion::functions_aggregate::sum::sum;
use datafusion::prelude::*;
use datafusion::logical_expr::{Expr, LogicalPlan, WindowFrame};
use datafusion::optimizer::{Optimizer, OptimizerContext, OptimizerRule};
use datafusion_optimizer::decorrelate_predicate_subquery::DecorrelatePredicateSubquery;
use datafusion_optimizer::eliminate_duplicated_expr::EliminateDuplicatedExpr;
use datafusion_optimizer::eliminate_group_by_constant::EliminateGroupByConstant;
use datafusion_optimizer::eliminate_join::EliminateJoin;
use datafusion_optimizer::eliminate_limit::EliminateLimit;
use datafusion_optimizer::eliminate_nested_union::EliminateNestedUnion;
use datafusion_optimizer::eliminate_outer_join::EliminateOuterJoin;
use datafusion_optimizer::extract_equijoin_predicate::ExtractEquijoinPredicate;
use datafusion_optimizer::filter_null_join_keys::FilterNullJoinKeys;
use datafusion_optimizer::propagate_empty_relation::PropagateEmptyRelation;
use datafusion_optimizer::push_down_limit::PushDownLimit;
use datafusion_optimizer::replace_distinct_aggregate::ReplaceDistinctWithAggregate;
use datafusion_optimizer::scalar_subquery_to_join::ScalarSubqueryToJoin;
use datafusion_optimizer::{
    eliminate_filter::EliminateFilter,
    optimize_projections::OptimizeProjections,
    simplify_expressions::SimplifyExpressions,
    common_subexpr_eliminate::CommonSubexprEliminate,
    push_down_filter::PushDownFilter
};
use datafusion::common::{Result, DataFusionError};
use datafusion::execution::context::SessionContext;
use arrow::datatypes::{Schema, Field, DataType};
use arrow::array::{Int32Array, Float64Array, StringArray, TimestampSecondArray};
use arrow::record_batch::RecordBatch;

/// Example demonstrating different DataFrame/SQL expressions and their logical plans
pub struct LogicalPlanExamples {
    ctx: SessionContext,
    optimizer: Optimizer,
}

impl LogicalPlanExamples {
    pub fn new() -> Result<Self> {
        let ctx = SessionContext::new();
        let rules: Vec<Arc<dyn OptimizerRule + Send + Sync>> = vec![
            Arc::new(PushDownFilter::new()),
            Arc::new(EliminateFilter::new()),
            Arc::new(OptimizeProjections::new()),
            Arc::new(SimplifyExpressions::new()),
            Arc::new(CommonSubexprEliminate::new()),
        ];

        // let rules: Vec<Arc<dyn OptimizerRule + Send + Sync>> = vec![
        //     Arc::new(EliminateNestedUnion::new()),
        //     Arc::new(SimplifyExpressions::new()),
        //     Arc::new(ReplaceDistinctWithAggregate::new()),
        //     Arc::new(EliminateJoin::new()),
        //     Arc::new(DecorrelatePredicateSubquery::new()),
        //     Arc::new(ScalarSubqueryToJoin::new()),
        //     Arc::new(ExtractEquijoinPredicate::new()),
        //     Arc::new(EliminateDuplicatedExpr::new()),
        //     Arc::new(EliminateFilter::new()),
        //     Arc::new(EliminateOuterJoin::new()),
        //     Arc::new(CommonSubexprEliminate::new()),
        //     Arc::new(EliminateLimit::new()),
        //     Arc::new(PropagateEmptyRelation::new()),
        //     // Must be after PropagateEmptyRelation
        //     Arc::new(EliminateOuterJoin::new()),
        //     Arc::new(FilterNullJoinKeys::default()),
        //     Arc::new(EliminateOuterJoin::new()),
        //     // Filters can't be pushed down past Limits, we should do PushDownFilter after PushDownLimit
        //     Arc::new(PushDownLimit::new()),
        //     Arc::new(PushDownFilter::new()),
        //     // This rule creates nested aggregates off of count(distinct value), which we don't support.
        //     //Arc::new(SingleDistinctToGroupBy::new()),

        //     // The previous optimizations added expressions and projections,
        //     // that might benefit from the following rules
        //     Arc::new(SimplifyExpressions::new()),
        //     Arc::new(CommonSubexprEliminate::new()),
        //     Arc::new(EliminateGroupByConstant::new()),
        //     // This rule can drop event time calculation fields if they aren't used elsewhere.
        //     Arc::new(OptimizeProjections::new()),
        // ];

        // Create optimizer with available rules
        let optimizer = Optimizer::with_rules(rules);

        Ok(Self { ctx, optimizer })
    }

    /// Set up test data
    pub async fn setup_test_data(&self) -> Result<()> {
        // Create test tables
        let orders_schema = Schema::new(vec![
            Field::new("order_id", DataType::Int32, false),
            Field::new("user_id", DataType::Int32, false),
            Field::new("amount", DataType::Float64, false),
            Field::new("product_type", DataType::Utf8, false),
            Field::new("timestamp", DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None), false),
        ]);

        let users_schema = Schema::new(vec![
            Field::new("user_id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("email", DataType::Utf8, false),
            Field::new("registered_at", DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None), false),
        ]);

        // Create test data using Arrow arrays
        let orders_batch = RecordBatch::try_new(
            Arc::new(orders_schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(Int32Array::from(vec![1, 1, 2, 2, 3])),
                Arc::new(Float64Array::from(vec![100.0, 200.0, 150.0, 300.0, 75.0])),
                Arc::new(StringArray::from(vec!["ON_SALE", "REGULAR", "ON_SALE", "REGULAR", "ON_SALE"])),
                Arc::new(TimestampSecondArray::from(vec![1640995200, 1640995200, 1640995200, 1640995200, 1640995200])),
            ],
        )?;

        let users_batch = RecordBatch::try_new(
            Arc::new(users_schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
                Arc::new(StringArray::from(vec!["alice@example.com", "bob@example.com", "charlie@example.com"])),
                Arc::new(TimestampSecondArray::from(vec![1640995200, 1640995200, 1640995200])),
            ],
        )?;

        // Register tables
        self.ctx.register_batch("orders", orders_batch)?;
        self.ctx.register_batch("users", users_batch)?;

        Ok(())
    }

    /// Print logical plan in a readable format
    pub fn print_logical_plan(&self, plan: LogicalPlan, title: &str) {
        println!("\n{}", "=".repeat(80));
        println!("{}", title);
        println!("{}", "=".repeat(80));
        println!("{}", plan.display_indent());
    }

    /// Optimize logical plan and print result
    pub async fn optimize_and_print(&self, plan: LogicalPlan, title: &str) -> Result<()> {
        let optimized = self.optimizer.optimize(
            plan,
            &OptimizerContext::default(),
            |_plan, _rule| {},
        )?;
        self.print_logical_plan(optimized, &format!("{} (OPTIMIZED)", title));
        
        Ok(())
    }

    /// Example 1: Simple projection and filter
    pub async fn example_simple_projection_filter(&self) -> Result<()> {
        println!("\n🔍 Example 1: Simple Projection and Filter");
        
        // DataFrame equivalent: 
        // let df = self.ctx.table("orders").await?
        //     .select(vec![col("user_id"), col("amount")])?
        //     .filter(col("amount").gt(lit(100)))?;
        // let plan = df.logical_plan().clone();
        // let sql = "SELECT user_id, amount FROM orders WHERE amount > 100";
        let sql = "SELECT SUM(amount) FROM orders";
        let plan = self.ctx.sql(sql).await?.logical_plan().clone();
        self.print_logical_plan(plan.clone(), "Original Logical Plan");
        
        self.optimize_and_print(plan, "Optimized Logical Plan").await?;
        
        Ok(())
    }

    /// Example 2: Redundant projections
    pub async fn example_redundant_projections(&self) -> Result<()> {
        println!("\n🔍 Example 2: Redundant Projections");
        
        // DataFrame equivalent: 
        // let df = self.ctx.table("orders").await?
        //     .select(vec![col("user_id"), col("amount")])?
        //     .select(vec![col("user_id"), col("amount")])?;
        let sql = "SELECT user_id, amount FROM (SELECT user_id, amount FROM orders)";
        
        let plan = self.ctx.sql(sql).await?.logical_plan().clone();
        self.print_logical_plan(plan.clone(), "Original Logical Plan");
        
        self.optimize_and_print(plan, "Optimized Logical Plan").await?;
        
        Ok(())
    }

    /// Example 3: Redundant filters
    pub async fn example_redundant_filters(&self) -> Result<()> {
        println!("\n🔍 Example 3: Redundant Filters");
        
        // DataFrame equivalent: 
        // let df = self.ctx.table("orders").await?
        //     .filter(col("amount").gt(lit(50)))?
        //     .filter(col("amount").gt(lit(100)))?;
        let sql = "SELECT * FROM orders WHERE amount > 50 AND amount > 100";
        
        let plan = self.ctx.sql(sql).await?.logical_plan().clone();
        self.print_logical_plan(plan.clone(), "Original Logical Plan");
        
        self.optimize_and_print(plan, "Optimized Logical Plan").await?;
        
        Ok(())
    }

    /// Example 4: Join with projection pushdown
    pub async fn example_join_projection_pushdown(&self) -> Result<()> {
        println!("\n🔍 Example 4: Join with Projection Pushdown");
        
        // DataFrame equivalent: 
        // let orders = self.ctx.table("orders").await?;
        // let users = self.ctx.table("users").await?;
        // let df = orders.join(users, JoinType::Inner, &["user_id"], &["user_id"])?
        //     .select(vec![col("user_id"), col("amount"), col("name")])?
        //     .filter(col("amount").gt(lit(100)))?;
        let sql = "
            SELECT o.user_id, o.amount, u.name 
            FROM orders o 
            JOIN users u ON o.user_id = u.user_id 
            WHERE o.amount > 100
        ";
        
        let plan = self.ctx.sql(sql).await?.logical_plan().clone();
        self.print_logical_plan(plan.clone(), "Original Logical Plan");
        
        self.optimize_and_print(plan, "Optimized Logical Plan").await?;
        
        Ok(())
    }

    /// Example 5: Complex nested query
    pub async fn example_complex_nested_query(&self) -> Result<()> {
        println!("\n🔍 Example 5: Complex Nested Query");
        
        // DataFrame equivalent: 
        // let high_value = self.ctx.table("orders").await?
        //     .filter(col("amount").gt(lit(1000)))?;
        // let user_stats = high_value.aggregate(
        //     vec![col("user_id")],
        //     vec![sum(col("amount")).alias("total_spent")]
        // )?;
        // let result = user_stats.filter(col("total_spent").gt(lit(5000)))?;
        let sql = "
            SELECT user_id, total_spent 
            FROM (
                SELECT user_id, SUM(amount) as total_spent
                FROM orders 
                WHERE amount > 1000
                GROUP BY user_id
            ) user_stats
            WHERE total_spent > 5000
        ";
        
        let plan = self.ctx.sql(sql).await?.logical_plan().clone();
        self.print_logical_plan(plan.clone(), "Original Logical Plan");
        
        self.optimize_and_print(plan, "Optimized Logical Plan").await?;
    
        
        Ok(())
    }

    /// Example 6: Window functions
    pub async fn example_window_functions(&self) -> Result<()> {
        println!("\n🔍 Example 6: Window Functions");
        
        // DataFrame equivalent: 
        // let df = self.ctx.table("orders").await?
        //     .filter(col("amount").gt(lit(50)))?
        //     .select(vec![
        //         col("user_id"),
        //         col("amount"),
        //         sum(col("amount")).over(vec![col("user_id")], vec![col("timestamp")], WindowFrame::rows(Some(4), None))?.alias("sum_1m"),
        //         avg(col("amount")).over(vec![col("user_id")], vec![col("timestamp")], WindowFrame::rows(Some(19), None))?.alias("avg_5m")
        //     ])?;
        let sql = "
            SELECT 
                user_id,
                amount,
                SUM(amount) OVER (PARTITION BY user_id ORDER BY timestamp ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) as sum_1m,
                AVG(amount) OVER (PARTITION BY user_id ORDER BY timestamp ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as avg_5m
            FROM orders
            WHERE amount > 50
        ";
        
        let plan = self.ctx.sql(sql).await?.logical_plan().clone();
        self.print_logical_plan(plan.clone(), "Original Logical Plan");
        
        self.optimize_and_print(plan, "Optimized Logical Plan").await?;
        
        Ok(())
    }

    /// Example 7: Union with optimization
    pub async fn example_union_optimization(&self) -> Result<()> {
        println!("\n🔍 Example 7: Union with Optimization");
        
        // DataFrame equivalent: 
        // let df1 = self.ctx.table("orders").await?
        //     .filter(col("amount").gt(lit(100)))?
        //     .select(vec![col("user_id"), col("amount")])?;
        // let df2 = self.ctx.table("orders").await?
        //     .filter(col("product_type").eq(lit("ON_SALE")))?
        //     .select(vec![col("user_id"), col("amount")])?;
        // let result = df1.union(df2)?;
        let sql = "
            SELECT user_id, amount FROM orders WHERE amount > 100
            UNION ALL
            SELECT user_id, amount FROM orders WHERE product_type = 'ON_SALE'
        ";
        
        let plan = self.ctx.sql(sql).await?.logical_plan().clone();
        self.print_logical_plan(plan.clone(), "Original Logical Plan");
        
        self.optimize_and_print(plan, "Optimized Logical Plan").await?;
        
        Ok(())
    }

    /// Example 8: Expression simplification
    pub async fn example_expression_simplification(&self) -> Result<()> {
        println!("\n🔍 Example 8: Expression Simplification");
        
        // DataFrame equivalent: 
        // let df = self.ctx.table("orders").await?
        //     .filter(
        //         col("amount").gt(lit(50))
        //             .and(col("amount").gt(lit(100)))
        //             .and(lit(1).eq(lit(1)))
        //             .and(col("amount").is_not_null())
        //     )?;
        let sql = "
            SELECT * FROM orders 
            WHERE amount > 50 
            AND amount > 100 
            AND 1 = 1
            AND amount IS NOT NULL
        ";
        
        let plan = self.ctx.sql(sql).await?.logical_plan().clone();
        self.print_logical_plan(plan.clone(), "Original Logical Plan");
        
        self.optimize_and_print(plan, "Optimized Logical Plan").await?;
        
        Ok(())
    }

    /// Example 9: Aggregation optimization
    pub async fn example_aggregation_optimization(&self) -> Result<()> {
        println!("\n🔍 Example 9: Aggregation Optimization");
        
        // DataFrame equivalent: 
        // let df = self.ctx.table("orders").await?
        //     .filter(col("amount").gt(lit(50)))?
        //     .aggregate(
        //         vec![col("user_id")],
        //         vec![
        //             sum(col("amount")).alias("total_spent"),
        //             count(lit(1)).alias("num_orders"),
        //             avg(col("amount")).alias("avg_order_value")
        //         ]
        //     )?
        //     .filter(col("total_spent").gt(lit(200)))?
        //     .sort(vec![col("total_spent").sort(false, false)])?;
        let sql = "
            SELECT 
                user_id,
                SUM(amount) as total_spent,
                COUNT(*) as num_orders,
                AVG(amount) as avg_order_value
            FROM orders 
            WHERE amount > 50
            GROUP BY user_id
            HAVING MIN(amount) > 200
            ORDER BY total_spent DESC
        ";
        
        let plan = self.ctx.sql(sql).await?.logical_plan().clone();
        self.print_logical_plan(plan.clone(), "Original Logical Plan");
        
        self.optimize_and_print(plan, "Optimized Logical Plan").await?;
        
        Ok(())
    }

    /// Example 10: Complex aggregation with window functions
    pub async fn example_complex_aggregation(&self) -> Result<()> {
        println!("\n🔍 Example 10: Complex Aggregation with Window Functions");
        
        // DataFrame equivalent:
        // let df = self.ctx.table("orders").await?
        //     .filter(col("amount").gt(lit(25)))?
        //     .aggregate(
        //         vec![col("user_id"), col("timestamp")],
        //         vec![sum(col("amount")).alias("total_spent")]
        //     )?
        //     .select(vec![
        //         col("user_id"),
        //         col("total_spent"),
        //         sum(col("total_spent")).over(
        //             vec![col("user_id")], 
        //             vec![col("timestamp")], 
        //             WindowFrame::rows(Some(3), None)
        //         )?.alias("rolling_4h_spent")
        //     ])?
        //     .filter(col("total_spent").gt(lit(100)))?;
        // let plan = df.logical_plan();
        let sql = "
            SELECT 
                user_id,
                SUM(amount) as total_spent,
                SUM(SUM(amount)) OVER (
                    PARTITION BY user_id 
                    ORDER BY timestamp 
                    ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
                ) as rolling_4h_spent
            FROM orders 
            WHERE amount > 25
            GROUP BY user_id, timestamp
            HAVING SUM(amount) > 100
        ";
        
        let plan = self.ctx.sql(sql).await?.logical_plan().clone();
        self.print_logical_plan(plan.clone(), "Original Logical Plan");
        
        self.optimize_and_print(plan, "Optimized Logical Plan").await?;
        
        Ok(())
    }

    /// Example 11: Multiple aggregates in single SELECT
    pub async fn example_multiple_aggregates(&self) -> Result<()> {
        println!("\n🔍 Example 11: Multiple Aggregates in Single SELECT");
        
        // DataFrame equivalent:
        // let df = self.ctx.table("orders").await?
        //     .filter(col("amount").gt(lit(50)))?
        //     .aggregate(
        //         vec![col("user_id")],
        //         vec![
        //             sum(col("amount")).alias("total_spent"),
        //             count(col("amount")).alias("num_orders"),
        //             max(col("amount")).alias("max_order"),
        //             // Note: Multiple aggregation functions are better demonstrated in SQL
        //             // DataFrame API has limitations with some aggregation functions
        //         ]
        //     )?
        //     .filter(col("total_spent").gt(lit(200)))?;
        // let plan = df.logical_plan().clone();
        
        let sql = "
            SELECT 
                user_id,
                SUM(amount) as total_spent,
                COUNT(*) as num_orders,
                AVG(amount) as avg_order_value,
                MIN(amount) as min_order,
                MAX(amount) as max_order
            FROM orders 
            WHERE amount > 50
            GROUP BY user_id
            HAVING SUM(amount) > 200
        ";
        
        let plan = self.ctx.sql(sql).await?.logical_plan().clone();
        self.print_logical_plan(plan.clone(), "Original Logical Plan");
        
        self.optimize_and_print(plan, "Optimized Logical Plan").await?;
        
        Ok(())
    }

    /// Example 12: Multiple window aggregations in single SELECT
    pub async fn example_multiple_window_aggregations(&self) -> Result<()> {
        println!("\n🔍 Example 12: Multiple Window Aggregations in Single SELECT");
        
        // DataFrame equivalent:
        // let df = self.ctx.table("orders").await?
        //     .filter(col("amount").gt(lit(25)))?
        //     .select(vec![
        //         col("user_id"),
        //         col("amount"),
        //         col("timestamp")
        //     ])?
        //     .filter(col("amount").gt(lit(100)))?;
        // let plan = df.logical_plan().clone();
        
        let sql = "
            SELECT 
                user_id,
                amount,
                timestamp,
                SUM(amount) OVER (
                    PARTITION BY user_id 
                    ORDER BY timestamp 
                    ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
                ) as rolling_4h_sum,
                AVG(amount) OVER (
                    PARTITION BY user_id 
                    ORDER BY timestamp 
                    ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
                ) as rolling_6h_avg,
                COUNT(*) OVER (
                    PARTITION BY user_id 
                    ORDER BY timestamp 
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                ) as rolling_3h_count,
                MAX(amount) OVER (
                    PARTITION BY user_id 
                    ORDER BY timestamp 
                    ROWS BETWEEN 10 PRECEDING AND CURRENT ROW
                ) as rolling_11h_max
            FROM orders 
            WHERE amount > 25
        ";
        
        let plan = self.ctx.sql(sql).await?.logical_plan().clone();
        self.print_logical_plan(plan.clone(), "Original Logical Plan (SQL)");
        
        self.optimize_and_print(plan, "Optimized Logical Plan").await?;
        
        Ok(())
    }

    /// Example 13: Combining regular aggregate and window aggregate in single SELECT
    pub async fn example_regular_and_window_aggregate(&self) -> Result<()> {
        println!("\n🔍 Example 13: Regular Aggregate and Window Aggregate in Single SELECT");
        
        let sql = "
            SELECT 
                user_id,
                amount,
                timestamp,
                -- Regular aggregate (computed per group)
                SUM(amount) as user_total_spent,
                COUNT(*) as user_order_count,
                -- Window aggregate (computed per row with sliding window)
                SUM(amount) OVER (
                    PARTITION BY user_id 
                    ORDER BY timestamp 
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                ) as rolling_3h_sum,
                AVG(amount) OVER (
                    PARTITION BY user_id 
                    ORDER BY timestamp 
                    ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                ) as rolling_5h_avg
            FROM orders 
            WHERE amount > 50
            GROUP BY user_id, amount, timestamp
        ";
        
        let plan = self.ctx.sql(sql).await?.logical_plan().clone();
        self.print_logical_plan(plan.clone(), "Original Logical Plan");
        
        self.optimize_and_print(plan, "Optimized Logical Plan").await?;
        
        Ok(())
    }

    /// Run all examples
    pub async fn run_all_examples(&self) -> Result<()> {
        // Set up test data
        self.setup_test_data().await?;
        
        // Run examples
        self.example_simple_projection_filter().await?;
        self.example_redundant_projections().await?;
        self.example_redundant_filters().await?;
        self.example_join_projection_pushdown().await?;
        self.example_complex_nested_query().await?;
        self.example_window_functions().await?;
        self.example_union_optimization().await?;
        self.example_expression_simplification().await?;
        self.example_aggregation_optimization().await?;
        self.example_complex_aggregation().await?;
        self.example_multiple_aggregates().await?;
        self.example_multiple_window_aggregations().await?;
        self.example_regular_and_window_aggregate().await?;
        
        Ok(())
    }

    /// Custom example with manual logical plan construction
    pub async fn custom_example(&self) -> Result<()> {
        // Show how to access logical plan from a DataFrame
        let sql = "SELECT user_id, amount FROM orders WHERE amount > 100";
        let df = self.ctx.sql(sql).await?;
        let plan = df.logical_plan().clone();
        
        self.print_logical_plan(plan.clone(), "Logical Plan from DataFrame");
        
        Ok(())
    }
}

/// Helper function to run examples
pub async fn run_logical_plan_examples() -> Result<()> {
    let examples = LogicalPlanExamples::new()?;
    examples.run_all_examples().await?;
    examples.custom_example().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_logical_plan_examples() {
        let examples = LogicalPlanExamples::new().unwrap();
        examples.setup_test_data().await.unwrap();
        
        // Test a simple example
        examples.example_simple_projection_filter().await.unwrap();
        // examples.example_redundant_projections().await.unwrap();
        // examples.example_redundant_filters().await.unwrap();
        // examples.example_join_projection_pushdown().await.unwrap();
        // examples.example_complex_nested_query().await.unwrap();
        // examples.example_window_functions().await.unwrap();
        // examples.example_union_optimization().await.unwrap();
        // examples.example_expression_simplification().await.unwrap();
        // examples.example_aggregation_optimization().await.unwrap();
        // examples.example_complex_aggregation().await.unwrap();
        // examples.example_multiple_aggregates().await.unwrap();
        // examples.example_multiple_window_aggregations().await.unwrap();
        // examples.example_regular_and_window_aggregate().await.unwrap();
    }
} 