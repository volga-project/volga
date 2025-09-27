use std::collections::BTreeMap;
use std::sync::Arc;
use arrow::array::{RecordBatch, UInt64Array};
use datafusion::physical_plan::WindowExpr;
use datafusion::scalar::ScalarValue;

use crate::runtime::operators::window::aggregates::merge_accumulator_state;
use crate::runtime::operators::window::{create_window_aggregator, WindowAggregator};
use crate::runtime::operators::window::state::AccumulatorState;
// use crate::runtime::operators::window::window_operator::create_sliding_accumulator;
use crate::storage::storage::{Timestamp, extract_timestamp};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TimeGranularity {
    Minutes(u32),
    Hours(u32),
    Days(u32),
    Months(u32),
}

impl TimeGranularity {

    pub fn to_millis(&self) -> i64 {
        match self {
            TimeGranularity::Minutes(m) => *m as i64 * 60 * 1000,
            TimeGranularity::Hours(h) => *h as i64 * 60 * 60 * 1000,
            TimeGranularity::Days(d) => *d as i64 * 24 * 60 * 60 * 1000,
            TimeGranularity::Months(m) => *m as i64 * 30 * 24 * 60 * 60 * 1000, // Approximate
        }
    }

    pub fn is_multiple_of(&self, other: &TimeGranularity) -> bool {
        let self_millis = self.to_millis();
        let other_millis = other.to_millis();
        
        self_millis > other_millis && self_millis % other_millis == 0
    }

    pub fn tile_start(&self, timestamp: Timestamp) -> Timestamp {
        let duration_millis = self.to_millis();
        (timestamp / duration_millis) * duration_millis
    }

    pub fn next_tile_start(&self, timestamp: Timestamp) -> Timestamp {
        self.tile_start(timestamp) + self.to_millis()
    }
}

/// A tile represents pre-aggregated data for a specific time bucket
#[derive(Debug, Clone)]
pub struct Tile {
    pub tile_start: Timestamp,
    pub tile_end: Timestamp,
    pub granularity: TimeGranularity,
    pub accumulator_state: Option<AccumulatorState>,
    pub entry_count: u64,
}

impl Tile {
    pub fn new(tile_start: Timestamp, granularity: TimeGranularity) -> Self {
        Self {
            tile_start,
            tile_end: tile_start + granularity.to_millis(),
            granularity,
            accumulator_state: None,
            entry_count: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TileConfig {
    pub granularities: Vec<TimeGranularity>,
}

impl TileConfig {
    pub fn new(mut granularities: Vec<TimeGranularity>) -> Result<Self, String> {
        granularities.sort();
        
        for i in 1..granularities.len() {
            if !granularities[i].is_multiple_of(&granularities[i-1]) {
                return Err(format!(
                    "Granularity {:?} must be a multiple of {:?}",
                    granularities[i], granularities[i-1]
                ));
            }
        }
        
        Ok(Self { granularities })
    }
    
    pub fn default_config() -> Self {
        Self::new(vec![
            TimeGranularity::Minutes(1),
            TimeGranularity::Minutes(5),
            TimeGranularity::Hours(1),
            TimeGranularity::Days(1),
        ]).expect("Default config should be valid")
    }
}

#[derive(Debug, Clone)]
pub struct Tiles {
    config: TileConfig,
    window_expr: Arc<dyn WindowExpr>,
    // Map: granularity -> tile_start -> tile
    tiles: BTreeMap<TimeGranularity, BTreeMap<Timestamp, Tile>>,
}

impl Tiles {
    pub fn new(config: TileConfig, window_expr: Arc<dyn WindowExpr>) -> Self {
        let mut tiles = BTreeMap::new();
        
        for granularity in &config.granularities {
            tiles.insert(*granularity, BTreeMap::new());
        }
        
        Self {
            config,
            window_expr,
            tiles,
        }
    }

    pub fn add_batch(&mut self, batch: &RecordBatch, ts_column_index: usize) {
        // Extract timestamps
        let mut entries = Vec::new();
        
        for row_idx in 0..batch.num_rows() {
            let timestamp = extract_timestamp(batch.column(ts_column_index), row_idx);
            entries.push((timestamp, row_idx));
        }
        
        for (timestamp, row_idx) in entries {
            self.add_entry(batch, row_idx, timestamp);
        }
    }

    fn add_entry(&mut self, batch: &RecordBatch, row_idx: usize, timestamp: Timestamp) {
        let granularities = self.config.granularities.clone();
        
        for granularity in granularities {
            self.add_entry_to_granularity(batch, row_idx, timestamp, granularity);
        }
    }

    fn add_entry_to_granularity(
        &mut self, 
        batch: &RecordBatch, 
        row_idx: usize, 
        timestamp: Timestamp, 
        granularity: TimeGranularity
    ) {
        let tile_start = granularity.tile_start(timestamp);
        
        let tiles_for_granularity = self.tiles.get_mut(&granularity)
            .expect(&format!("No tiles found for granularity {:?}", granularity));
        
        let tile = tiles_for_granularity.entry(tile_start)
            .or_insert_with(|| Tile::new(tile_start, granularity));
        
        Self::update_tile_with_entry(&self.window_expr, tile, batch, row_idx);
    }

    fn update_tile_with_entry(window_expr: &Arc<dyn WindowExpr>, tile: &mut Tile, batch: &RecordBatch, row_idx: usize) {
        
        let indices = UInt64Array::from(vec![row_idx as u64]);
        let entry_batch = arrow::compute::take_record_batch(batch, &indices)
            .expect("Failed to extract entry row");
        
        // let mut accumulator = create_sliding_accumulator(window_expr, tile.accumulator_state.clone());
        
        let mut accumulator = match create_window_aggregator(&window_expr) {
            WindowAggregator::Accumulator(accumulator) => accumulator,
            WindowAggregator::Evaluator(evaluator) => panic!("Evaluator is not supported for retractable accumulator"),
        };

        if let Some(tile_state) = &tile.accumulator_state {
            merge_accumulator_state(accumulator.as_mut(), tile_state.clone());
        }

        let args = window_expr.evaluate_args(&entry_batch)
            .expect("Failed to evaluate window args");
        
        accumulator.update_batch(&args)
            .expect("Failed to update accumulator");
        
        tile.accumulator_state = Some(accumulator.state()
            .expect("Failed to get accumulator state"));
        
        tile.entry_count += 1;
    }

    pub fn get_tiles_for_range(&self, start_time: Timestamp, end_time: Timestamp) -> Vec<Tile> {
        let mut selected_tiles: Vec<Tile> = Vec::new();
        let mut remaining_ranges = vec![(start_time, end_time)];
        
        // Try granularities from largest to smallest (reverse order)
        for &granularity in self.config.granularities.iter().rev() {
            let tiles_for_granularity = match self.tiles.get(&granularity) {
                Some(tiles) => tiles,
                None => continue,
            };
            
            let mut new_remaining_ranges = Vec::new();
            
            for (range_start, range_end) in remaining_ranges {
                // Find all tiles that are fully contained within this range using range query
                let tiles_in_range: Vec<_> = tiles_for_granularity
                    .range(range_start..range_end)
                    .filter(|(&tile_start, tile)| {
                        tile_start >= range_start && tile.tile_end <= range_end
                    })
                    .collect();
                
                for (_, tile) in &tiles_in_range {
                    selected_tiles.push((*tile).clone());
                }
                
                if tiles_in_range.is_empty() {
                    new_remaining_ranges.push((range_start, range_end));
                } else {
                    let mut current_pos = range_start;
                    
                    for (&tile_start, tile) in &tiles_in_range {
                        if current_pos < tile_start {
                            new_remaining_ranges.push((current_pos, tile_start));
                        }
                        
                        current_pos = tile.tile_end;
                    }
                    
                    if current_pos < range_end {
                        new_remaining_ranges.push((current_pos, range_end));
                    }
                }
            }
            
            remaining_ranges = new_remaining_ranges;
            
            if remaining_ranges.is_empty() {
                break;
            }
        }
        
        selected_tiles
    }


    pub fn compute_aggregate_for_range(&self, start_time: Timestamp, end_time: Timestamp) -> ScalarValue {
        let tiles = self.get_tiles_for_range(start_time, end_time);
        
        if tiles.is_empty() {
            return ScalarValue::Null;
        }
        
        let mut final_accumulator = match create_window_aggregator(&self.window_expr) {
            WindowAggregator::Accumulator(accumulator) => accumulator,
            WindowAggregator::Evaluator(_) => panic!("Evaluator is not supported for retractable accumulator"),
        };
        
        for tile in tiles {
            if let Some(tile_state) = &tile.accumulator_state {
                merge_accumulator_state(final_accumulator.as_mut(), tile_state.clone());
            }
        }
        
        final_accumulator.evaluate()
            .expect("Failed to evaluate final accumulator")
    }

    pub fn get_stats(&self) -> TileStats {
        let mut total_tiles = 0;
        let mut total_entries = 0;
        let mut granularity_stats = BTreeMap::new();
        
        for (granularity, tiles_map) in &self.tiles {
            let tile_count = tiles_map.len();
            let entry_count: u64 = tiles_map.values().map(|tile| tile.entry_count).sum();
            
            total_tiles += tile_count;
            total_entries += entry_count;
            
            granularity_stats.insert(*granularity, GranularityStats {
                tile_count,
                entry_count,
            });
        }
        
        TileStats {
            total_tiles,
            total_entries,
            granularity_stats,
        }
    }

    pub fn cleanup_old_tiles(&mut self, cutoff_time: Timestamp) {
        for tiles_map in self.tiles.values_mut() {
            tiles_map.retain(|_, tile| {
                tile.tile_end > cutoff_time
            });
        }
    }
}

#[derive(Debug)]
pub struct TileStats {
    pub total_tiles: usize,
    pub total_entries: u64,
    pub granularity_stats: BTreeMap<TimeGranularity, GranularityStats>,
}

#[derive(Debug)]
pub struct GranularityStats {
    pub tile_count: usize,
    pub entry_count: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use arrow::array::{Float64Array, TimestampMillisecondArray, StringArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;
    use datafusion::physical_plan::windows::BoundedWindowAggExec;
    use crate::api::planner::{Planner, PlanningContext};
    use crate::runtime::operators::source::source_operator::{SourceConfig, VectorSourceConfig};
    use crate::runtime::operators::operator::OperatorConfig;

    fn create_test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Millisecond, None), false),
            Field::new("value", DataType::Float64, false),
            Field::new("partition_key", DataType::Utf8, false),
        ]))
    }

    async fn extract_window_exec_from_sql(sql: &str) -> Arc<BoundedWindowAggExec> {
        let ctx = SessionContext::new();
        let mut planner = Planner::new(PlanningContext::new(ctx));
        let schema = create_test_schema();
        
        planner.register_source(
            "test_table".to_string(), 
            SourceConfig::VectorSourceConfig(VectorSourceConfig::new(vec![])), 
            schema.clone()
        );
        
        let logical_graph = planner.sql_to_graph(sql).await.unwrap();
        let nodes: Vec<_> = logical_graph.get_nodes().collect();
        
        for node in &nodes {
            if let OperatorConfig::WindowConfig(config) = &node.operator_config {
                return config.window_exec.clone();
            }
        }
        
        panic!("No window operator found in SQL: {}", sql);
    }

    fn extract_first_window_expr(window_exec: &Arc<BoundedWindowAggExec>) -> Arc<dyn datafusion::physical_plan::WindowExpr> {
        window_exec.window_expr()[0].clone()
    }

    #[test]
    fn test_tile_config_validation() {
        // Valid config
        let valid_config = TileConfig::new(vec![
            TimeGranularity::Minutes(1),
            TimeGranularity::Minutes(5),
            TimeGranularity::Hours(1),
        ]);
        assert!(valid_config.is_ok());
        
        // Invalid config - 7 minutes is not a multiple of 5 minutes
        let invalid_config = TileConfig::new(vec![
            TimeGranularity::Minutes(5),
            TimeGranularity::Minutes(7),
        ]);
        assert!(invalid_config.is_err());
    }

    #[tokio::test]
    async fn test_tiles() {
        // Use a real SUM window expression from SQL
        let sql = "SELECT 
            timestamp,
            value,
            partition_key,
            SUM(value) OVER (PARTITION BY partition_key ORDER BY timestamp RANGE BETWEEN INTERVAL '5000' MILLISECOND PRECEDING AND CURRENT ROW) as sum_val
        FROM test_table";
        
        let window_exec = extract_window_exec_from_sql(sql).await;
        let window_expr = extract_first_window_expr(&window_exec);

        // Create tiles with multiple granularities
        let config = TileConfig::new(vec![
            TimeGranularity::Minutes(1),
            TimeGranularity::Minutes(5),
            TimeGranularity::Hours(1),
        ]).expect("Failed to create tile config");
        
        let mut tiles = Tiles::new(config, window_expr);

        // Create test data spanning multiple hours
        let base_time = 1625097600000; // 2021-07-01 00:00:00 UTC
        
        // First, create sorted data for readability
        let sorted_data = vec![
            (base_time + 0 * 60 * 1000, 10.0),   // 00:00:00, value=10
            (base_time + 2 * 60 * 1000, 20.0),   // 00:02:00, value=20
            (base_time + 5 * 60 * 1000, 25.0),   // 00:05:00, value=25
            (base_time + 7 * 60 * 1000, 35.0),   // 00:07:00, value=35
            (base_time + 8 * 60 * 1000, 40.0),   // 00:08:00, value=40
            (base_time + 12 * 60 * 1000, 60.0),  // 00:12:00, value=60
            (base_time + 15 * 60 * 1000, 75.0),  // 00:15:00, value=75
            (base_time + 18 * 60 * 1000, 90.0),  // 00:18:00, value=90
            (base_time + 22 * 60 * 1000, 110.0), // 00:22:00, value=110
            (base_time + 25 * 60 * 1000, 125.0), // 00:25:00, value=125
        ];
        
        // Now unsort the data to test that tile updates work independently of order
        let unsorted_indices = vec![3, 1, 6, 0, 8, 2, 7, 5, 9, 4]; // Mix up the order
        let unsorted_data: Vec<(i64, f64)> = unsorted_indices.iter()
            .map(|&i| sorted_data[i])
            .collect();

        let timestamps: Vec<i64> = unsorted_data.iter().map(|(t, _)| *t).collect();
        let values: Vec<f64> = unsorted_data.iter().map(|(_, v)| *v).collect();
        
        let batch = create_test_batch(timestamps, values);
        
        // Add batch to tiles - should work with unsorted data
        tiles.add_batch(&batch, 0);

        // Validate tiles were created at all granularities
        let stats = tiles.get_stats();
        println!("Stats after adding unsorted data: {:?}", stats);
        
        assert_eq!(stats.total_entries, 10 * 3); // 10 entries * 3 granularities
        assert_eq!(stats.granularity_stats.len(), 3); // All 3 granularities should have tiles
        
        // Verify each granularity has tiles
        for (_, granularity_stat) in &stats.granularity_stats {
            assert!(granularity_stat.tile_count > 0);
            assert!(granularity_stat.entry_count > 0);
        }

        // Test range queries that should return mixed granularities
        
        // Query 1: Small range (should use fine granularity tiles)
        let small_range_start = base_time + 2 * 60 * 1000;  // 00:02:00
        let small_range_end = base_time + 8 * 60 * 1000;    // 00:08:00
        
        let small_range_tiles = tiles.get_tiles_for_range(small_range_start, small_range_end);
        println!("  Small range: {:?}", small_range_tiles);
        
        // Query 2: Medium range (should mix granularities)  
        let medium_range_start = base_time + 5 * 60 * 1000;  // 00:05:00
        let medium_range_end = base_time + 18 * 60 * 1000;   // 00:18:00
        
        let medium_range_tiles = tiles.get_tiles_for_range(medium_range_start, medium_range_end);
        println!("  Medium range: {:?}", medium_range_tiles);

        // Query 3: Large range (should prefer coarse granularity tiles)
        let large_range_start = base_time;                    // 00:00:00  
        let large_range_end = base_time + 30 * 60 * 1000;    // 00:30:00
        
        let large_range_tiles = tiles.get_tiles_for_range(large_range_start, large_range_end);
        println!("  Large range: {:?}", large_range_tiles);

        // Verify exact tile selection for each range
        
        // Small range: 00:02:00 to 00:08:00 (6 minutes)
        // Should select fine-grained tiles (1-minute granularity) that are fully contained
        // Expected tiles: [00:02:00-00:03:00], [00:05:00-00:06:00], [00:07:00-00:08:00]
        assert_eq!(small_range_tiles.len(), 3, "Small range should select exactly 3 tiles");
        
        let mut small_tile_starts: Vec<i64> = small_range_tiles.iter()
            .map(|tile| tile.tile_start)
            .collect();
        small_tile_starts.sort();
        
        let expected_small_starts = vec![
            base_time + 2 * 60 * 1000,  // 00:02:00
            base_time + 5 * 60 * 1000,  // 00:05:00  
            base_time + 7 * 60 * 1000,  // 00:07:00
        ];
        assert_eq!(small_tile_starts, expected_small_starts, "Small range tile starts mismatch");
        
        // Verify all selected tiles are 1-minute granularity (finest)
        for tile in &small_range_tiles {
            assert_eq!(tile.granularity, TimeGranularity::Minutes(1), "Small range should use 1-minute tiles");
        }
        
        // Medium range: 00:05:00 to 00:18:00 (13 minutes)  
        // Should select a mix of granularities, preferring coarser when possible
        // Expected: 2 five-minute tile [00:05:00-00:10:00], [00:10:00-00:15:00] + 1 one-minute tiles [00:15:00-00:16:00]
        // Note: 5-minute tile [00:15:00-00:20:00] extends beyond range, so use 1-minute tile instead
        assert_eq!(medium_range_tiles.len(), 3, "Medium range should select exactly 3 tiles (2x5min + 1x1min)");
        
        let mut medium_tile_starts: Vec<i64> = medium_range_tiles.iter()
            .map(|tile| tile.tile_start)
            .collect();
        medium_tile_starts.sort();
        
        let expected_medium_starts = vec![
            base_time + 5 * 60 * 1000,   // 00:05:00 (5-minute tile covering 00:05:00-00:10:00)
            base_time + 10 * 60 * 1000,  // 00:10:00 (5-minute tile covering 00:10:00-00:15:00)
            base_time + 15 * 60 * 1000,  // 00:18:00 (1-minute tile covering 00:15:00-00:16:00)
        ];
        assert_eq!(medium_tile_starts, expected_medium_starts, "Medium range tile starts mismatch");
        
        // Verify mixed granularities: 1 five-minute tile + 2 one-minute tiles
        let five_min_tiles: Vec<_> = medium_range_tiles.iter()
            .filter(|tile| tile.granularity == TimeGranularity::Minutes(5))
            .collect();
        let one_min_tiles: Vec<_> = medium_range_tiles.iter()
            .filter(|tile| tile.granularity == TimeGranularity::Minutes(1))
            .collect();
            
        assert_eq!(five_min_tiles.len(), 2, "Should have exactly 2 five-minute tiles");
        assert_eq!(one_min_tiles.len(), 1, "Should have exactly 1 one-minute tile");
        
        // Large range: 00:00:00 to 00:30:00 (30 minutes)
        // Should prefer coarsest granularity (1-hour) where possible
        // Since we only have 30 minutes of data, should use 5-minute
        assert!(large_range_tiles.len() == 6, "Large range should select exactly 6 tiles");
        
        let mut large_tile_starts: Vec<i64> = large_range_tiles.iter()
            .map(|tile| tile.tile_start)
            .collect();
        large_tile_starts.sort();
        
        // Verify tiles cover all data points
        let expected_large_tile_starts = vec![
            base_time + 0 * 60 * 1000,   // 00:00:00
            base_time + 5 * 60 * 1000,   // 00:05:00
            base_time + 10 * 60 * 1000,  // 00:10:00
            base_time + 15 * 60 * 1000,  // 00:15:00
            base_time + 20 * 60 * 1000,  // 00:20:00
            base_time + 25 * 60 * 1000,  // 00:25:00
        ];

        assert_eq!(large_tile_starts, expected_large_tile_starts, "Large range tile starts mismatch");
        // Verify all selected tiles are 1-minute granularity (finest)
        for tile in &large_range_tiles {
            assert_eq!(tile.granularity, TimeGranularity::Minutes(5), "Large range should use 5-minute tiles");
        }

        println!("  Medium range: {:?}", medium_range_tiles); 
        println!("  Large range: {:?}", large_range_tiles);

        // Calculate expected results manually for verification
        // Window: RANGE BETWEEN INTERVAL '5000' MILLISECOND PRECEDING AND CURRENT ROW
        // This means for each timestamp, we sum values from (timestamp - 5000) to timestamp (inclusive)
        
        // Small range: 00:02:00 to 00:08:00 (exclusive)
        // Data points in range: (00:02:00, 20), (00:05:00, 25), (00:07:00, 35)
        // Expected SUM: 20 + 25 + 35 = 80
        let expected_small_sum = 20.0 + 25.0 + 35.0;
        
        // Medium range: 00:05:00 to 00:18:00 (exclusive)  
        // Data points in range: (00:05:00, 25), (00:07:00, 35), (00:08:00, 40), (00:12:00, 60), (00:15:00, 75)
        // Expected SUM: 25 + 35 + 40 + 60 + 75 = 235
        let expected_medium_sum = 25.0 + 35.0 + 40.0 + 60.0 + 75.0;
        
        // Large range: 00:00:00 to 00:30:00 (exclusive)
        // All data points: (00:00:00, 10), (00:02:00, 20), (00:05:00, 25), (00:07:00, 35), (00:08:00, 40), 
        //                  (00:12:00, 60), (00:15:00, 75), (00:18:00, 90), (00:22:00, 110), (00:25:00, 125)
        // Expected SUM: 10 + 20 + 25 + 35 + 40 + 60 + 75 + 90 + 110 + 125 = 590
        let expected_large_sum = 10.0 + 20.0 + 25.0 + 35.0 + 40.0 + 60.0 + 75.0 + 90.0 + 110.0 + 125.0;
        
        // Compute actual results
        let small_result = tiles.compute_aggregate_for_range(small_range_start, small_range_end);
        let medium_result = tiles.compute_aggregate_for_range(medium_range_start, medium_range_end);
        let large_result = tiles.compute_aggregate_for_range(large_range_start, large_range_end);
        
        println!("Expected vs Actual results:");
        println!("  Small range: expected={}, actual={:?}", expected_small_sum, small_result);
        println!("  Medium range: expected={}, actual={:?}", expected_medium_sum, medium_result);
        println!("  Large range: expected={}, actual={:?}", expected_large_sum, large_result);
        
        // Verify exact results
        match small_result {
            ScalarValue::Float64(Some(actual)) => {
                assert_eq!(actual, expected_small_sum, "Small range SUM mismatch");
            }
            _ => panic!("Small range result should be Float64, got: {:?}", small_result),
        }
        
        match medium_result {
            ScalarValue::Float64(Some(actual)) => {
                assert_eq!(actual, expected_medium_sum, "Medium range SUM mismatch");
            }
            _ => panic!("Medium range result should be Float64, got: {:?}", medium_result),
        }
        
        match large_result {
            ScalarValue::Float64(Some(actual)) => {
                assert_eq!(actual, expected_large_sum, "Large range SUM mismatch");
            }
            _ => panic!("Large range result should be Float64, got: {:?}", large_result),
        }
    }

    #[tokio::test]
    async fn test_agg_correctness() {
        // Create a SQL query with multiple aggregates to test consistency
        let sql = "SELECT 
            timestamp,
            value,
            partition_key,
            SUM(value) OVER (PARTITION BY partition_key ORDER BY timestamp RANGE BETWEEN INTERVAL '10000' MILLISECOND PRECEDING AND CURRENT ROW) as sum_val,
            COUNT(value) OVER (PARTITION BY partition_key ORDER BY timestamp RANGE BETWEEN INTERVAL '10000' MILLISECOND PRECEDING AND CURRENT ROW) as count_val,
            AVG(value) OVER (PARTITION BY partition_key ORDER BY timestamp RANGE BETWEEN INTERVAL '10000' MILLISECOND PRECEDING AND CURRENT ROW) as avg_val,
            MIN(value) OVER (PARTITION BY partition_key ORDER BY timestamp RANGE BETWEEN INTERVAL '10000' MILLISECOND PRECEDING AND CURRENT ROW) as min_val,
            MAX(value) OVER (PARTITION BY partition_key ORDER BY timestamp RANGE BETWEEN INTERVAL '10000' MILLISECOND PRECEDING AND CURRENT ROW) as max_val
        FROM test_table";
        
        let window_exec = extract_window_exec_from_sql(sql).await;
        let window_exprs = window_exec.window_expr();
        
        // Extract individual window expressions for each aggregate
        let sum_expr = window_exprs[0].clone();
        let count_expr = window_exprs[1].clone(); 
        let avg_expr = window_exprs[2].clone();
        let min_expr = window_exprs[3].clone();
        let max_expr = window_exprs[4].clone();
        
        // Create tiles with explicit config for each aggregate
        let config = TileConfig::new(vec![
            TimeGranularity::Minutes(1),
            TimeGranularity::Minutes(5),
            TimeGranularity::Hours(1),
        ]).expect("Failed to create tile config");
        
        let mut sum_tiles = Tiles::new(config.clone(), sum_expr);
        let mut count_tiles = Tiles::new(config.clone(), count_expr);
        let mut avg_tiles = Tiles::new(config.clone(), avg_expr);
        let mut min_tiles = Tiles::new(config.clone(), min_expr);
        let mut max_tiles = Tiles::new(config, max_expr);
        
        // Create test data with corner cases: gaps, duplicates, unsorted
        // Use realistic time scale: minutes instead of seconds, with hour-long gaps
        let base_time = 1625097600000; // 2021-07-01 00:00:00 UTC
        
        let test_data = vec![
            // Normal progression (every few minutes)
            (base_time + 0 * 60 * 1000, 100.0),     // 00:00:00, value=100
            (base_time + 3 * 60 * 1000, 200.0),     // 00:03:00, value=200
            (base_time + 7 * 60 * 1000, 300.0),     // 00:07:00, value=300
            
            // Duplicate timestamps (same minute, different values)
            (base_time + 12 * 60 * 1000, 150.0),    // 00:12:00, value=150
            (base_time + 12 * 60 * 1000, 250.0),    // 00:12:00, value=250 (duplicate timestamp)
            
            // Large gap (1 hour = size of largest tile granularity)
            (base_time + 75 * 60 * 1000, 400.0),    // 01:15:00, value=400 (1+ hour gap)
            
            // More normal data
            (base_time + 78 * 60 * 1000, 500.0),    // 01:18:00, value=500
            (base_time + 82 * 60 * 1000, 600.0),    // 01:22:00, value=600
            
            // Another duplicate timestamp
            (base_time + 90 * 60 * 1000, 350.0),    // 01:30:00, value=350
            (base_time + 90 * 60 * 1000, 450.0),    // 01:30:00, value=450 (duplicate timestamp)
            
            // Final data point
            (base_time + 95 * 60 * 1000, 700.0),    // 01:35:00, value=700
        ];
        
        // Unsort the data to test order independence
        let unsorted_indices = vec![6, 2, 9, 0, 4, 8, 1, 10, 3, 7, 5]; // Mix up the order
        let unsorted_data: Vec<(i64, f64)> = unsorted_indices.iter()
            .map(|&i| test_data[i])
            .collect();
        
        let timestamps: Vec<i64> = unsorted_data.iter().map(|(t, _)| *t).collect();
        let values: Vec<f64> = unsorted_data.iter().map(|(_, v)| *v).collect();
        let batch = create_test_batch(timestamps, values);
        
        // Add the same batch to all tile sets
        sum_tiles.add_batch(&batch, 0);
        count_tiles.add_batch(&batch, 0);
        avg_tiles.add_batch(&batch, 0);
        min_tiles.add_batch(&batch, 0);
        max_tiles.add_batch(&batch, 0);
        
        // Test multiple time ranges with different characteristics
        let test_ranges = vec![
            // Range 1: Early data with duplicates (00:00:00 to 00:15:00)
            (base_time, base_time + 15 * 60 * 1000, "early_with_duplicates"),
            
            // Range 2: Across gap (00:10:00 to 01:20:00) - spans the 1-hour gap
            (base_time + 10 * 60 * 1000, base_time + 80 * 60 * 1000, "across_gap"),
            
            // Range 3: Recent data with duplicates (01:20:00 to 01:40:00)
            (base_time + 80 * 60 * 1000, base_time + 100 * 60 * 1000, "recent_with_duplicates"),
            
            // Range 4: Full range (00:00:00 to 02:00:00)
            (base_time, base_time + 120 * 60 * 1000, "full_range"),
        ];
        
        for (range_start, range_end, range_name) in test_ranges {
            
            // Get results from each tile set
            let sum_result = sum_tiles.compute_aggregate_for_range(range_start, range_end);
            let count_result = count_tiles.compute_aggregate_for_range(range_start, range_end);
            let avg_result = avg_tiles.compute_aggregate_for_range(range_start, range_end);
            let min_result = min_tiles.compute_aggregate_for_range(range_start, range_end);
            let max_result = max_tiles.compute_aggregate_for_range(range_start, range_end);
            
            // Calculate expected values manually
            // Window: 10 seconds preceding + current row
            let data_in_range: Vec<f64> = test_data.iter()
                .filter(|(timestamp, _)| *timestamp >= range_start && *timestamp < range_end)
                .map(|(_, value)| *value)
                .collect();
            
            let expected_sum: f64 = data_in_range.iter().sum();
            let expected_count = data_in_range.len() as i64;
            let expected_avg = if expected_count > 0 { expected_sum / (expected_count as f64) } else { 0.0 };
            let expected_min = if !data_in_range.is_empty() { 
                data_in_range.iter().fold(f64::INFINITY, |a, &b| a.min(b))
            } else { 
                0.0 
            };
            let expected_max = if !data_in_range.is_empty() { 
                data_in_range.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b))
            } else { 
                0.0 
            };
            
            // Verify SUM
            match sum_result {
                ScalarValue::Float64(Some(actual_sum)) => {
                    assert_eq!(actual_sum, expected_sum, 
                        "SUM mismatch for range {}: expected {}, got {}", range_name, expected_sum, actual_sum);
                }
                _ => panic!("SUM result should be Float64 for range {}, got: {:?}", range_name, sum_result),
            }
            
            // Verify COUNT
            match count_result {
                ScalarValue::Int64(Some(actual_count)) => {
                    assert_eq!(actual_count, expected_count,
                        "COUNT mismatch for range {}: expected {}, got {}", range_name, expected_count, actual_count);
                }
                _ => panic!("COUNT result should be Int64 for range {}, got: {:?}", range_name, count_result),
            }
            
            // Verify AVG
            match avg_result {
                ScalarValue::Float64(Some(actual_avg)) => {
                    let tolerance = 1e-10; // Allow for floating point precision
                    assert!((actual_avg - expected_avg).abs() < tolerance,
                        "AVG mismatch for range {}: expected {}, got {}, diff: {}", 
                        range_name, expected_avg, actual_avg, (actual_avg - expected_avg).abs());
                }
                _ => panic!("AVG result should be Float64 for range {}, got: {:?}", range_name, avg_result),
            }
            
            // Verify MIN
            match min_result {
                ScalarValue::Float64(Some(actual_min)) => {
                    assert_eq!(actual_min, expected_min,
                        "MIN mismatch for range {}: expected {}, got {}", range_name, expected_min, actual_min);
                }
                _ => panic!("MIN result should be Float64 for range {}, got: {:?}", range_name, min_result),
            }
            
            // Verify MAX
            match max_result {
                ScalarValue::Float64(Some(actual_max)) => {
                    assert_eq!(actual_max, expected_max,
                        "MAX mismatch for range {}: expected {}, got {}", range_name, expected_max, actual_max);
                }
                _ => panic!("MAX result should be Float64 for range {}, got: {:?}", range_name, max_result),
            }
        }
        
        // Test edge case: empty range
        let empty_range_start = base_time + 150 * 60 * 1000;  // Beyond all data (02:30:00)
        let empty_range_end = base_time + 180 * 60 * 1000;    // (03:00:00)
        
        let empty_sum = sum_tiles.compute_aggregate_for_range(empty_range_start, empty_range_end);
        let empty_count = count_tiles.compute_aggregate_for_range(empty_range_start, empty_range_end);
        let empty_avg = avg_tiles.compute_aggregate_for_range(empty_range_start, empty_range_end);
        let empty_min = min_tiles.compute_aggregate_for_range(empty_range_start, empty_range_end);
        let empty_max = max_tiles.compute_aggregate_for_range(empty_range_start, empty_range_end);
        
        // Verify empty range results
        match empty_sum {
            ScalarValue::Float64(Some(0.0)) | ScalarValue::Null => {
                // Both are acceptable for empty ranges
            }
            _ => panic!("Empty range SUM should be 0.0 or Null, got: {:?}", empty_sum),
        }
        
        match empty_count {
            ScalarValue::Int64(Some(0)) | ScalarValue::Null => {
                // Both are acceptable for empty ranges
            }
            _ => panic!("Empty range COUNT should be 0 or Null, got: {:?}", empty_count),
        }
        
        match empty_avg {
            ScalarValue::Float64(Some(_)) | ScalarValue::Null => {
                // Both are acceptable for empty ranges (AVG could be null or some default value)
            }
            _ => panic!("Empty range AVG should be Float64 or Null, got: {:?}", empty_avg),
        }
        
        match empty_min {
            ScalarValue::Float64(Some(_)) | ScalarValue::Null => {
                // Both are acceptable for empty ranges (MIN could be null or some default value)
            }
            _ => panic!("Empty range MIN should be Float64 or Null, got: {:?}", empty_min),
        }
        
        match empty_max {
            ScalarValue::Float64(Some(_)) | ScalarValue::Null => {
                // Both are acceptable for empty ranges (MAX could be null or some default value)
            }
            _ => panic!("Empty range MAX should be Float64 or Null, got: {:?}", empty_max),
        }
    }

    // Helper function to create test batches
    fn create_test_batch(timestamps: Vec<i64>, values: Vec<f64>) -> RecordBatch {
        let schema = create_test_schema();
        let partition_keys = vec!["test"; timestamps.len()];

        let timestamp_array = Arc::new(TimestampMillisecondArray::from(timestamps));
        let value_array = Arc::new(Float64Array::from(values));
        let partition_array = Arc::new(StringArray::from(partition_keys));

        RecordBatch::try_new(schema, vec![timestamp_array, value_array, partition_array])
            .expect("Failed to create test batch")
    }

}
