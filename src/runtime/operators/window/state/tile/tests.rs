use super::*;

use datafusion::common::ScalarValue;
use datafusion::physical_plan::WindowExpr;
use std::sync::Arc;

use crate::api::planner::{Planner, PlanningContext};
use crate::common::Key;
use crate::runtime::functions::key_by::key_by_function::extract_datafusion_window_exec;
use crate::runtime::operators::source::source_operator::{SourceConfig, VectorSourceConfig};
use crate::runtime::operators::window::aggregates::merge_accumulator_state;
use crate::runtime::operators::window::create_window_aggregator;
use crate::runtime::operators::window::state::tile::update::{
    apply_batch_to_tiles, plan_update_runs_for_batch,
};
use crate::runtime::operators::window::state::tile::{merge_tile_runs, project_tiles};
use crate::runtime::operators::window::store::keys::StateNamespace;
use crate::runtime::operators::window::store::TileStore;
use crate::storage::{WriteBatch, InMemSortedKV, SortedKV};
use arrow::array::{Float64Array, StringArray, TimestampMillisecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::windows::BoundedWindowAggExec;
use datafusion::prelude::SessionContext;
use std::collections::BTreeMap;

fn merge_tile_states(window_expr: &Arc<dyn WindowExpr>, tiles: &[Tile]) -> ScalarValue {
    if tiles.is_empty() {
        return ScalarValue::Null;
    }
    let mut acc = create_window_aggregator(window_expr);
    for tile in tiles {
        if let Some(state) = &tile.state.accumulator_state {
            merge_accumulator_state(acc.as_mut(), state.as_ref());
        }
    }
    acc.evaluate().expect("evaluate tile merge")
}

async fn ingest_tiles(
    store: &TileStore,
    key: &Key,
    window_id: usize,
    config: &TileConfig,
    window_expr: &Arc<dyn WindowExpr>,
    batch: &RecordBatch,
    ts_column_index: usize,
) {
    let runs = plan_update_runs_for_batch(config, batch, ts_column_index);
    let futs: Vec<_> = runs
        .iter()
        .map(|r| {
            let gran = r.granularity;
            let start = r.start_ts;
            async move {
                let wt = store.get(key, gran, start).await.unwrap();
                ((gran, start), wt)
            }
        })
        .collect();
    let mut by_key: BTreeMap<_, _> = futures::future::join_all(futs).await.into_iter().collect();
    apply_batch_to_tiles(
        &mut by_key,
        window_id,
        config,
        window_expr,
        batch,
        ts_column_index,
    );
    let mut wb = WriteBatch::new();
    store.append_window_tiles(&mut wb, key, &by_key).unwrap();
    store.write(wb).await.unwrap();
}

async fn load_tiles_for_plan(
    store: &TileStore,
    key: &Key,
    window_id: usize,
    runs: &[TileScanRun],
) -> Vec<Tile> {
    let mut by_key = BTreeMap::new();
    for run in merge_tile_runs(runs.to_vec()) {
        for (tile_start, wt) in store
            .scan(key, run.granularity, run.start_ts, run.end_ts_exclusive)
            .await
            .unwrap()
        {
            by_key.insert((run.granularity, tile_start), wt);
        }
    }
    project_tiles(&by_key, window_id)
}

fn create_test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("value", DataType::Float64, false),
        Field::new("partition_key", DataType::Utf8, false),
    ]))
}

fn test_key(partition: &str) -> Key {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "partition",
        DataType::Utf8,
        false,
    )]));
    let arr = StringArray::from(vec![partition]);
    let key_batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).expect("key batch");
    Key::new(key_batch).expect("key")
}

async fn extract_window_exec_from_sql(sql: &str) -> Arc<BoundedWindowAggExec> {
    let ctx = SessionContext::new();
    let mut planner = Planner::new(PlanningContext::new(ctx));
    let schema = create_test_schema();
    planner.register_source(
        "test_table".to_string(),
        SourceConfig::VectorSourceConfig(VectorSourceConfig::new(vec![])),
        schema.clone(),
    );
    extract_datafusion_window_exec(sql, &mut planner).await
}

fn create_test_batch(timestamps: Vec<i64>, values: Vec<f64>) -> RecordBatch {
    let schema = create_test_schema();
    let partition_keys = vec!["test"; timestamps.len()];
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampMillisecondArray::from(timestamps)),
            Arc::new(Float64Array::from(values)),
            Arc::new(StringArray::from(partition_keys)),
        ],
    )
    .expect("batch")
}

#[test]
fn test_tile_config_validation() {
    assert!(TileConfig::new(vec![
        TimeGranularity::Minutes(1),
        TimeGranularity::Minutes(5),
        TimeGranularity::Hours(1),
    ])
    .is_ok());
    assert!(TileConfig::new(vec![
        TimeGranularity::Minutes(5),
        TimeGranularity::Minutes(7),
    ])
    .is_err());
}

#[tokio::test]
async fn test_tile_store_apply_and_plan_load() {
    let sql = "SELECT timestamp, value, partition_key,
        SUM(value) OVER (PARTITION BY partition_key ORDER BY timestamp
            RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW) as sum_val
        FROM test_table";
    let window_expr = extract_window_exec_from_sql(sql).await.window_expr()[0].clone();
    let config = TileConfig::new(vec![
        TimeGranularity::Minutes(1),
        TimeGranularity::Minutes(5),
    ])
    .unwrap();

    let store = TileStore::new(
        Arc::new(InMemSortedKV::new()) as Arc<dyn SortedKV>,
        StateNamespace::new(b"test"),
    );
    let key = test_key("k1");
    let base = 1_625_097_600_000i64;

    ingest_tiles(
        &store,
        &key,
        0,
        &config,
        &window_expr,
        &create_test_batch(
            vec![
                base,
                base + 2 * 60_000,
                base + 5 * 60_000,
                base + 7 * 60_000,
                base + 12 * 60_000,
                base + 15 * 60_000,
            ],
            vec![10.0, 20.0, 25.0, 35.0, 60.0, 75.0],
        ),
        0,
    )
    .await;

    // Geometry plan for [5m, 18m): 5m@5, 5m@10, 1m@15
    let plan = plan_time_range(&config, base + 5 * 60_000, base + 18 * 60_000);
    assert_eq!(plan.tile_runs.len(), 2);
    assert_eq!(plan.tile_runs[0].granularity, TimeGranularity::Minutes(5));
    assert_eq!(plan.tile_runs[1].granularity, TimeGranularity::Minutes(1));

    let loaded = load_tiles_for_plan(&store, &key, 0, &plan.tile_runs).await;
    // Sparse: 5m@5 and 5m@10 and 1m@15 exist; empty tiles in runs are absent.
    assert!(loaded.iter().any(|t| t.tile_start == base + 5 * 60_000));
    assert!(loaded.iter().any(|t| t.tile_start == base + 10 * 60_000));
    assert!(loaded.iter().any(|t| t.tile_start == base + 15 * 60_000));

    assert_eq!(
        merge_tile_states(&window_expr, &loaded),
        ScalarValue::Float64(Some(25.0 + 35.0 + 60.0 + 75.0))
    );
}

#[tokio::test]
async fn test_tile_store_merges_same_tile_across_applies() {
    let sql = "SELECT timestamp, value, partition_key,
        SUM(value) OVER (PARTITION BY partition_key ORDER BY timestamp
            RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW) as sum_val
        FROM test_table";
    let window_expr = extract_window_exec_from_sql(sql).await.window_expr()[0].clone();
    let config = TileConfig::new(vec![TimeGranularity::Minutes(1)]).unwrap();
    let store = TileStore::new(
        Arc::new(InMemSortedKV::new()) as Arc<dyn SortedKV>,
        StateNamespace::new(b"test"),
    );
    let key = test_key("k1");
    let base = 1_625_097_600_000i64;

    ingest_tiles(
        &store,
        &key,
        0,
        &config,
        &window_expr,
        &create_test_batch(vec![base, base + 60_000], vec![10.0, 20.0]),
        0,
    )
    .await;
    ingest_tiles(
        &store,
        &key,
        0,
        &config,
        &window_expr,
        &create_test_batch(vec![base + 60_000], vec![5.0]),
        0,
    )
    .await;

    let plan = plan_time_range(&config, base, base + 2 * 60_000);
    let loaded = load_tiles_for_plan(&store, &key, 0, &plan.tile_runs).await;
    let one_min = loaded
        .iter()
        .find(|t| t.tile_start == base + 60_000)
        .unwrap();
    assert_eq!(one_min.state.entry_count, 2);
    assert_eq!(
        merge_tile_states(&window_expr, &loaded),
        ScalarValue::Float64(Some(35.0))
    );
}
