use std::collections::HashMap;
use std::fmt;

use arrow::array::RecordBatch;
use datafusion::scalar::ScalarValue;
use futures::future;
use indexmap::IndexMap;

use crate::common::Key;
use crate::runtime::operators::window::aggregates::{Aggregation, BucketRange};
use crate::runtime::operators::window::aggregates::plain::PlainAggregation;
use crate::runtime::operators::window::aggregates::retractable::RetractableAggregation;
use crate::runtime::operators::window::shared::stack_concat_results;
use crate::storage::read::plan::RangesLoadPlan;
use crate::runtime::operators::window::state::window_logic;
use crate::runtime::operators::window::{AggregatorType, Cursor};
use crate::storage::index::{SortedRangeIndex, SortedRangeView};

use super::{ExecutionMode, WindowOperator};

#[derive(Debug)]
enum AdvancePlanOutcome {
    Noop { still_pending: bool },
    Execute(AdvancePlan),
}

struct WindowAggWorkItem {
    window_id: super::WindowId,
    aggregation: Box<dyn Aggregation>,
}

impl fmt::Debug for WindowAggWorkItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WindowAggWorkItem")
            .field("window_id", &self.window_id)
            .field("aggregator_type", &self.aggregation.aggregator_type())
            .finish()
    }
}

#[derive(Debug)]
struct AggregationPlan {
    window_agg_work_items: Vec<WindowAggWorkItem>,
    load_plans: Vec<RangesLoadPlan>, // aligned 1:1 with window_agg_work_items
    processed_pos_by_window: HashMap<super::WindowId, Cursor>,
    entry_delta: Option<EntryDeltaPlan>,
}

#[derive(Debug, Clone, Copy)]
struct EntryDeltaPlan {
    prev: Option<Cursor>,
    entry_range: BucketRange,
}

#[derive(Debug)]
struct AdvancePlan {
    aggregation_plan: AggregationPlan,
    max_pos_seen: Cursor,
    prev_processed_pos: Cursor,
}

#[derive(Debug)]
struct LoadedRanges {
    views_by_item: Vec<Vec<SortedRangeView>>, // aligned 1:1 with plan.window_agg_work_items / load_plans
}

#[derive(Debug)]
struct AggregationOutputs {
    by_window: IndexMap<
        super::WindowId,
        Vec<crate::runtime::operators::window::aggregates::AggregationExecResult>,
    >,
}

#[derive(Debug)]
struct AdvanceOutcome {
    advanced_to: Option<Cursor>,
    still_pending: bool,
    accumulator_states_by_window:
        HashMap<super::WindowId, Option<crate::runtime::operators::window::window_operator_state::AccumulatorState>>,
}

impl WindowOperator {
    fn empty_output(&self) -> RecordBatch {
        RecordBatch::new_empty(self.output_schema.clone())
    }

    fn configure_aggregations(
        &self,
        windows_state: &crate::runtime::operators::window::window_operator_state::WindowsState,
        advance_to: Cursor,
    ) -> AggregationPlan {
        let mut window_agg_work_items: Vec<WindowAggWorkItem> = Vec::new();
        let mut load_plans: Vec<RangesLoadPlan> = Vec::new();
        let mut processed_pos_by_window: HashMap<super::WindowId, Cursor> = HashMap::new();
        let ref_window_id = *self
            .window_configs
            .keys()
            .next()
            .expect("window_configs must be non-empty");
        let mut entry_delta: Option<EntryDeltaPlan> = None;
        let batch_index = &windows_state.bucket_index;

        for (window_id, window_config) in self.window_configs.iter() {
            let window_frame = window_config.window_expr.get_window_frame();
            let window_state = windows_state
                .window_states
                .get(window_id)
                .expect("Window state should exist");
            let aggregator_type = window_config.aggregator_type;
            let tiles_owned = window_state.tiles.clone();
            let accumulator_state_owned = window_state.accumulator_state.clone();
            let prev_processed_pos = window_state.processed_pos;

            processed_pos_by_window.insert(*window_id, advance_to);

            if *window_id == ref_window_id {
                if let Some(update_range) = batch_index.delta_span(prev_processed_pos, advance_to) {
                    entry_delta = Some(EntryDeltaPlan {
                        prev: prev_processed_pos,
                        entry_range: update_range,
                    });
                }
            }

            let mut aggs: Vec<Box<dyn Aggregation>> = Vec::new();

            if self.execution_mode == ExecutionMode::Regular {
                match aggregator_type {
                    AggregatorType::RetractableAccumulator => {
                        if batch_index.delta_span(prev_processed_pos, advance_to).is_some() {
                            aggs.push(Box::new(RetractableAggregation::from_range(
                                *window_id,
                                prev_processed_pos,
                                advance_to,
                                batch_index,
                                window_config.window_expr.clone(),
                                accumulator_state_owned.clone(),
                                self.state_ref().ts_column_index(),
                                batch_index.bucket_granularity(),
                            )));
                        }
                    }
                    AggregatorType::PlainAccumulator => {
                        if batch_index.delta_span(prev_processed_pos, advance_to).is_some() {
                            aggs.push(Box::new(PlainAggregation::for_range(
                                batch_index,
                                window_config.window_expr.clone(),
                                tiles_owned.clone(),
                                crate::runtime::operators::window::aggregates::plain::CursorBounds {
                                    prev: prev_processed_pos,
                                    new: advance_to,
                                },
                            )));
                        }
                    }
                    AggregatorType::Evaluator => panic!("Evaluator is not supported yet"),
                }
            } else {
                match aggregator_type {
                    AggregatorType::RetractableAccumulator => {
                        if batch_index.delta_span(prev_processed_pos, advance_to).is_some() {
                            aggs.push(Box::new(RetractableAggregation::from_range(
                                *window_id,
                                prev_processed_pos,
                                advance_to,
                                batch_index,
                                window_config.window_expr.clone(),
                                accumulator_state_owned.clone(),
                                self.state_ref().ts_column_index(),
                                batch_index.bucket_granularity(),
                            )));
                        }
                    }
                    _ => {
                        let _ = window_frame;
                        let _ = tiles_owned;
                    }
                }
            }

            for agg in aggs {
                load_plans.push(RangesLoadPlan {
                    requests: agg.get_data_requests(),
                    window_expr_for_args: agg.window_expr().clone(),
                });
                window_agg_work_items.push(WindowAggWorkItem {
                    window_id: *window_id,
                    aggregation: agg,
                });
            }
        }

        AggregationPlan {
            window_agg_work_items,
            load_plans,
            processed_pos_by_window,
            entry_delta,
        }
    }

    async fn plan_advance_windows(&self, key: &Key, advance_to: Option<Cursor>) -> AdvancePlanOutcome {
        let windows_state_guard = self
            .state_ref()
            .get_windows_state(key)
            .await
            .expect("Windows state should exist - insert_batch should have created it");
        let windows_state = windows_state_guard.value();
        let batch_index = &windows_state.bucket_index;
        let max_pos_seen = batch_index.max_pos_seen();
        let desired_advance_to = advance_to.unwrap_or(max_pos_seen).min(max_pos_seen);

        let ref_window_id = *self
            .window_configs
            .keys()
            .next()
            .expect("window_configs must be non-empty");
        let prev_processed_pos = windows_state
            .window_states
            .get(&ref_window_id)
            .expect("Window state should exist")
            .processed_pos
            .unwrap_or(Cursor::new(i64::MIN, 0));

        let still_pending_noop = max_pos_seen > prev_processed_pos;
        if desired_advance_to <= prev_processed_pos {
            drop(windows_state_guard);
            return AdvancePlanOutcome::Noop {
                still_pending: still_pending_noop,
            };
        }

        if cfg!(debug_assertions) {
            let mut it = windows_state.window_states.values();
            let first = it.next().and_then(|s| s.processed_pos);
            for s in it {
                debug_assert_eq!(
                    first,
                    s.processed_pos,
                    "all windows must share processed_pos (required for consistent advancement)"
                );
            }
        }

        let aggregation_plan = self.configure_aggregations(windows_state, desired_advance_to);
        drop(windows_state_guard);

        if aggregation_plan.window_agg_work_items.is_empty() {
            return AdvancePlanOutcome::Noop {
                still_pending: still_pending_noop,
            };
        }

        AdvancePlanOutcome::Execute(AdvancePlan {
            aggregation_plan,
            max_pos_seen,
            prev_processed_pos,
        })
    }

    async fn execute_aggs_for_plan(
        &self,
        key: &Key,
        plan: &AdvancePlan,
    ) -> (LoadedRanges, AggregationOutputs) {
        let views_by_item = self
            .state_ref()
            .load_sorted_ranges_views(key, &plan.aggregation_plan.load_plans)
            .await;
        debug_assert_eq!(
            plan.aggregation_plan.window_agg_work_items.len(),
            views_by_item.len(),
            "window_agg_work_items and loaded views must stay aligned"
        );

        let futures: Vec<_> = plan
            .aggregation_plan
            .window_agg_work_items
            .iter()
            .zip(views_by_item.iter())
            .map(|(item, views)| async move {
                let result = item
                    .aggregation
                    .produce_aggregates_from_ranges(views, self.thread_pool.as_ref())
                    .await;
                (item.window_id, result)
            })
            .collect();

        let results = future::join_all(futures).await;
        let mut by_window: IndexMap<
            super::WindowId,
            Vec<crate::runtime::operators::window::aggregates::AggregationExecResult>,
        > = IndexMap::new();
        for (window_id, r) in results {
            by_window.entry(window_id).or_default().push(r);
        }

        (
            LoadedRanges { views_by_item },
            AggregationOutputs { by_window },
        )
    }

    fn derive_advance_results(&self, plan: &AdvancePlan, outputs: &AggregationOutputs) -> AdvanceOutcome {
        let mut candidate_processed_pos: Option<Cursor> = None;
        for rs in outputs.by_window.values() {
            for r in rs {
                if let Some(p) = r.processed_pos {
                    match candidate_processed_pos {
                        None => candidate_processed_pos = Some(p),
                        Some(existing) => debug_assert_eq!(
                            existing, p,
                            "range aggregations must agree on processed_pos"
                        ),
                    }
                }
            }
        }

        let advanced_to = candidate_processed_pos.filter(|p| *p > plan.prev_processed_pos);
        let still_pending = match advanced_to {
            Some(p) => plan.max_pos_seen > p,
            None => plan.max_pos_seen > plan.prev_processed_pos,
        };

        let accumulator_states_by_window: HashMap<
            super::WindowId,
            Option<crate::runtime::operators::window::window_operator_state::AccumulatorState>,
        > = outputs
            .by_window
            .iter()
            .map(|(window_id, results)| {
                let accumulator_state = results
                    .last()
                    .and_then(|r| r.accumulator_state.clone());
                (*window_id, accumulator_state)
            })
            .collect();

        AdvanceOutcome {
            advanced_to,
            still_pending,
            accumulator_states_by_window,
        }
    }

    async fn finalize_advance(
        &self,
        key: &Key,
        mut plan: AdvancePlan,
        loaded: LoadedRanges,
        outputs: AggregationOutputs,
        outcome: AdvanceOutcome,
    ) -> (RecordBatch, bool) {
        let mut out_batch = self.empty_output();

        if let Some(actual_processed_pos) = outcome.advanced_to {
            for v in plan.aggregation_plan.processed_pos_by_window.values_mut() {
                *v = actual_processed_pos;
            }

            self.state_ref()
                .update_window_positions_and_accumulators(
                    key,
                    &plan.aggregation_plan.processed_pos_by_window,
                    &outcome.accumulator_states_by_window,
                )
                .await;

            if self.execution_mode != ExecutionMode::Request {
                let input_values = match plan.aggregation_plan.entry_delta {
                    None => Vec::new(),
                    Some(ed) => extract_input_values_for_output(
                        (ed.prev, actual_processed_pos, ed.entry_range),
                        &loaded.views_by_item,
                        &self.input_schema,
                    ),
                };

                if !input_values.is_empty() {
                    let aggregated_values: Vec<Vec<ScalarValue>> = outputs
                        .by_window
                        .values()
                        .map(|results| {
                            results
                                .iter()
                                .flat_map(|r| r.values.clone())
                                .collect()
                        })
                        .collect();

                    debug_assert!(
                        aggregated_values
                            .iter()
                            .all(|col| col.len() == input_values.len()),
                        "All window result columns must match input row count"
                    );

                    out_batch = stack_concat_results(
                        input_values,
                        aggregated_values,
                        &self.output_schema,
                        &self.input_schema,
                    );
                }
            }
        }

        (out_batch, outcome.still_pending)
    }

    pub(super) async fn advance_windows(&self, key: &Key, advance_to: Option<Cursor>) -> (RecordBatch, bool) {
        let plan = self.plan_advance_windows(key, advance_to).await;
        let AdvancePlanOutcome::Execute(plan) = plan else {
            let AdvancePlanOutcome::Noop { still_pending } = plan else { unreachable!() };
            return (self.empty_output(), still_pending);
        };

        let (loaded, outputs) = self.execute_aggs_for_plan(key, &plan).await;
        let outcome = self.derive_advance_results(&plan, &outputs);
        self.finalize_advance(key, plan, loaded, outputs, outcome).await
    }
}

fn extract_input_values_for_output(
    (prev, new, entry_range): (Option<Cursor>, Cursor, BucketRange),
    views_by_agg: &[Vec<SortedRangeView>],
    input_schema: &arrow::datatypes::SchemaRef,
) -> Vec<Vec<ScalarValue>> {
    let view_for_entries = views_by_agg
        .iter()
        .flat_map(|g| g.iter())
        .find(|v| {
            let r = v.bucket_range();
            r.start <= entry_range.start && r.end >= entry_range.end
        });
    let Some(view) = view_for_entries else {
        return Vec::new();
    };

    let idx = SortedRangeIndex::new(view);
    let Some(mut pos) = window_logic::first_update_pos(&idx, prev) else {
        return Vec::new();
    };

    let end = match idx.seek_rowpos_gt(new) {
        Some(after) => match idx.prev_pos(after) {
            Some(p) => p,
            None => return Vec::new(),
        },
        None => idx.last_pos(),
    };
    if end < pos {
        return Vec::new();
    }

    let input_cols = input_schema.fields().len();
    let mut out: Vec<Vec<ScalarValue>> = Vec::new();
    loop {
        let bucket_ts = idx.bucket_ts(&pos);
        if bucket_ts >= entry_range.start && bucket_ts <= entry_range.end {
            let b = &view.segments()[pos.segment];
            let batch = b.batch();
            let mut row_vals = Vec::with_capacity(input_cols);
            for col_idx in 0..input_cols {
                row_vals.push(
                    ScalarValue::try_from_array(batch.column(col_idx), pos.row)
                        .expect("Should be able to extract scalar value"),
                );
            }
            out.push(row_vals);
        }
        if pos == end {
            break;
        }
        pos = idx.next_pos(pos).expect("must have next pos until end");
    }
    out
}

