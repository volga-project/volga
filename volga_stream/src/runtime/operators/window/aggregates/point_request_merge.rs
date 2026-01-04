use crate::runtime::operators::window::aggregates::BucketRange;
use crate::runtime::operators::window::state::index::{DataBounds, DataRequest};
use crate::runtime::operators::window::TimeGranularity;

#[derive(Debug, Clone)]
pub struct PlannedRange {
    pub orig_idx: usize,
    pub bucket_range: BucketRange,
    pub bounds: DataBounds,
    /// Used only for stable ordering when merging (e.g. point timestamp).
    pub sort_ts: i64,
}

/// Merge overlapping/adjacent planned ranges into non-overlapping `DataRequest`s.
///
/// Returns `(requests, req_idx_by_orig)` where `req_idx_by_orig[orig_idx]` points to the merged
/// request containing that original plan.
pub fn merge_planned_ranges(
    bucket_granularity: TimeGranularity,
    num_origs: usize,
    mut planned: Vec<PlannedRange>,
) -> (Vec<DataRequest>, Vec<Option<usize>>) {
    if planned.is_empty() {
        return (Vec::new(), vec![None; num_origs]);
    }

    planned.sort_by_key(|t| (t.bucket_range.start, t.bucket_range.end, t.sort_ts, t.orig_idx));

    let mut requests: Vec<DataRequest> = Vec::new();
    let mut req_idx_by_orig: Vec<Option<usize>> = vec![None; num_origs];

    let mut cur_req: Option<DataRequest> = None;
    let mut cur_idx: usize = 0;

    for t in planned {
        if t.orig_idx >= num_origs {
            panic!("orig_idx out of range: {} >= {}", t.orig_idx, num_origs);
        }

        match cur_req {
            None => {
                let req = DataRequest {
                    bucket_range: t.bucket_range,
                    bounds: t.bounds,
                };
                requests.push(req);
                cur_idx = requests.len() - 1;
                cur_req = Some(req);
                req_idx_by_orig[t.orig_idx] = Some(cur_idx);
            }
            Some(mut req) => {
                let bucket_adjacent_or_overlap =
                    t.bucket_range.start <= bucket_granularity.next_start(req.bucket_range.end);
                let can_merge = match (req.bounds, t.bounds) {
                    (DataBounds::All, DataBounds::All) => bucket_adjacent_or_overlap,
                    (
                        DataBounds::Time {
                            start_ts: _a_s,
                            end_ts: a_e,
                        },
                        DataBounds::Time {
                            start_ts: b_s,
                            end_ts: _b_e,
                        },
                    ) => bucket_adjacent_or_overlap && b_s <= a_e,
                    _ => false,
                };

                if can_merge {
                    req.bucket_range.end = req.bucket_range.end.max(t.bucket_range.end);
                    req.bounds = match (req.bounds, t.bounds) {
                        (
                            DataBounds::Time {
                                start_ts: a_s,
                                end_ts: a_e,
                            },
                            DataBounds::Time {
                                start_ts: b_s,
                                end_ts: b_e,
                            },
                        ) => DataBounds::Time {
                            start_ts: a_s.min(b_s),
                            end_ts: a_e.max(b_e),
                        },
                        (b, _) => b,
                    };
                    cur_req = Some(req);
                    *requests.last_mut().expect("must exist") = req;
                    req_idx_by_orig[t.orig_idx] = Some(cur_idx);
                } else {
                    let req2 = DataRequest {
                        bucket_range: t.bucket_range,
                        bounds: t.bounds,
                    };
                    requests.push(req2);
                    cur_idx = requests.len() - 1;
                    cur_req = Some(req2);
                    req_idx_by_orig[t.orig_idx] = Some(cur_idx);
                }
            }
        }
    }

    (requests, req_idx_by_orig)
}


