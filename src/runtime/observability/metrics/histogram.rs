//! Cumulative bucket histogram helpers (Prom-shaped).

use serde::{Deserialize, Serialize};

use super::names::{LATENCY_BUCKET_BOUNDARIES, LATENCY_HISTOGRAM_LEN};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistogramMetrics {
    /// Cumulative hist buckets (finite bounds + trailing +Inf count).
    pub buckets: Vec<u64>,
    pub p99: f64,
    pub p95: f64,
    pub p50: f64,
    pub avg: f64,
}

impl HistogramMetrics {
    pub fn new(buckets: Vec<u64>) -> Self {
        let (p99, p95, p50, avg) = Self::calculate_stats(&buckets);
        Self {
            buckets,
            p99,
            p95,
            p50,
            avg,
        }
    }

    pub fn merge(hists: Vec<&HistogramMetrics>) -> Self {
        if hists.is_empty() {
            return Self::new(vec![0u64; LATENCY_HISTOGRAM_LEN]);
        }

        // Merge by summing corresponding cumulative buckets
        let mut merged_histogram = vec![0u64; LATENCY_HISTOGRAM_LEN];
        for stats in hists.iter() {
            for (i, bucket) in merged_histogram.iter_mut().enumerate() {
                if i < stats.buckets.len() {
                    *bucket += stats.buckets[i];
                }
            }
        }

        Self::new(merged_histogram)
    }

    /// Percentiles/avg from cumulative buckets (finite bounds + trailing +Inf).
    pub fn calculate_stats(histogram: &Vec<u64>) -> (f64, f64, f64, f64) {
        // Handle edge cases
        if histogram.is_empty() || histogram.len() != LATENCY_HISTOGRAM_LEN {
            return (0.0, 0.0, 0.0, 0.0);
        }
        
        // Total samples = +Inf cumulative bucket (includes overflow past last finite bound)
        let total_samples = *histogram.last().unwrap();
        if total_samples == 0 {
            return (0.0, 0.0, 0.0, 0.0);
        }
        
        // Calculate percentiles
        let p99 = Self::calculate_percentile(histogram, total_samples, 99.0);
        let p95 = Self::calculate_percentile(histogram, total_samples, 95.0);
        let p50 = Self::calculate_percentile(histogram, total_samples, 50.0);
        
        // Calculate average using weighted sum of bucket midpoints
        let avg = Self::calculate_weighted_average(histogram, total_samples);
        
        (p99, p95, p50, avg)
    }

    fn calculate_percentile(histogram: &Vec<u64>, total_samples: u64, percentile: f64) -> f64 {
        let boundaries = &LATENCY_BUCKET_BOUNDARIES;
        let last_finite = *boundaries.last().unwrap();
        let target_count = (total_samples as f64 * percentile / 100.0) as u64;
        
        // Find the bucket containing the target count (last index is +Inf / overflow)
        for (i, &bucket_count) in histogram.iter().enumerate() {
            if bucket_count >= target_count {
                let prev_count = if i == 0 { 0 } else { histogram[i - 1] };
                let bucket_samples = bucket_count - prev_count;
                let boundary = if i < boundaries.len() {
                    boundaries[i]
                } else {
                    // Overflow: report lower bound (last finite boundary)
                    last_finite
                };

                if bucket_samples == 0 {
                    return boundary;
                }

                let prev_boundary = if i == 0 {
                    0.0
                } else if i <= boundaries.len() {
                    boundaries[i - 1]
                } else {
                    last_finite
                };
                let samples_into_bucket = target_count - prev_count;
                let fraction = samples_into_bucket as f64 / bucket_samples as f64;

                return prev_boundary + (boundary - prev_boundary) * fraction;
            }
        }
        
        last_finite
    }

    fn calculate_weighted_average(histogram: &Vec<u64>, total_samples: u64) -> f64 {
        let boundaries = &LATENCY_BUCKET_BOUNDARIES;
        let last_finite = *boundaries.last().unwrap();
        let mut weighted_sum = 0.0;
        
        for (i, &bucket_count) in histogram.iter().enumerate() {
            let bucket_midpoint = if i < boundaries.len() {
                let boundary = boundaries[i];
                let prev_boundary = if i == 0 { 0.0 } else { boundaries[i - 1] };
                (prev_boundary + boundary) / 2.0
            } else {
                // Overflow samples (>= last finite): use last finite as lower-bound estimate
                last_finite
            };
            
            // Get samples in this bucket (not cumulative)
            let prev_count = if i == 0 { 0 } else { histogram[i - 1] };
            let bucket_samples = bucket_count - prev_count;
            
            weighted_sum += bucket_midpoint * bucket_samples as f64;
        }
        
        weighted_sum / total_samples as f64
    }
}

