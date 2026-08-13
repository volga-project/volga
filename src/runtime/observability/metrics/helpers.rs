//! Thin wrappers around metrics macros with standard task/pipeline labels.

use metrics::{counter, gauge, histogram};

use super::names::{MetricsLabels, LABEL_PIPELINE_ID, LABEL_TASK_ID, LABEL_WORKER_ID};

/// Record a per-task histogram sample, with optional pipeline/worker labels.
pub fn record_task_histogram(
    name: &'static str,
    value: f64,
    task_id: &str,
    labels: Option<&MetricsLabels>,
) {
    let task = task_id.to_string();
    if let Some(labels) = labels {
        histogram!(
            name,
            LABEL_TASK_ID => task,
            LABEL_PIPELINE_ID => labels.pipeline_id.clone(),
            LABEL_WORKER_ID => labels.worker_id.clone(),
        )
        .record(value);
    } else {
        histogram!(name, LABEL_TASK_ID => task).record(value);
    }
}

/// Increment a per-task counter, with optional pipeline/worker labels.
pub fn increment_task_counter(
    name: &'static str,
    delta: u64,
    task_id: &str,
    labels: Option<&MetricsLabels>,
) {
    if delta == 0 {
        return;
    }
    let task = task_id.to_string();
    if let Some(labels) = labels {
        counter!(
            name,
            LABEL_TASK_ID => task,
            LABEL_PIPELINE_ID => labels.pipeline_id.clone(),
            LABEL_WORKER_ID => labels.worker_id.clone(),
        )
        .increment(delta);
    } else {
        counter!(name, LABEL_TASK_ID => task).increment(delta);
    }
}

/// Set a per-task gauge, with optional pipeline/worker labels.
pub fn set_task_gauge(
    name: &'static str,
    value: f64,
    task_id: &str,
    labels: Option<&MetricsLabels>,
) {
    let task = task_id.to_string();
    if let Some(labels) = labels {
        gauge!(
            name,
            LABEL_TASK_ID => task,
            LABEL_PIPELINE_ID => labels.pipeline_id.clone(),
            LABEL_WORKER_ID => labels.worker_id.clone(),
        )
        .set(value);
    } else {
        gauge!(name, LABEL_TASK_ID => task).set(value);
    }
}

/// Record a pipeline-scoped histogram sample (master / job-level).
pub fn record_pipeline_histogram(name: &'static str, value_ms: f64, pipeline_id: &str) {
    histogram!(name, LABEL_PIPELINE_ID => pipeline_id.to_string()).record(value_ms);
}

/// Increment a pipeline-scoped counter (master / job-level).
pub fn increment_pipeline_counter(name: &'static str, delta: u64, pipeline_id: &str) {
    if delta == 0 {
        return;
    }
    counter!(name, LABEL_PIPELINE_ID => pipeline_id.to_string()).increment(delta);
}

