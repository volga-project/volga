use serde::{Deserialize, Serialize};

/// One Prom matrix point.
#[derive(Clone, Debug, PartialEq)]
pub struct Sample {
    pub ts: f64,
    pub value: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PromQueryRange {
    pub status: String,
    #[serde(default)]
    pub data: PromData,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PromData {
    #[serde(default, rename = "resultType")]
    pub result_type: String,
    #[serde(default)]
    pub result: Vec<PromSeries>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PromSeries {
    #[serde(default)]
    pub metric: serde_json::Map<String, serde_json::Value>,
    #[serde(default)]
    pub values: Vec<(f64, String)>,
}

impl PromQueryRange {
    pub fn samples(&self) -> Vec<Sample> {
        let mut out = Vec::new();
        for series in &self.data.result {
            for (ts, value) in &series.values {
                if let Ok(v) = value.parse::<f64>() {
                    if v.is_finite() {
                        out.push(Sample { ts: *ts, value: v });
                    }
                }
            }
        }
        out.sort_by(|a, b| a.ts.total_cmp(&b.ts));
        out
    }

    /// Sum values that share a timestamp (multiple series).
    pub fn samples_summed(&self) -> Vec<Sample> {
        let mut by_ts: Vec<(f64, f64)> = Vec::new();
        for sample in self.samples() {
            match by_ts.last_mut() {
                Some((ts, acc)) if (*ts - sample.ts).abs() < 0.001 => *acc += sample.value,
                _ => by_ts.push((sample.ts, sample.value)),
            }
        }
        by_ts
            .into_iter()
            .map(|(ts, value)| Sample { ts, value })
            .collect()
    }
}

/// Named PromQL. Oracles require [`REQUIRED_PROM_QUERIES`]; extras are dump-only.
pub type PromQuery = (&'static str, &'static str);

pub const REQUIRED_PROM_QUERY_NAMES: &[&str] = &[
    "checkpoint_completed",
    "checkpoint_failed",
    "records_sent_rate",
    "watermark_lag_p99",
];

pub const REQUIRED_PROM_QUERIES: &[PromQuery] = &[
    (
        "checkpoint_completed",
        concat!(
            "increase(volga_checkpoint_completed",
            r#"{pipeline_id=~".*"}"#,
            "[5m])"
        ),
    ),
    (
        "checkpoint_failed",
        concat!(
            "increase(volga_checkpoint_failed",
            r#"{pipeline_id=~".*"}"#,
            "[5m])"
        ),
    ),
    (
        "records_sent_rate",
        concat!(
            "sum(rate(volga_stream_task_records_sent",
            r#"{pipeline_id=~".*"}"#,
            "[1m]))"
        ),
    ),
    (
        "watermark_lag_p99",
        concat!(
            "quantile(0.99, volga_stream_task_watermark_lag_ms",
            r#"{pipeline_id=~".*"}"#,
            ")"
        ),
    ),
];

pub const EXTRA_PROM_QUERIES: &[PromQuery] = &[
    (
        "checkpoint_duration_p99",
        concat!(
            "histogram_quantile(0.99, sum by (le) (rate(volga_checkpoint_duration_ms_bucket",
            r#"{pipeline_id=~".*"}"#,
            "[1m])))"
        ),
    ),
    (
        "busy_ms_per_s",
        concat!(
            "avg by (task_id) (volga_stream_task_busy_time_ms_per_second",
            r#"{pipeline_id=~".*"}"#,
            ")"
        ),
    ),
    (
        "idle_ms_per_s",
        concat!(
            "avg by (task_id) (volga_stream_task_idle_time_ms_per_second",
            r#"{pipeline_id=~".*"}"#,
            ")"
        ),
    ),
    (
        "backpressured_ms_per_s",
        concat!(
            "avg by (task_id) (volga_stream_task_backpressured_time_ms_per_second",
            r#"{pipeline_id=~".*"}"#,
            ")"
        ),
    ),
    (
        "watermark_lag_p50",
        concat!(
            "quantile(0.50, volga_stream_task_watermark_lag_ms",
            r#"{pipeline_id=~".*"}"#,
            ")"
        ),
    ),
    (
        "sink_records_rate",
        concat!(
            "sum(rate(volga_sink_records_written",
            r#"{pipeline_id=~".*"}"#,
            "[1m]))"
        ),
    ),
    (
        "wo_ingest_p99",
        concat!(
            "histogram_quantile(0.99, sum by (le) (rate(volga_wo_ingest_ms_bucket",
            r#"{pipeline_id=~".*"}"#,
            "[1m])))"
        ),
    ),
    (
        "wo_late_dropped_rate",
        concat!(
            "sum(rate(volga_wo_late_dropped_rows",
            r#"{pipeline_id=~".*"}"#,
            "[1m]))"
        ),
    ),
    (
        "wo_maintain_pruned_rate",
        concat!(
            "sum(rate(volga_wo_maintain_pruned_rows",
            r#"{pipeline_id=~".*"}"#,
            "[1m]))"
        ),
    ),
    (
        "wo_state_bytes",
        concat!(
            "sum(volga_wo_state_raw_bytes",
            r#"{pipeline_id=~".*"}"#,
            ") + sum(volga_wo_state_tiles_bytes",
            r#"{pipeline_id=~".*"}"#,
            ") + sum(volga_wo_state_triggers_bytes",
            r#"{pipeline_id=~".*"}"#,
            ") + sum(volga_wo_state_key_states_bytes",
            r#"{pipeline_id=~".*"}"#,
            ")"
        ),
    ),
];

pub fn extra_prom_queries() -> Vec<(String, String)> {
    EXTRA_PROM_QUERIES
        .iter()
        .map(|(name, expr)| (name.to_string(), expr.to_string()))
        .collect()
}

pub fn prom_queries(extra: &[(String, String)]) -> Vec<(String, String)> {
    REQUIRED_PROM_QUERIES
        .iter()
        .map(|(name, expr)| (name.to_string(), expr.to_string()))
        .chain(extra.iter().cloned())
        .collect()
}

pub async fn query_range(
    client: &reqwest::Client,
    prom_url: &str,
    query: &str,
    start: f64,
    end: f64,
    step: &str,
) -> anyhow::Result<PromQueryRange> {
    let url = format!("{}/api/v1/query_range", prom_url.trim_end_matches('/'));
    let resp = client
        .get(&url)
        .query(&[
            ("query", query),
            ("start", &format!("{start:.3}")),
            ("end", &format!("{end:.3}")),
            ("step", step),
        ])
        .send()
        .await?
        .error_for_status()?;
    let body: PromQueryRange = resp.json().await?;
    if body.status != "success" {
        anyhow::bail!(
            "prom query_range failed: {}",
            body.error.unwrap_or_else(|| body.status.clone())
        );
    }
    Ok(body)
}

pub async fn dump_prom_series(
    prom_url: &str,
    start: f64,
    end: f64,
    step: &str,
    queries: &[(String, String)],
) -> anyhow::Result<serde_json::Value> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()?;
    let mut series = serde_json::Map::new();
    for (name, query) in queries {
        let result = query_range(&client, prom_url, query, start, end, step).await?;
        series.insert(name.clone(), serde_json::to_value(&result)?);
    }
    Ok(serde_json::json!({
        "prom_url": prom_url,
        "start": start,
        "end": end,
        "step": step,
        "series": series,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn required_prom_queries_cover_oracle_names() {
        let names: Vec<_> = REQUIRED_PROM_QUERIES.iter().map(|(n, _)| *n).collect();
        assert_eq!(names, REQUIRED_PROM_QUERY_NAMES);
        let extra: Vec<_> = EXTRA_PROM_QUERIES.iter().map(|(n, _)| *n).collect();
        for name in REQUIRED_PROM_QUERY_NAMES {
            assert!(!extra.contains(name), "extra duplicates required {name}");
        }
    }
}
