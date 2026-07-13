use std::time::Duration;
use std::collections::HashSet;

use anyhow::{anyhow, Result};
use arrow::array::StringArray;

use super::handles::MasterHandle;
use crate::runtime::master::{LifecycleEvent, LifecycleEventRecord};
use crate::storage::InMemoryStorageSnapshot;

#[derive(Debug, Clone, Default)]
pub struct OutputOracle;

impl OutputOracle {
    pub fn assert_string_column_values(
        snapshot: &InMemoryStorageSnapshot,
        column_index: usize,
        expected_values: &[String],
        expected_total_rows: usize,
    ) -> Result<()> {
        let values = Self::string_column_values(snapshot, column_index)?;
        if values.len() != expected_total_rows {
            return Err(anyhow!(
                "expected {expected_total_rows} output rows, got {}",
                values.len()
            ));
        }
        Self::assert_expected_values(&values, expected_values)
    }

    fn assert_expected_values(values: &[String], expected_values: &[String]) -> Result<()> {
        let expected = expected_values
            .iter()
            .map(String::as_str)
            .collect::<HashSet<_>>();
        for expected_value in &expected {
            if !values.iter().any(|value| value == expected_value) {
                return Err(anyhow!(
                    "expected output value '{expected_value}' was not emitted"
                ));
            }
        }
        for value in values {
            if !expected.contains(value.as_str()) {
                return Err(anyhow!("unexpected output value '{value}'"));
            }
        }
        Ok(())
    }

    pub fn assert_string_column_matches(
        snapshot: &InMemoryStorageSnapshot,
        column_index: usize,
        expected_total_rows: usize,
        description: &str,
        predicate: impl Fn(&str) -> bool,
    ) -> Result<()> {
        let values = Self::string_column_values(snapshot, column_index)?;
        if values.len() != expected_total_rows {
            return Err(anyhow!(
                "expected {expected_total_rows} output rows, got {}",
                values.len()
            ));
        }
        for value in values {
            if !predicate(&value) {
                return Err(anyhow!(
                    "output value '{value}' does not satisfy {description}"
                ));
            }
        }
        Ok(())
    }

    fn string_column_values(
        snapshot: &InMemoryStorageSnapshot,
        column_index: usize,
    ) -> Result<Vec<String>> {
        let mut output = Vec::new();
        for batch in snapshot.record_batches() {
            let values = batch
                .column(column_index)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow!("expected StringArray at output column {column_index}"))?;
            output.extend((0..batch.num_rows()).map(|row| values.value(row).to_string()));
        }
        Ok(output)
    }
}

#[derive(Debug, Clone, Default)]
pub struct LifecycleOracle;

impl LifecycleOracle {
    pub async fn wait_for(
        master: &MasterHandle,
        cursor: &mut u64,
        timeout: Duration,
        predicate: impl Fn(&LifecycleEvent) -> bool,
    ) -> Result<LifecycleEventRecord> {
        let started = tokio::time::Instant::now();
        loop {
            for record in master.lifecycle_events_since(*cursor).await? {
                *cursor = record.sequence;
                if predicate(&record.event) {
                    return Ok(record);
                }
            }
            if started.elapsed() >= timeout {
                return Err(anyhow!("timed out waiting for lifecycle event"));
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }
}
