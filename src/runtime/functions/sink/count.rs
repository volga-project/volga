use std::any::Any;

use anyhow::{anyhow, Result};
use async_trait::async_trait;

use crate::common::message::Message;
use crate::runtime::functions::function_trait::FunctionTrait;
use crate::runtime::functions::sink::sink_function::SinkFunctionTrait;
use crate::runtime::metrics::{increment_task_counter, MetricsLabels, METRIC_SINK_RECORDS_WRITTEN};
use crate::runtime::runtime_context::RuntimeContext;

/// Drop-payload sink: count records via `volga_sink_records_written` and discard the batch.
#[derive(Debug, Default)]
pub struct CountSinkFunction {
    vertex_id: Option<String>,
    metrics_labels: Option<MetricsLabels>,
}

impl CountSinkFunction {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl SinkFunctionTrait for CountSinkFunction {
    async fn sink(&mut self, message: Message) -> Result<()> {
        let vertex_id = self
            .vertex_id
            .as_deref()
            .ok_or_else(|| anyhow!("count sink is not open"))?;
        increment_task_counter(
            METRIC_SINK_RECORDS_WRITTEN,
            message.num_records() as u64,
            vertex_id,
            self.metrics_labels.as_ref(),
        );
        Ok(())
    }
}

#[async_trait]
impl FunctionTrait for CountSinkFunction {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()> {
        self.vertex_id = Some(context.vertex_id().to_string());
        self.metrics_labels = context.metrics_labels();
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::common::create_test_string_batch;
    use crate::runtime::metrics::{
        collect_task_metric_values, init_metrics, METRIC_SINK_RECORDS_WRITTEN,
    };
    use crate::runtime::runtime_context::RuntimeContext;

    #[tokio::test]
    async fn count_sink_increments_records_written_and_drops_payload() -> Result<()> {
        init_metrics();
        let mut sink = CountSinkFunction::new();
        let context = RuntimeContext::new("count_sink".to_string().into(), 0, 1, None, None, None);
        sink.open(&context).await?;

        sink.sink(Message::new(
            None,
            create_test_string_batch(vec!["a".to_string(), "b".to_string(), "c".to_string()]),
            None,
            None,
        ))
        .await?;
        sink.sink(Message::new(
            None,
            create_test_string_batch(vec!["d".to_string(), "e".to_string()]),
            None,
            None,
        ))
        .await?;

        let values = collect_task_metric_values("count_sink", None, &[METRIC_SINK_RECORDS_WRITTEN]);
        assert_eq!(
            values.counters.get(METRIC_SINK_RECORDS_WRITTEN).copied(),
            Some(5)
        );
        Ok(())
    }
}
