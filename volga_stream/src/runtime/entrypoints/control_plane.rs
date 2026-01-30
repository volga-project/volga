use anyhow::Result;

use crate::runtime::execution_plan::ExecutionPlan;

pub async fn run_with_plan(_plan: ExecutionPlan) -> Result<()> {
    Ok(())
}
