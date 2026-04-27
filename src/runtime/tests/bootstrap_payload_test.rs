#![cfg(test)]

use std::collections::HashMap;

use crate::api::PipelineSpecBuilder;
use crate::control_plane::types::{AttemptId, ExecutionIds};
use crate::executor::bootstrap::{MasterBootstrapPayload, WorkerBootstrapPayload};
use crate::runtime::execution_plan::ExecutionPlan;

#[test]
fn bootstrap_payload_roundtrip_bincode() {
    let execution_ids = ExecutionIds::fresh(AttemptId(7));
    let spec = PipelineSpecBuilder::new().build();
    let plan = ExecutionPlan::from_spec(
        execution_ids.clone(),
        spec,
        HashMap::new(),
        HashMap::new(),
    );

    let master = MasterBootstrapPayload {
        execution_ids: execution_ids.clone(),
        plan: plan.clone(),
    };
    let worker = WorkerBootstrapPayload {
        execution_ids,
        plan,
        worker_id: "worker-0".to_string(),
        assigned_task_ids: vec!["task-0".to_string()],
    };

    let master_bytes = bincode::serialize(&master).expect("serialize master payload");
    let worker_bytes = bincode::serialize(&worker).expect("serialize worker payload");

    let master_decoded: MasterBootstrapPayload =
        bincode::deserialize(&master_bytes).expect("deserialize master payload");
    let worker_decoded: WorkerBootstrapPayload =
        bincode::deserialize(&worker_bytes).expect("deserialize worker payload");

    assert_eq!(master_decoded.plan.worker_task_ids.len(), 0);
    assert_eq!(worker_decoded.worker_id, "worker-0");
}
