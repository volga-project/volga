use crate::api::spec::resources::{ResourceProfile, ResourceProfiles, ResourceStrategy};
use crate::runtime::execution_graph::ExecutionGraph;

#[derive(Clone, Debug)]
pub struct ResourcePlan {
    pub worker_resources: Vec<ResourceProfile>,
}

pub trait ResourcePlanner: Send + Sync {
    fn plan(
        &self,
        graph: &ExecutionGraph,
        num_workers: usize,
        strategy: &ResourceStrategy,
        profiles: &ResourceProfiles,
    ) -> ResourcePlan;
}

pub struct DefaultResourcePlanner;

impl ResourcePlanner for DefaultResourcePlanner {
    fn plan(
        &self,
        _graph: &ExecutionGraph,
        num_workers: usize,
        strategy: &ResourceStrategy,
        profiles: &ResourceProfiles,
    ) -> ResourcePlan {
        let workers = num_workers.max(1);
        let profile = match strategy {
            ResourceStrategy::PerWorker => profiles.worker_default.clone(),
            ResourceStrategy::PerOperatorType => {
                // TODO: implement operator-type planning with stateless chaining.
                profiles.worker_default.clone()
            }
        };
        ResourcePlan {
            worker_resources: vec![profile; workers],
        }
    }
}
