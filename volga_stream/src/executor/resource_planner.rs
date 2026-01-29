use crate::api::spec::resources::{ResourceProfile, ResourceProfiles, ResourceStrategy};
use crate::executor::placement::WorkerTaskPlacement;
#[derive(Clone, Debug)]
pub struct ResourcePlan {
    pub worker_resources: Vec<ResourceProfile>,
}

pub struct ResourcePlanner;

impl ResourcePlanner {
    pub fn plan(
        placements: &[WorkerTaskPlacement],
        strategy: &ResourceStrategy,
        profiles: &ResourceProfiles,
    ) -> ResourcePlan {
        let workers = placements.len().max(1);
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
