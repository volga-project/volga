import logging
import time
from typing import Optional, Dict

from ray.actor import ActorHandle
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from volga.ray_utils import get_head_node_id
from volga.streaming.api.job_graph.job_graph import JobGraph
from volga.streaming.runtime.master.job_master import JobMaster

import ray

# logger = logging.getLogger(__name__)
logger = logging.getLogger("ray")


class JobClient:

    def submit(
        self,
        job_graph: JobGraph,
        job_config: Optional[Dict] = None,
        master_on_head: bool = True
    ) -> ActorHandle:
        # TODO master resources
        options_kwargs = {
            'max_restarts': -1,
            'max_concurrency': 1000
        }

        if master_on_head:
            options_kwargs['num_cpus'] = 0
            options_kwargs['scheduling_strategy'] = NodeAffinitySchedulingStrategy(
                node_id=get_head_node_id(),
                soft=False
            )

        master = JobMaster.options(**options_kwargs).remote(
            job_config=job_config,
        )
        logger.info('Started JobMaster')
        submit_res = ray.get(master.submit_job.remote(job_graph))
        logger.info(f'Submitted {job_graph.job_name} with status {submit_res}')

        # TODO return submit_res as well
        return master

    def execute(
        self,
        job_graph: JobGraph,
        job_config: Optional[Dict] = None,
        timeout_s: Optional[int] = None
    ):
        job_master = self.submit(job_graph=job_graph, job_config=job_config)
        ray.get(job_master.wait_sources_finished.remote(timeout_s))
        OPTIMISTIC_FINISH_TIME_S = 5 # we assume all workers finish within this time after sources reported finish
        # TODO implement proper job finish where all workers report finish
        time.sleep(OPTIMISTIC_FINISH_TIME_S)
        ray.get(job_master.destroy.remote())
        return job_master



