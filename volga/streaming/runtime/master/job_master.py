import logging
from typing import Optional, Dict

import ray

from volga.streaming.api.job_graph.job_graph import JobGraph
from volga.streaming.runtime.config.streaming_config import StreamingConfig
from volga.streaming.runtime.core.execution_graph.execution_graph import ExecutionGraph
from volga.streaming.runtime.master.context.job_master_runtime_context import JobMasterRuntimeContext
from volga.streaming.runtime.master.resource_manager.resource_manager import ResourceManager
from volga.streaming.runtime.master.scheduler.job_scheduler import JobScheduler


logger = logging.getLogger("ray")


@ray.remote
class JobMaster:

    def __init__(self, job_config: Optional[Dict]):
        streaming_config = StreamingConfig.from_dict(job_config)
        self.master_config = streaming_config.master_config
        self.runtime_context = JobMasterRuntimeContext(streaming_config)
        self.job_scheduler = JobScheduler(
            job_master=ray.get_runtime_context().current_actor,
            runtime_context=self.runtime_context
        )
        self.resource_manager = ResourceManager()

    def submit_job(self, job_graph: JobGraph) -> bool:
        execution_graph = ExecutionGraph.from_job_graph(job_graph)
        logger.info('Execution graph:')
        logger.info(f'\n{execution_graph.gen_digraph()}')

        # set resources
        execution_graph.set_resources(self.master_config.resource_config)

        self.runtime_context.execution_graph = execution_graph
        self.runtime_context.job_graph = job_graph
        return self.job_scheduler.schedule_job()

    def destroy(self):
        self.job_scheduler.destroy_job()
