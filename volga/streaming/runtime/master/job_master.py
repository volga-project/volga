import logging
import time
from typing import Optional, Dict, List

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

        self.running = True
        self.sources_finished = {} # source vertex id to bool

    def submit_job(self, job_graph: JobGraph) -> bool:
        execution_graph = ExecutionGraph.from_job_graph(job_graph)
        logger.info('Execution graph:')
        logger.info(f'\n{execution_graph.gen_digraph()}')

        # set resources
        execution_graph.set_resources(self.master_config.resource_config)

        self.runtime_context.execution_graph = execution_graph
        self.runtime_context.job_graph = job_graph

        # init sources states
        for v in self.runtime_context.execution_graph.get_source_vertices():
            self.sources_finished[v.execution_vertex_id] = False

        return self.job_scheduler.schedule_job()

    def notify_source_finished(self, task_id: str):
        if task_id not in self.sources_finished:
            raise RuntimeError(f'Unable to locate source for {task_id} execution vertex')

        self.sources_finished[task_id] = True
        logger.info(f'Source operator {task_id} finished')

    def _all_sources_finished(self) -> bool:
        # optimistic close
        all_sources_finished = True
        for v in self.sources_finished:
            all_sources_finished &= self.sources_finished[v]
        return all_sources_finished

    def wait_sources_finished(self):
        while self.running:
            if self._all_sources_finished():
                break
            time.sleep(0.1)
        logger.info('All sources finished')

    def destroy(self):
        self.running = False
        self.job_scheduler.destroy_job()
