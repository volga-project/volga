import logging
import time
from typing import Optional, Dict

import ray

from volga.streaming.api.job_graph.job_graph import JobGraph
from volga.streaming.api.operators.chained import ChainedSourceOperator
from volga.streaming.api.operators.operators import ISourceOperator, SourceOperator
from volga.streaming.runtime.config.streaming_config import StreamingConfig
from volga.streaming.runtime.core.execution_graph.execution_graph import ExecutionGraph
from volga.streaming.runtime.master.context.job_master_runtime_context import JobMasterRuntimeContext
from volga.streaming.runtime.master.resource_manager.node_assign_strategy import ParallelismFirst, OperatorFirst
from volga.streaming.runtime.master.resource_manager.resource_manager import ResourceManager
from volga.streaming.runtime.master.scheduler.job_scheduler import JobScheduler
from volga.streaming.runtime.master.stats.stats_manager import StatsManager
from volga.streaming.runtime.sources.source_splits_manager import SourceSplitManager, SourceSplit

logger = logging.getLogger("ray")


@ray.remote
class JobMaster:

    def __init__(self, job_config: Optional[Dict]):
        streaming_config = StreamingConfig.from_dict(job_config)
        self.master_config = streaming_config.master_config
        self.runtime_context = JobMasterRuntimeContext(streaming_config)

        self.resource_manager = ResourceManager()
        self.stats_manager = StatsManager()
        node_assign_strategy = OperatorFirst() # TODO config this
        # node_assign_strategy = ParallelismFirst()
        self.job_scheduler = JobScheduler(
            job_master=ray.get_runtime_context().current_actor,
            resource_manager=self.resource_manager,
            stats_manager=self.stats_manager,
            node_assign_strategy=node_assign_strategy,
            runtime_context=self.runtime_context
        )
        self.source_split_manager: Optional[SourceSplitManager] = None

        self.running = True
        self.sources_finished = {} # source vertex id to bool
        self.resource_manager.init()

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

        self._init_source_split_manager_if_needed()

        res = self.job_scheduler.schedule_job()
        if res:
            self.stats_manager.start()
        return res

    def notify_source_finished(self, task_id: str):
        if task_id not in self.sources_finished:
            raise RuntimeError(f'Unable to locate source for {task_id} execution vertex')

        self.sources_finished[task_id] = True
        logger.info(f'Source operator {task_id} finished')

    def _init_source_split_manager_if_needed(self):
        jg: JobGraph = self.runtime_context.job_graph
        if jg is None:
            raise RuntimeError('Job graph is not set')

        source_vertices = jg.get_source_vertices()
        split_enumerators = {}
        for sv in source_vertices:
            op = sv.stream_operator
            assert isinstance(op, ISourceOperator)
            if isinstance(op, ChainedSourceOperator):
                head_op = op.head_operator
                assert isinstance(head_op, SourceOperator)
                split_enumerator = head_op.split_enumerator
            elif isinstance(op, SourceOperator):
                split_enumerator = op.split_enumerator
            else:
                raise RuntimeError('Unknown source operator type')

            if split_enumerator is not None:
                # we assume operator_id == vertex_id
                split_enumerators[sv.vertex_id] = split_enumerator

        if len(split_enumerators) != 0:
            self.source_split_manager = SourceSplitManager(split_enumerators)

    def poll_next_source_split(self, operator_id: int, task_id: int) -> SourceSplit:
        if self.source_split_manager is None:
            raise RuntimeError('Attempt to use un-inited SourceSplitManager')

        return self.source_split_manager.poll_next_split(operator_id, task_id)

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
        self.stats_manager.stop()
        self.job_scheduler.destroy_job()
