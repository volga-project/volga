import logging

import ray

from volga.streaming.runtime.core.execution_graph.execution_graph import ExecutionVertex
from volga.streaming.runtime.core.processor.processor import Processor, SourceProcessor, OneInputProcessor
from volga.streaming.runtime.worker.task.stream_task import StreamTask, SourceStreamTask, \
    OneInputStreamTask, TwoInputStreamTask

# logger = logging.getLogger(__name__)
logger = logging.getLogger("ray")


@ray.remote
class JobWorker:

    def __init__(self):
        self.execution_vertex = None
        self.task = None

    def get_host_ip(self) -> str:
        return ray.util.get_node_ip_address()

    def init(self, execution_vertex: ExecutionVertex):
        self.execution_vertex = execution_vertex

    def start_or_rollback(self):
        self.task = self._create_stream_task()
        self.task.start_or_recover()

    def _create_stream_task(self) -> StreamTask:
        task = None
        stream_processor = Processor.build_processor(self.execution_vertex.stream_operator)
        if isinstance(stream_processor, SourceProcessor):
            task = SourceStreamTask(
                processor=stream_processor,
                execution_vertex=self.execution_vertex
            )
        elif isinstance(stream_processor, OneInputProcessor):
            task = OneInputStreamTask(
                processor=stream_processor,
                execution_vertex=self.execution_vertex
            )
        else:
            input_op_ids = set()
            for input_edge in self.execution_vertex.input_edges:
                input_op_ids.add(input_edge.source_execution_vertex.job_vertex.vertex_id)
            input_op_ids = list(input_op_ids)
            if len(input_op_ids) != 2:
                raise RuntimeError(f'Two input vertex should have exactly 2 edges, {len(input_op_ids)} given')
            left_stream_name = str(input_op_ids[0])
            right_stream_name = str(input_op_ids[1])
            task = TwoInputStreamTask(
                processor=stream_processor,
                execution_vertex=self.execution_vertex,
                left_stream_name=left_stream_name,
                right_stream_name=right_stream_name
            )
        return task

    def close(self):
        if self.task is not None:
            self.task.close()
        logger.info(f'Closed worker {self.execution_vertex.execution_vertex_id}')

    def exit(self):
        ray.actor.exit_actor()