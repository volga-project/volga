import logging
import time
import socket
from threading import Thread
from typing import Tuple, Optional, List

import ray
from ray.actor import ActorHandle

from volga.streaming.api.operators.operators import SourceOperator, ISourceOperator
from volga.streaming.runtime.core.execution_graph.execution_graph import ExecutionVertex
from volga.streaming.runtime.core.processor.processor import Processor, SourceProcessor, OneInputProcessor
from volga.streaming.runtime.master.stats.stats_manager import WorkerStatsUpdate
from volga.streaming.runtime.worker.task.stream_task import StreamTask, SourceStreamTask, \
    OneInputStreamTask, TwoInputStreamTask

logger = logging.getLogger('ray')


VALID_PORT_RANGE = (10000, 20000)


class WorkerNodeInfo:

    def __init__(self, node_ip: str, node_id: str, na_ports: List[int]):
        self.node_ip = node_ip
        self.node_id = node_id
        self.na_ports = na_ports # list of not-available ports

    def __repr__(self):
        return f'(node_id={self.node_id}, node_ip={self.node_ip}, num_na_ports={len(self.na_ports)})'


@ray.remote
class JobWorker:

    def __init__(self, job_master: ActorHandle):
        self.job_master = job_master
        self.execution_vertex = None
        self.task = None
        self.task_watcher_thread = None
        self.running = True

    def get_host_info(self) -> WorkerNodeInfo:
        ctx = ray.get_runtime_context()
        node_id = ctx.get_node_id()
        node_ip = ray.util.get_node_ip_address()
        na_ports = []
        for port in range(VALID_PORT_RANGE[0], VALID_PORT_RANGE[1] + 1):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                sock.bind(('0.0.0.0', port))
            except:
                na_ports.append(port)
            sock.close()
        if len(na_ports) == VALID_PORT_RANGE[1] - VALID_PORT_RANGE[0] + 1:
            raise RuntimeError(f'No available ports on node {node_ip}')
        return WorkerNodeInfo(node_ip, node_id, na_ports)

    def init(self, execution_vertex: ExecutionVertex):
        self.execution_vertex = execution_vertex

    def start_or_rollback(self) -> Optional[str]:
        # TODO there is some sort of race condition between starting transfer actors and workers -
        # TODO we need to start transfer actors first and asynchronously other workers after a small delay -
        # TODO figure out how to synchronize this
        time.sleep(1)

        self.task = self._create_stream_task()
        err = self.task.start_or_recover()
        if err is not None:
            return err

        # watch task state
        self.task_watcher_thread = Thread(target=self._watch_task_state_loop)
        self.task_watcher_thread.start()
        return None

    def _watch_task_state_loop(self):
        while self.running:
            # currently only check source
            if isinstance(self.task, SourceStreamTask):
                source_op = self.task.processor.operator
                assert isinstance(source_op, ISourceOperator)
                source_finished = source_op.get_source_context().finished

                # notify master if source finished
                if source_finished:
                    self.job_master.notify_source_finished.remote(
                        task_id=source_op.get_source_context().runtime_context.task_id
                    )
                    break
            time.sleep(0.1)

    def _create_stream_task(self) -> StreamTask:
        init_timeout_s = 5
        # we wait in case init has not happened yet
        start_ts = time.time()
        not_inited = self.execution_vertex is None
        while self.execution_vertex is None:
            if time.time() - start_ts > init_timeout_s:
                actor_id = ray.get_runtime_context().get_actor_id()
                raise RuntimeError(f'Init timeout for actor {actor_id}')
            time.sleep(0.1)

        assert self.execution_vertex is not None
        init_delay = time.time() - start_ts
        if not_inited:
            logger.info(f'Worker {self.execution_vertex.execution_vertex_id} inited with delay {init_delay}s')

        stream_processor = Processor.build_processor(self.execution_vertex.stream_operator)
        if isinstance(stream_processor, SourceProcessor):
            task = SourceStreamTask(
                job_master=self.job_master,
                processor=stream_processor,
                execution_vertex=self.execution_vertex
            )
        elif isinstance(stream_processor, OneInputProcessor):
            task = OneInputStreamTask(
                job_master=self.job_master,
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
                job_master=self.job_master,
                processor=stream_processor,
                execution_vertex=self.execution_vertex,
                left_stream_name=left_stream_name,
                right_stream_name=right_stream_name
            )
        return task

    def close(self):
        if self.task is not None:
            self.task.close()
        self.running = False
        if self.task_watcher_thread is not None:
            self.task_watcher_thread.join(timeout=5)
        logger.info(f'Closed worker {self.execution_vertex.execution_vertex_id}')

    def exit(self):
        ray.actor.exit_actor()

    def collect_stats(self) -> WorkerStatsUpdate:
        return self.task.collect_stats()