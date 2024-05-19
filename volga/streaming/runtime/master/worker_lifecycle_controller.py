import logging
import time
from random import randint
from typing import Dict, List, Any

import ray
from ray.actor import ActorHandle

from volga.streaming.runtime.core.execution_graph.execution_graph import ExecutionGraph, ExecutionVertex
from volga.streaming.runtime.transfer.channel import Channel
from volga.streaming.runtime.worker.job_worker import JobWorker

VALID_PORT_RANGE = (30000, 65000)


# logger = logging.getLogger(__name__)
logger = logging.getLogger("ray")


class WorkerNetworkInfo:

    def __init__(self, node_ip: str, node_id: str, out_edges_ports: Dict[Any, int]):
        self.node_ip = node_ip
        self.node_id = node_id
        # port per output edge
        self.out_edges_ports = out_edges_ports


class WorkerLifecycleController:

    def __init__(self, job_master: ActorHandle):
        self.job_master = job_master
        self._used_ports = {}

    def create_workers(self, execution_graph: ExecutionGraph):
        workers = {}
        vertex_ids = []
        logger.info(f'Creating {len(execution_graph.execution_vertices_by_id)} workers...')
        for vertex_id in execution_graph.execution_vertices_by_id:
            vertex = execution_graph.execution_vertices_by_id[vertex_id]
            resources = vertex.resources
            options_kwargs = {
                'max_restarts': -1
            }
            if resources.num_cpus is not None:
                options_kwargs['num_cpus'] = resources.num_cpus
            if resources.num_gpus is not None:
                options_kwargs['num_gpus'] = resources.num_gpus
            if resources.memory is not None:
                options_kwargs['memory'] = resources.memory
            worker = JobWorker.options(**options_kwargs).remote(job_master=self.job_master)
            vertex_ids.append(vertex_id)
            workers[vertex_id] = worker
            vertex.set_worker(worker)

        worker_hosts_info = ray.get([workers[vertex_id].get_host_info.remote() for vertex_id in vertex_ids])
        worker_infos = []
        for i in range(len(vertex_ids)):
            vertex_id = vertex_ids[i]
            node_id, node_ip = worker_hosts_info[i]
            vertex = execution_graph.execution_vertices_by_id[vertex_id]
            # gen ports for each output edge
            out_edges_ports = {}
            for out_edge in vertex.output_edges:
                out_edges_ports[out_edge.id] = self._gen_port(node_ip)
            ni = WorkerNetworkInfo(
                node_ip=node_ip,
                node_id=node_id,
                # TODO we assume node_ip == node_id
                out_edges_ports=out_edges_ports
            )
            vertex.set_worker_network_info(ni)
            worker_infos.append((vertex_id, ni.node_id, ni.node_ip, ni.out_edges_ports))

        logger.info(f'Created {len(workers)} workers')
        logger.info(f'Workers writer network info: {worker_infos}')

    # construct channels based on Ray assigned actor IPs and update execution_graph
    def connect_and_init_workers(self, execution_graph: ExecutionGraph):
        logger.info(f'Initializing {len(execution_graph.execution_vertices_by_id)} workers...')

        # create channels
        for edge in execution_graph.execution_edges:
            worker_network_info: WorkerNetworkInfo = edge.source_execution_vertex.worker_network_info
            if worker_network_info is None:
                raise RuntimeError(f'Vertex {edge.source_execution_vertex.job_vertex.get_name()} has no worker network info')

            edge.set_channel(Channel(
                channel_id=edge.id,
                source_ip=worker_network_info.node_ip,
                source_port=worker_network_info.out_edges_ports[edge.id]
            ))

        # init workers
        f = []
        for execution_vertex in execution_graph.execution_vertices_by_id.values():
            worker = execution_vertex.worker
            f.append(worker.init.remote(execution_vertex))

        t = time.time()
        ray.wait(f)
        logger.info(f'Inited workers in {time.time() - t}s')

    def start_workers(self, execution_graph: ExecutionGraph):
        logger.info(f'Starting workers...')
        # start source workers first
        f = []
        for w in execution_graph.get_source_workers():
            f.append(w.start_or_rollback.remote())

        t = time.time()
        ray.wait(f)
        logger.info(f'Started source workers in {time.time() - t}s')

        # start rest
        f = []
        for w in execution_graph.get_non_source_workers():
            f.append(w.start_or_rollback.remote())

        t = time.time()
        ray.wait(f)
        logger.info(f'Started non-source workers in {time.time() - t}s')

    def delete_workers(self, vertices: List[ExecutionVertex]):
        # close workers first
        workers = [v.worker for v in vertices]

        # wait for actors to properly close
        timeout=5
        closed_finished_refs, closed_pending_refs = ray.wait(
            [w.close.remote() for w in workers],
            timeout=timeout,
            num_returns=len(workers)
        )
        if len(closed_finished_refs) == len(workers):
            logger.info('All workers closed gracefully')
        else:
            logger.info(f'Timeout ({timeout}s) waiting for actors to close gracefully, {len(closed_pending_refs)} not ready')

        for w in workers:
            w.exit.remote()

    def _gen_port(self, node_id) -> int:
        while True:
            port = randint(VALID_PORT_RANGE[0], VALID_PORT_RANGE[1])
            if node_id not in self._used_ports:
                self._used_ports[node_id] = [port]
                break
            else:
                if len(self._used_ports[node_id]) == VALID_PORT_RANGE[1] - VALID_PORT_RANGE[0]:
                    raise RuntimeError('Too many open ports')
                if port not in self._used_ports[node_id]:
                    self._used_ports[node_id].append(port)
                    break

        return port
