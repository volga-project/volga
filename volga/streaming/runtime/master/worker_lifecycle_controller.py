import logging
import time
from random import randint
from typing import List

import ray
from ray.actor import ActorHandle
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from volga.streaming.runtime.core.execution_graph.execution_graph import ExecutionGraph, ExecutionVertex
from volga.streaming.runtime.master.resource_manager.node_assign_strategy import NodeAssignStrategy
from volga.streaming.runtime.master.resource_manager.resource_manager import ResourceManager
from volga.streaming.runtime.network.channel import LocalChannel, gen_ipc_addr, RemoteChannel
from volga.streaming.runtime.worker.job_worker import JobWorker

VALID_PORT_RANGE = (30000, 65000)


# logger = logging.getLogger(__name__)
logger = logging.getLogger("ray")


class WorkerNodeInfo:

    def __init__(self, node_ip: str, node_id: str):
        self.node_ip = node_ip
        self.node_id = node_id


class WorkerLifecycleController:

    def __init__(
        self,
        job_master: ActorHandle,
        resource_manager: ResourceManager,
        node_assign_strategy: NodeAssignStrategy
    ):
        self.job_master = job_master
        self.resource_manager = resource_manager
        self.node_assign_strategy = node_assign_strategy
        self._reserved_node_ports = {}

    def create_workers(self, execution_graph: ExecutionGraph):
        workers = {}
        vertex_ids = []
        logger.info(f'Assigning workers to nodes...')
        node_assignment = self.resource_manager.assign_resources(execution_graph, self.node_assign_strategy)

        vertices_per_node = {}
        for vertex_id in node_assignment:
            node = node_assignment[vertex_id]
            if node.node_name in vertices_per_node:
                vertices_per_node[node.node_name].append(vertex_id)
            else:
                vertices_per_node[node.node_name] = [vertex_id]

        logger.info(f'Assigned workers to nodes: {vertices_per_node}')
        logger.info(f'Creating {len(execution_graph.execution_vertices_by_id)} workers...')
        for vertex_id in execution_graph.execution_vertices_by_id:
            vertex = execution_graph.execution_vertices_by_id[vertex_id]
            if vertex_id not in node_assignment:
                raise RuntimeError(f'Can not find vertex_id {vertex_id} in node assignment {node_assignment}')
            host_node = node_assignment[vertex_id]
            resources = vertex.resources

            # set consistent seed for hash function for all workers
            worker_runtime_env = {
                'env_vars': {'PYTHONHASHSEED': '0'}
            }
            options_kwargs = {
                'max_restarts': -1,
                'max_concurrency': 10,
                'runtime_env': worker_runtime_env,
                'scheduling_strategy': NodeAffinitySchedulingStrategy(
                    node_id=host_node.node_id,
                    soft=False
                )
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
            ni = WorkerNodeInfo(
                node_ip=node_ip,
                node_id=node_id,
            )
            vertex.set_worker_node_info(ni)
            worker_infos.append((vertex_id, ni.node_id, ni.node_ip))

        logger.info(f'Created {len(workers)} workers')
        logger.info(f'Workers writer node info: {worker_infos}')

    # construct channels based on Ray assigned actor IPs and update execution_graph
    def connect_and_init_workers(self, execution_graph: ExecutionGraph):
        logger.info(f'Initing {len(execution_graph.execution_vertices_by_id)} workers...')
        job_name = execution_graph.job_name
        # create channels
        for edge in execution_graph.execution_edges:
            source_worker_network_info: WorkerNodeInfo = edge.source_execution_vertex.worker_node_info
            target_worker_network_info: WorkerNodeInfo = edge.target_execution_vertex.worker_node_info
            if source_worker_network_info is None or target_worker_network_info is None:
                raise RuntimeError(f'No worker network info')

            if source_worker_network_info.node_id == target_worker_network_info.node_id:
                channel = LocalChannel(
                    channel_id=edge.id,
                    ipc_addr=gen_ipc_addr(job_name=job_name, channel_id=edge.id),
                )
            else:
                # unique ports per node-node connection
                port = self._gen_port(
                    key=f'{source_worker_network_info.node_id}-{target_worker_network_info.node_id}'
                )
                channel = RemoteChannel(
                    channel_id=edge.id,
                    source_local_ipc_addr=gen_ipc_addr(job_name, edge.id, source_worker_network_info.node_id),
                    source_node_ip=source_worker_network_info.node_ip,
                    source_node_id=source_worker_network_info.node_id,
                    target_local_ipc_addr=gen_ipc_addr(job_name, edge.id, target_worker_network_info.node_id),
                    target_node_ip=target_worker_network_info.node_ip,
                    target_node_id=target_worker_network_info.node_id,
                    port=port,
                )

            edge.set_channel(channel)

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
        timeout = 5
        refs = [w.close.remote() for w in workers]
        closed_finished_refs, closed_pending_refs = ray.wait(
            refs,
            timeout=timeout,
            num_returns=len(workers)
        )
        if len(closed_finished_refs) == len(workers):
            logger.info('All workers closed gracefully')
        else:
            pending_vertex_indices = [i for i in range(len(refs)) if refs[i] in closed_pending_refs]
            pending_vertices = [vertices[i] for i in pending_vertex_indices]
            logger.info(f'Timeout ({timeout}s) waiting for actors to close gracefully, {len(closed_pending_refs)} not ready: {pending_vertices}')

        for w in workers:
            w.exit.remote()

    def _gen_port(self, key: str) -> int:
        if key not in self._reserved_node_ports:
            port = randint(VALID_PORT_RANGE[0], VALID_PORT_RANGE[1])
            self._reserved_node_ports[key] = port
            return port
        else:
            return self._reserved_node_ports[key]

