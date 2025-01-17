import logging
import sys
import time
from typing import List, Tuple, Dict

import ray
from ray.actor import ActorHandle
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
from volga.streaming.api.job_graph.job_graph import VertexType
from volga.streaming.api.operators.chained import ChainedOperator
from volga.streaming.api.operators.operators import SinkOperator
from volga.streaming.runtime.config.streaming_config import StreamingWorkerConfig

from volga.streaming.runtime.core.execution_graph.execution_graph import ExecutionGraph, ExecutionVertex
from volga.streaming.runtime.master.resource_manager.node_assign_strategy import NodeAssignStrategy
from volga.streaming.runtime.master.resource_manager.resource_manager import ResourceManager
from volga.stats.stats_manager import StatsManager
from volga.streaming.runtime.master.transfer_controller import TransferController
from volga.streaming.runtime.network.channel import LocalChannel, gen_ipc_addr, RemoteChannel
from volga.streaming.runtime.worker.job_worker import JobWorker, WorkerNodeInfo, VALID_PORT_RANGE


logger = logging.getLogger("ray")


class WorkerLifecycleController:

    def __init__(
        self,
        job_master: ActorHandle,
        resource_manager: ResourceManager,
        stats_manager: StatsManager,
        node_assign_strategy: NodeAssignStrategy,
        worker_config: StreamingWorkerConfig
    ):
        self.job_master = job_master
        self.resource_manager = resource_manager
        self.stats_manager = stats_manager
        self.node_assign_strategy = node_assign_strategy
        self._reserved_ports = {}
        self._na_ports_per_node = {}
        self.worker_config = worker_config
        self.transfer_controller = TransferController()

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
                'max_concurrency': 999,
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
            worker = JobWorker.options(**options_kwargs).remote(job_master=self.job_master, worker_config=self.worker_config)
            vertex_ids.append(vertex_id)
            workers[vertex_id] = worker
            vertex.set_worker(worker)

            if isinstance(vertex.job_vertex.stream_operator, ChainedOperator):
                # chained operator with sink case
                is_sink = isinstance(vertex.job_vertex.stream_operator.tail_operator, SinkOperator)
            else:
                is_sink = vertex.job_vertex.vertex_type == VertexType.SINK

            if is_sink or vertex.job_vertex.vertex_type == VertexType.SOURCE:
                self.stats_manager.register_target(worker)

        worker_hosts_infos = ray.get([workers[vertex_id].get_host_info.remote() for vertex_id in vertex_ids])
        for i in range(len(vertex_ids)):
            vertex_id = vertex_ids[i]
            node_info: WorkerNodeInfo = worker_hosts_infos[i]
            self._na_ports_per_node[node_info.node_id] = node_info.na_ports
            vertex = execution_graph.execution_vertices_by_id[vertex_id]
            vertex.set_worker_node_info(node_info)

        logger.info(f'Created {len(workers)} workers')
        logger.info(f'Workers node info: {worker_hosts_infos}')

    # construct channels based on Ray assigned actor IPs and update execution_graph
    def connect_and_init_workers(self, execution_graph: ExecutionGraph):
        sys.setrecursionlimit(10000)
        logger.info(f'Initing {len(execution_graph.execution_vertices_by_id)} workers...')
        job_name = execution_graph.job_name

        # in and out remote channels per each node, needed for transfer actors
        remote_channels_per_node: Dict[str, Tuple[List[RemoteChannel], List[RemoteChannel]]] = {}

        # create channels
        for edge in execution_graph.execution_edges:
            source_worker_network_info: WorkerNodeInfo = edge.source_execution_vertex.worker_node_info
            target_worker_network_info: WorkerNodeInfo = edge.target_execution_vertex.worker_node_info
            if source_worker_network_info is None or target_worker_network_info is None:
                raise RuntimeError(f'No worker network info')

            if source_worker_network_info.node_id == target_worker_network_info.node_id:
                channel = LocalChannel(
                    channel_id=edge.id,
                    ipc_addr=gen_ipc_addr(job_name, f'local-{edge.target_execution_vertex.execution_vertex_id}'),
                )
            else:
                source_node_id = source_worker_network_info.node_id
                target_node_id = target_worker_network_info.node_id

                source_ipc_unique_key = f'source-{source_node_id}'# in remote setting all data writers write to one port, hence key per node
                target_ipc_unique_key = f'target-{edge.target_execution_vertex.execution_vertex_id}'

                channel = RemoteChannel(
                    channel_id=edge.id,
                    source_local_ipc_addr=gen_ipc_addr(job_name, source_ipc_unique_key),
                    source_node_ip=source_worker_network_info.node_ip,
                    source_node_id=source_node_id,
                    target_local_ipc_addr=gen_ipc_addr(job_name, target_ipc_unique_key),
                    target_node_ip=target_worker_network_info.node_ip,
                    target_node_id=target_node_id,
                    port=self._gen_port(target_node_id),
                )
                if source_node_id not in remote_channels_per_node:
                    remote_channels_per_node[source_node_id] = ([], [])

                remote_channels_per_node[source_node_id][1].append(channel)

                if target_node_id not in remote_channels_per_node:
                    remote_channels_per_node[target_node_id] = ([], [])

                remote_channels_per_node[target_node_id][0].append(channel)

            edge.set_channel(channel)

        # init transfer actors if needed
        self.transfer_controller.create_transfer_actors(job_name, remote_channels_per_node)

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
        t = time.time()
        futs = {}
        for node_id in self.transfer_controller.transfer_actors:
            actor, name = self.transfer_controller.transfer_actors[node_id]
            futs[actor.start.remote()] = name

        for vertex in execution_graph.execution_vertices_by_id.values():
            actor = vertex.worker
            name = vertex.execution_vertex_id
            futs[actor.start_or_rollback.remote()] = name

        errs = ray.get(list(futs.keys()))
        has_err = False
        big_err = "Unable to start workers:"
        for i in range(len(futs.keys())):
            err = errs[i]
            name = list(futs.values())[i]
            if err is not None:
                has_err = True
                big_err += f"\n[{name}]: {err}"
        if has_err:
            raise RuntimeError(big_err)

        logger.info(f'Started workers in {time.time() - t}s')

    def delete_workers(self, vertices: List[ExecutionVertex]):
        # close workers first
        workers = [v.worker for v in vertices]

        # wait for actors to properly close
        timeout = 5 # TODO config this
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

    def _gen_port(self, node_id: str) -> int:
        if node_id in self._reserved_ports:
            return self._reserved_ports[node_id]

        # gen first available port in range which is not in na ports
        for port in range(VALID_PORT_RANGE[0], VALID_PORT_RANGE[-1] + 1):
            if not port in self._na_ports_per_node[node_id]:
                return port

        raise RuntimeError(f'Unable to generate port for {node_id}')

