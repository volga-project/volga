import logging
import time
import random
from typing import List, Tuple, Dict

import ray
from ray.actor import ActorHandle
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
from volga.streaming.api.job_graph.job_graph import VertexType
from volga.streaming.api.operators.chained import ChainedOperator
from volga.streaming.api.operators.operators import SinkOperator

from volga.streaming.runtime.core.execution_graph.execution_graph import ExecutionGraph, ExecutionVertex
from volga.streaming.runtime.master.resource_manager.node_assign_strategy import NodeAssignStrategy
from volga.streaming.runtime.master.resource_manager.resource_manager import ResourceManager
from volga.streaming.runtime.master.stats.stats_manager import StatsManager
from volga.streaming.runtime.master.transfer_controller import TransferController
from volga.streaming.runtime.network.channel import LocalChannel, gen_ipc_addr, RemoteChannel
from volga.streaming.runtime.worker.job_worker import JobWorker


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
        stats_manager: StatsManager,
        node_assign_strategy: NodeAssignStrategy
    ):
        self.job_master = job_master
        self.resource_manager = resource_manager
        self.stats_manager = stats_manager
        self.node_assign_strategy = node_assign_strategy
        self._reserved_node_ports = {}
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

            if isinstance(vertex.job_vertex.stream_operator, ChainedOperator):
                # chained operator with sink case
                is_sink = isinstance(vertex.job_vertex.stream_operator.tail_operator, SinkOperator)
            else:
                is_sink = vertex.job_vertex.vertex_type == VertexType.SINK

            if is_sink or vertex.job_vertex.vertex_type == VertexType.SOURCE:
                self.stats_manager.register_worker(worker)

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
        logger.info(f'Workers node info: {worker_infos}')

    # construct channels based on Ray assigned actor IPs and update execution_graph
    def connect_and_init_workers(self, execution_graph: ExecutionGraph):
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
                    ipc_addr=gen_ipc_addr(job_name=job_name, channel_id=edge.id),
                )
            else:
                # unique ports per node-node connection
                source_node_id = source_worker_network_info.node_id
                target_node_id = target_worker_network_info.node_id
                port = self.gen_port(f'{source_node_id}-{target_node_id}', target_node_id, self._reserved_node_ports)

                channel = RemoteChannel(
                    channel_id=edge.id,
                    source_local_ipc_addr=gen_ipc_addr(job_name, edge.id, source_node_id),
                    source_node_ip=source_worker_network_info.node_ip,
                    source_node_id=source_node_id,
                    target_local_ipc_addr=gen_ipc_addr(job_name, edge.id, target_node_id),
                    target_node_ip=target_worker_network_info.node_ip,
                    target_node_id=target_node_id,
                    port=port,
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

    # TODO fix this
    @staticmethod
    def gen_port(conn_id: str, node_id: str, reserved_node_ports: Dict[str, Tuple[int, str]]) -> int:
        return random.randint(50000, 60000)
        # port_pool_per_node = [*range(10000, 20000)] # TODO config this
        # if conn_id not in reserved_node_ports:
        #     port = None
        #     # gen next from pool
        #     for _port in port_pool_per_node:
        #         # scan all reserved_node_ports to see if it is used for this node_id
        #         used = False
        #         for _conn_id in reserved_node_ports:
        #             _node_id = reserved_node_ports[_conn_id][1]
        #             _reserved_port = reserved_node_ports[_conn_id][0]
        #             if node_id == _node_id and _reserved_port == _port:
        #                 used = True
        #                 break
        #         if not used:
        #             port = _port
        #             break
        #     if port is None:
        #         raise RuntimeError(f'Port pool is too small for node {node_id}, all used')
        #
        #     reserved_node_ports[conn_id] = (port, node_id)
        #     return port
        # else:
        #     return reserved_node_ports[conn_id][0]

