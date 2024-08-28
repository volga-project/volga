from typing import List, Dict, Tuple

from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from volga.streaming.runtime.network.channel import RemoteChannel
from volga.streaming.runtime.network.remote.transfer_actor import TransferActor
import ray


# TODO add watch/restart functionality
class TransferController:

    def __init__(self):
        self.transfer_actors = {}

    def create_transfer_actors(
        self,
        job_name: str,
        remote_channels_per_node: Dict[str, Tuple[List[RemoteChannel], List[RemoteChannel]]]
    ):
        if len(remote_channels_per_node) == 0:
            return

        for node_id in remote_channels_per_node:
            if node_id in self.transfer_actors:
                raise RuntimeError(f'Duplicate node_id {node_id} during transfer actors init')

            in_channels = remote_channels_per_node[node_id][0]
            out_channels = remote_channels_per_node[node_id][1]

            # check in_channels and out_channels properly point to the same node
            node_ip = None
            for ch in in_channels:
                if node_ip is None:
                    node_ip = ch.target_node_ip
                if node_id != ch.target_node_id or node_ip != ch.target_node_ip:
                    raise RuntimeError('Mismatched in channels for transfer actor')

            for ch in out_channels:
                if node_ip is None:
                    node_ip = ch.source_node_ip
                if node_id != ch.source_node_id or node_ip != ch.source_node_ip:
                    raise RuntimeError('Mismatched out channels for transfer actor')

            name = f'transfer-{node_ip}'

            options_kwargs = {
                'num_cpus': 0,
                'max_restarts': -1,
                'max_concurrency': 10,
                'scheduling_strategy': NodeAffinitySchedulingStrategy(
                    node_id=node_id,
                    soft=False
                )
            }

            transfer_actor = TransferActor.options(**options_kwargs).remote(job_name, name, in_channels, out_channels)

            self.transfer_actors[node_id] = transfer_actor

    def start_transfer_actors(self):
        f = []
        for node_id in self.transfer_actors:
            actor = self.transfer_actors[node_id]
            f.append(actor.start.remote())
        errs = ray.get(f)
        has_err = False
        big_err = "Unable to start transfer actors:"
        for err in errs:
            if err is not None:
                has_err = True
                big_err += f"\n{err}"
        if has_err:
            raise RuntimeError(big_err)
