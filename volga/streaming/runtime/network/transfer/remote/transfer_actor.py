import os
from typing import List, Optional, Tuple, Any

import ray
import zmq

from volga.streaming.runtime.network.channel import RemoteChannel
from volga.streaming.runtime.network.config import NetworkConfig, DEFAULT_NETWORK_CONFIG
from volga.streaming.runtime.network.stats import Stats
from volga.streaming.runtime.network.transfer.io_loop import IOLoop
from volga.streaming.runtime.network.transfer.remote.remote_transfer_handler import TransferSender, TransferReceiver


@ray.remote
class TransferActor:

    def __init__(
        self,
        job_name: str,
        name: str,
        in_channels: Optional[List[RemoteChannel]],
        out_channels: Optional[List[RemoteChannel]],
        network_config: NetworkConfig = DEFAULT_NETWORK_CONFIG
    ):
        if in_channels is None and out_channels is None:
            raise ValueError('Transfer actor should have at least one of in_channels or out_channels')
        self._zmq_ctx = zmq.Context()
        self._loop = IOLoop()
        self._zmq_ctx = zmq.Context.instance()
        if out_channels is not None:
            self._sender = TransferSender(
                job_name=job_name,
                name=name,
                channels=out_channels,
                zmq_ctx=self._zmq_ctx,
                network_config=network_config
            )
            self._loop.register(self._sender)
        else:
            # sink node
            self._sender = None

        if in_channels is not None:
            self._receiver = TransferReceiver(
                job_name=job_name,
                name=name,
                channels=in_channels,
                zmq_ctx=self._zmq_ctx,
                network_config=network_config
            )
            self._loop.register(self._receiver)
        else:
            # source node
            self._receiver = None

    def get_stats(self) -> Tuple[Optional[Stats], Optional[Stats]]:
        sender_stats = self._sender.stats if self._sender is not None else None
        receiver_stats = self._receiver.stats if self._receiver is not None else None
        return sender_stats, receiver_stats

    def get_pid(self):
        return os.getpid()

    def start(self) -> Tuple[bool, Any]:
        return self._loop.start()

    def stop(self):
        self._loop.close()
        # TODO terminate zmq ctx
