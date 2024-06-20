from typing import List

import ray
import zmq

from volga.streaming.runtime.network.channel import RemoteChannel


@ray.remote
class TransferActor:

    def __init__(
        self,
        remote_in_channels: List[RemoteChannel],
        remote_out_channels: List[RemoteChannel],
        host_node_id,
    ):
        self._zmq_ctx = zmq.Context()
        self._sender = None

        self._receiver = None

    def start(self):
        self._receiver.start()
        self._sender.start()

    def stop(self):
        self._receiver.stop()
        self._sender.stop()
        # TODO terminate zmq ctx
