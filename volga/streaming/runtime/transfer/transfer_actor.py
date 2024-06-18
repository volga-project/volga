from typing import List

import ray
import zmq

from volga.streaming.runtime.transfer.channel import LocalChannel, RemoteChannel
from volga.streaming.runtime.transfer.v2.transfer_receiver import TransferReceiver
from volga.streaming.runtime.transfer.v2.transfer_sender import AsyncTransferSender


@ray.remote
class TransferActor:

    def __init__(
        self,
        remote_in_channels: List[RemoteChannel],
        remote_out_channels: List[RemoteChannel],
        host_node_id,
    ):
        self._zmq_ctx = zmq.Context()
        self._sender = AsyncTransferSender(
            remote_out_channels=remote_out_channels,
            host_node_id=host_node_id,
        )

        self._receiver = TransferReceiver(
            remote_in_channels=remote_in_channels,
            host_node_id=host_node_id,
        )

    def start(self):
        self._receiver.start()
        self._sender.start()

    def stop(self):
        self._receiver.stop()
        self._sender.stop()
        # TODO terminate zmq ctx
