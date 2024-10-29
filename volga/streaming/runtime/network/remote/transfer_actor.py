import os
from typing import List, Optional, Tuple, Any

import ray

from volga.streaming.runtime.network.channel import RemoteChannel
from volga.streaming.runtime.network.io_loop import IOLoop
from volga.streaming.runtime.network.remote.transfer_io_handlers import TransferReceiver, TransferSender


@ray.remote
class TransferActor:

    def __init__(
        self,
        job_name: str,
        name: str,
        sender_id: Optional[str] = None,
        receiver_id: Optional[str] = None,
        in_channels: Optional[List[RemoteChannel]] = None,
        out_channels: Optional[List[RemoteChannel]] = None,
    ):
        if sender_id is None and receiver_id is None:
            raise ValueError('Transfer actor should have at least one of sender_id or receiver_id')

        if in_channels is None and out_channels is None:
            raise ValueError('Transfer actor should have at least one of in_channels or out_channels')
        self.name = name
        self._loop = IOLoop(f'io-loop-{name}')
        if out_channels is not None and len(out_channels) != 0:
            assert sender_id is not None
            self._sender = TransferSender(
                handler_id=sender_id,
                job_name=job_name,
                name=f'{name}-sender',
                channels=out_channels
            )
            self._loop.register_io_handler(self._sender)
        else:
            # sink node
            self._sender = None

        if in_channels is not None and len(in_channels) != 0:
            assert receiver_id is not None
            self._receiver = TransferReceiver(
                handler_id=receiver_id,
                job_name=job_name,
                name=f'{name}-receiver',
                channels=in_channels
            )
            self._loop.register_io_handler(self._receiver)
        else:
            # source node
            self._receiver = None

    def get_pid(self):
        return os.getpid()

    def get_name(self):
        return self.name

    def start(self) -> Optional[str]:
        return self._loop.connect_and_start()

    def stop(self):
        self._loop.stop()
