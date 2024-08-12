from typing import List

from volga.streaming.runtime.network.channel import Channel
from volga.streaming.runtime.network.io_loop import IOHandler, RustIOHandler

from volga_rust import RustTransferReceiver, RustTransferSender


class TransferSender(IOHandler):
    def __init__(
        self,
        name: str,
        job_name: str,
        channels: List[Channel]
    ):
        super().__init__(name, job_name, channels)
        self._rust_transfer_sender = RustTransferSender(name, job_name, self._rust_channels)

    def start(self):
        super().start()
        self._rust_transfer_sender.start()

    def close(self):
        super().close()
        self._rust_transfer_sender.close()

    def get_rust_io_handler(self) -> RustIOHandler:
        return self._rust_transfer_sender


class TransferReceiver(IOHandler):

    def __init__(
        self,
        name: str,
        job_name: str,
        channels: List[Channel]
    ):
        super().__init__(name, job_name, channels)
        self._rust_transfer_receiver = RustTransferReceiver(name, job_name, self._rust_channels)

    def start(self):
        super().start()
        self._rust_transfer_receiver.start()

    def close(self):
        super().close()
        self._rust_transfer_receiver.close()

    def get_rust_io_handler(self) -> RustIOHandler:
        return self._rust_transfer_receiver

