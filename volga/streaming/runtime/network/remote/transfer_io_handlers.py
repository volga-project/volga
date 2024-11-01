from typing import List

from volga.streaming.runtime.network.channel import Channel
from volga.streaming.runtime.network.io_loop import IOHandler, RustIOHandler
from volga.streaming.runtime.network.network_config import DEFAULT_TRANSFER_CONFIG, TransferConfig

from volga_rust import RustTransferReceiver, RustTransferSender


class TransferSender(IOHandler):
    def __init__(
        self,
        handler_id: str,
        name: str,
        job_name: str,
        channels: List[Channel],
        config: TransferConfig = DEFAULT_TRANSFER_CONFIG
    ):
        super().__init__(handler_id, name, job_name, channels)
        self._rust_transfer_sender = RustTransferSender(handler_id, name, job_name, config.to_rust(), self._rust_channels)

    def start(self):
        super().start()
        self._rust_transfer_sender.start()

    def stop(self):
        super().stop()
        self._rust_transfer_sender.stop()

    def get_rust_io_handler(self) -> RustIOHandler:
        return self._rust_transfer_sender


class TransferReceiver(IOHandler):

    def __init__(
        self,
        handler_id: str,
        name: str,
        job_name: str,
        channels: List[Channel],
        config: TransferConfig = DEFAULT_TRANSFER_CONFIG
    ):
        super().__init__(handler_id, name, job_name, channels)
        self._rust_transfer_receiver = RustTransferReceiver(handler_id, name, job_name, config.to_rust(), self._rust_channels)

    def start(self):
        super().start()
        self._rust_transfer_receiver.start()

    def stop(self):
        super().stop()
        self._rust_transfer_receiver.stop()

    def get_rust_io_handler(self) -> RustIOHandler:
        return self._rust_transfer_receiver

