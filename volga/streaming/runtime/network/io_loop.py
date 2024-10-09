from abc import ABC, abstractmethod
from typing import List, Union, Optional

from volga.streaming.runtime.network.channel import Channel
from volga.streaming.runtime.network.metrics import MetricsRecorder
from volga.streaming.runtime.network.network_config import ZmqConfig, DEFAULT_ZMQ_CONFIG

from volga_rust import RustIOLoop, RustDataWriter, RustDataReader, RustTransferSender, RustTransferReceiver

RustIOHandler = Union[RustDataWriter, RustDataReader, RustTransferSender, RustTransferReceiver]


class IOHandler(ABC):

    def __init__(
        self,
        name: str,
        job_name: str,
        channels: List[Channel],
    ):
        self.name = name
        self.job_name = job_name
        self._rust_channels = [channel.to_rust_channel() for channel in channels]
        self._metrics_recorder = MetricsRecorder(name, job_name)

    @abstractmethod
    def start(self):
        # pass
        self._metrics_recorder.start()

    @abstractmethod
    def close(self):
        # pass
        self._metrics_recorder.close()

    @abstractmethod
    def get_rust_io_handler(self) -> RustIOHandler:
        raise NotImplementedError()


class IOLoop:

    def __init__(
        self,
        name: str,
        config: Optional[ZmqConfig] = DEFAULT_ZMQ_CONFIG
    ):
        self.name = name
        rust_config = None if config is None else config.to_rust()
        self._rust_io_loop = RustIOLoop(name, rust_config)
        self._handlers: List[IOHandler] = []

    def register_io_handler(self, handler: IOHandler):
        self._handlers.append(handler)
        rust_io_handler = handler.get_rust_io_handler()
        if isinstance(rust_io_handler, RustDataWriter):
            self._rust_io_loop.register_data_writer(rust_io_handler)
        elif isinstance(rust_io_handler, RustDataReader):
            self._rust_io_loop.register_data_reader(rust_io_handler)
        elif isinstance(rust_io_handler, RustTransferSender):
            self._rust_io_loop.register_transfer_sender(rust_io_handler)
        elif isinstance(rust_io_handler, RustTransferReceiver):
            self._rust_io_loop.register_transfer_receiver(rust_io_handler)

    def connect_and_start(self, num_threads: int = 1, timeout_ms: int = 30000) -> Optional[str]:
        for handler in self._handlers:
            handler.start()
        res = self._rust_io_loop.connect(num_threads, timeout_ms)
        if res is None:
            self._rust_io_loop.start()
        return res

    def close(self):
        for handler in self._handlers:
            handler.close()
        self._rust_io_loop.close()
