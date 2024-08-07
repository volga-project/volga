from typing import List, Optional

import msgpack

from volga.streaming.runtime.network_rust.channel import Channel, ChannelMessage
from volga_rust import RustDataReader

from volga.streaming.runtime.network_rust.io_loop import IOHandler, RustIOHandler


class DataReader(IOHandler):

    def __init__(
        self,
        name: str,
        job_name: str,
        channels: List[Channel]
    ):
        super().__init__(name, job_name, channels)
        self._rust_data_reader = RustDataReader(name, job_name, self._rust_channels)

    def get_rust_io_handler(self) -> RustIOHandler:
        return self._rust_data_reader

    def read_message(self) -> Optional[List[ChannelMessage]]:
        b = self._rust_data_reader.read_bytes()
        if b is None:
            return None
        return msgpack.loads(b)

    def start(self):
        super().start()
        self._rust_data_reader.start()

    def close(self):
        super().close()
        self._rust_data_reader.close()
