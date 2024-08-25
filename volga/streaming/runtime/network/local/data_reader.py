import time
from typing import List, Optional

import msgpack

from volga.streaming.runtime.network.channel import Channel, ChannelMessage
from volga.streaming.runtime.network.network_config import DataReaderConfig, DEFAULT_DATA_READER_CONFIG
from volga_rust import RustDataReader

from volga.streaming.runtime.network.io_loop import IOHandler, RustIOHandler


class DataReader(IOHandler):

    def __init__(
        self,
        name: str,
        job_name: str,
        channels: List[Channel],
        config: DataReaderConfig = DEFAULT_DATA_READER_CONFIG
    ):
        super().__init__(name, job_name, channels)
        self._rust_data_reader = RustDataReader(name, job_name, config.to_rust(), self._rust_channels)

        self._num_msgs_read = 0
        self._last_report_ts = time.time()
        self._start_ts = None

    def get_rust_io_handler(self) -> RustIOHandler:
        return self._rust_data_reader

    def read_message(self) -> Optional[List[ChannelMessage]]:
        b = self._rust_data_reader.read_bytes()
        if b is None:
            return None
        res = msgpack.loads(b)
        self._num_msgs_read += len(res)
        if time.time() - self._last_report_ts > 1:
            rx = self._num_msgs_read / (time.time() - self._start_ts)
            print(f'[{self.name}] Recvd {self._num_msgs_read} msgs, rx: {rx} msg/s')
            self._last_report_ts = time.time()
        return res

    def start(self):
        self._start_ts = time.time()
        super().start()
        self._rust_data_reader.start()

    def close(self):
        super().close()
        self._rust_data_reader.close()
