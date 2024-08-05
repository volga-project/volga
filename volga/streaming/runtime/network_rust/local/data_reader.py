from typing import List

import msgpack

from volga.streaming.api.message.message import Record
from volga.streaming.runtime.network_rust.channel import Channel, ChannelMessage
from volga_rust import RustDataReader

from volga.streaming.runtime.network_rust.metrics import MetricsRecorder


class DataReader:

    def __init__(
        self,
        name: str,
        job_name: str,
        channels: List[Channel]
    ):
        rust_channels = [channel.to_rust_channel() for channel in channels]
        self._rust_data_reader = RustDataReader(name, job_name, rust_channels)
        self._metrics_recorder = MetricsRecorder(name, job_name)

    def read_message(self) -> List[ChannelMessage]:
        b = self._rust_data_reader.read_bytes()
        return msgpack.loads(b)

    def start(self):
        self._rust_data_reader.start()
        self._metrics_recorder.start()

    def close(self):
        self._rust_data_reader.close()
        self._metrics_recorder.close()
