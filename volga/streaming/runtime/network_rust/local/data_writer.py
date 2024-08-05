import threading
from typing import List

import msgpack

from volga.streaming.api.message.message import Record
from volga.streaming.runtime.network_rust.channel import Channel, ChannelMessage
from volga_rust import RustDataReader, RustDataWriter, RustIOLoop

from volga.streaming.runtime.network_rust.metrics import MetricsRecorder

ENABLE_LOCKING = True
FLUSH_TIMEOUT_S = 0.1 # TODO this should be in a config

class DataWriter:

    def __init__(
        self,
        name: str,
        source_stream_name: str,
        job_name: str,
        channels: List[Channel]
    ):
        rust_channels = [channel.to_rust_channel() for channel in channels]
        self._rust_data_writer = RustDataWriter(name, job_name, rust_channels)
        self._source_stream_name = source_stream_name
        self._batch_per_channel = {channel.channel_id: [] for channel in channels}
        self._lock_per_channel = {channel.channel_id: threading.Lock() for channel in channels}
        self._last_update_ts_per_channel = {channel.channel_id: -1 for channel in channels}
        self._batch_size = 1000 # TODO config this
        self._flusher_thread = None
        self.running = False
        self._metrics_recorder = MetricsRecorder(name, job_name)

    def write_record(self, channel_id: str, record: Record) -> bool:
        # add sender operator_id
        record.set_stream_name(self._source_stream_name)
        message = record.to_channel_message()
        return self._write_message(channel_id, message)

    def _write_message(self, channel_id: str, message: ChannelMessage) -> bool:
        lock = self._lock_per_channel[channel_id]
        lock.acquire()
        batch = self._batch_per_channel[channel_id]
        batch.append(message)
        if len(batch) == self._batch_size:
            b = msgpack.dumps(batch)
            res = self._rust_data_writer.write_bytes(b, 1000, 1)

        return True

    def start(self):
        self.running = True
        self._rust_data_writer.start()
        self._metrics_recorder.start()

    def close(self):
        self._rust_data_writer.close()
        self._metrics_recorder.close()
