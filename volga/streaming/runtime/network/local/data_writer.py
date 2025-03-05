import threading
import time
from typing import List

import msgpack

from volga.streaming.api.message.message import Record
from volga.streaming.runtime.network.channel import Channel, ChannelMessage
from volga.streaming.runtime.network.network_config import DEFAULT_DATA_WRITER_CONFIG, DataWriterConfig
from volga_rust import RustDataWriter

from volga.streaming.runtime.network.io_loop import IOHandler, RustIOHandler
from volga.streaming.runtime.network.metrics import MetricsRecorder
from volga.streaming.runtime.network.serializer import Serializer, DEFAULT_SERIALIZER


class DataWriter(IOHandler):

    def __init__(
        self,
        handler_id: str,
        name: str,
        source_stream_name: str,
        job_name: str,
        channels: List[Channel],
        config: DataWriterConfig = DEFAULT_DATA_WRITER_CONFIG,
        serializer: Serializer = DEFAULT_SERIALIZER
    ):
        super().__init__(handler_id, name, job_name, channels)
        self._rust_data_writer = RustDataWriter(handler_id, name, job_name, config.to_rust(), self._rust_channels)
        self._source_stream_name = source_stream_name
        self._batch_per_channel = {channel.channel_id: [] for channel in channels}
        self._lock_per_channel = {channel.channel_id: threading.Lock() for channel in channels}
        self._batch_size = config.batch_size
        self._flusher_thread = threading.Thread(target=self._flusher_loop)
        self._flush_period_s = config.flush_period_s
        self._serializer = serializer
        self.running = False
        self._metrics_recorder = MetricsRecorder(name, job_name)

        # TODO reporting should be per channel?
        self._num_msgs_sent = 0
        self._last_report_ts = time.time()
        self._last_sent_size = None
        self._start_ts = None

    def get_rust_io_handler(self) -> RustIOHandler:
        return self._rust_data_writer

    def try_write_record(self, channel_id: str, record: Record) -> bool:
        # add sender operator_id
        record.set_stream_name(self._source_stream_name)
        message = record.to_channel_message()
        return self.try_write_message(channel_id, message)

    def try_write_message(self, channel_id: str, message: ChannelMessage) -> bool:
        res = self._try_write_message(channel_id, message)
        if res:
            # TODO reporting should be per channel?
            self._num_msgs_sent += 1
            if time.time() - self._last_report_ts > 1:
                tx = self._num_msgs_sent/(time.time() - self._start_ts)
                # print(f'[{self.name}] Sent {self._num_msgs_sent} msgs, tx {tx} msg/s, last sent size: {self._last_sent_size}')
                self._last_report_ts = time.time()

        return res

    def _try_write_message(self, channel_id: str, message: ChannelMessage) -> bool:
        lock = self._lock_per_channel[channel_id]
        lock.acquire()
        batch = self._batch_per_channel[channel_id]
        batch.append(message)
        if len(batch) == self._batch_size:
            b = self._serializer.dumps(batch)
            # TODO indicate unsuccessful write (e.g. backpressure)?
            res = self._rust_data_writer.write_bytes(channel_id, b, 100)
            if res is None:
                batch.pop()
                lock.release()
                return False
            else:
                self._batch_per_channel[channel_id] = []
                lock.release()
                self._last_sent_size = int(len(b)/len(batch))
                return True
        else:
            lock.release()
            return True

    def try_flush_if_needed(self):
        for channel_id in self._lock_per_channel:
            lock = self._lock_per_channel[channel_id]
            lock.acquire()
            batch = self._batch_per_channel[channel_id]
            if len(batch) == 0:
                lock.release()
                continue
            b = self._serializer.dumps(batch)
            # TODO indicate unsuccessful write (e.g. backpressure)?
            res = self._rust_data_writer.write_bytes(channel_id, b, 0)
            if res is not None:
                self._batch_per_channel[channel_id] = []
            lock.release()

    def _flusher_loop(self):
        while self.running:
            self.try_flush_if_needed()
            time.sleep(self._flush_period_s)

    def start(self):
        self._start_ts = time.time()
        super().start()
        self.running = True
        self._rust_data_writer.start()
        self._flusher_thread.start()

    def stop(self):
        super().stop()
        self.running = False
        self._flusher_thread.join(5)
        self.try_flush_if_needed()
        self._rust_data_writer.stop()
