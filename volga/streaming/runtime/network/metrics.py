import copy
import enum
import time
from threading import Thread, Lock
from typing import Tuple, Dict

from ray.util.metrics import Counter, Histogram


class Metric(enum.Enum):
    NUM_BUFFERS_SENT = 'volga_num_buffers_sent'
    NUM_BUFFERS_RCVD = 'volga_num_buffers_rcvd'
    NUM_BUFFERS_DELIVERED = 'volga_num_buffers_delivered'
    NUM_BUFFERS_RESENT = 'volga_num_buffers_resent'

    NUM_RECORDS_SENT = 'volga_num_records_sent'
    NUM_RECORDS_RCVD = 'volga_num_records_rcvd'
    NUM_RECORDS_DELIVERED = 'volga_num_records_delivered'

    LATENCY = 'volga_latency'


class MetricsRecorder:

    _instance = None

    FLUSH_PERIOD_S = 1
    METRIC_KEY_DELIMITER = ';'

    @staticmethod
    def instance(job_name: str) -> 'MetricsRecorder':
        if MetricsRecorder._instance is None:
            MetricsRecorder._instance = MetricsRecorder(job_name)
            MetricsRecorder._instance.start()
        return MetricsRecorder._instance

    def __init__(self, job_name: str):
        self.counters = {
            Metric.NUM_BUFFERS_SENT.value: Counter(
                Metric.NUM_BUFFERS_SENT.value,
                description='Number of buffers sent',
                tag_keys=('job_name', 'handler_type', 'handler_name', 'channel_id'),
            ),
            Metric.NUM_BUFFERS_RESENT.value: Counter(
                Metric.NUM_BUFFERS_RESENT.value,
                description='Number of buffers re-sent',
                tag_keys=('job_name', 'handler_type', 'handler_name', 'channel_id'),
            ),
            Metric.NUM_BUFFERS_RCVD.value: Counter(
                Metric.NUM_BUFFERS_RCVD.value,
                description='Number of buffers received',
                tag_keys=('job_name', 'handler_type', 'handler_name', 'channel_id'),
            ),
            Metric.NUM_BUFFERS_DELIVERED.value: Counter(
                Metric.NUM_BUFFERS_DELIVERED.value,
                description='Number of buffers confirmed delivered',
                tag_keys=('job_name', 'handler_type', 'handler_name', 'channel_id'),
            ),
            Metric.NUM_RECORDS_SENT.value: Counter(
                Metric.NUM_RECORDS_SENT.value,
                description='Number of records sent',
                tag_keys=('job_name', 'handler_type', 'handler_name', 'channel_id'),
            ),
            Metric.NUM_RECORDS_RCVD.value: Counter(
                Metric.NUM_RECORDS_RCVD.value,
                description='Number of records received',
                tag_keys=('job_name', 'handler_type', 'handler_name', 'channel_id'),
            ),
            Metric.NUM_RECORDS_DELIVERED.value: Counter(
                Metric.NUM_RECORDS_DELIVERED.value,
                description='Number of records confirmed delivered',
                tag_keys=('job_name', 'handler_type', 'handler_name', 'channel_id'),
            )
        }
        self.flusher_thread = Thread(target=self._flusher_loop)
        self.running = False

        for name in self.counters:
            self.counters[name].set_default_tags({'job_name': job_name})

        self.buffered_counters = {metric_name: {} for metric_name in self.counters}
        self.locks = {metric_name: Lock() for metric_name in self.counters}

        self.latency_hist = Histogram(
            Metric.LATENCY.value,
            description='Latency of roundtrip between two IOHandlers in ms',
            boundaries=[i + 1 for i in range(1000)],
            tag_keys=('job_name', 'handler_name', 'channel_id'),
        )
        self.latency_hist.set_default_tags({'job_name': job_name})

    def start(self):
        self.running = True
        self.flusher_thread.start()

    # TODO call this
    def stop(self):
        self.running = False
        self.flusher_thread.join(5)

    def _flusher_loop(self):
        while self.running:
            self._flush_metrics()
            time.sleep(self.FLUSH_PERIOD_S)

    def _flush_metrics(self):
        self._lock_all()
        to_flush = copy.deepcopy(self.buffered_counters)

        # reset
        for metric_name in self.buffered_counters:
            self.buffered_counters[metric_name] = {}
        self._release_all()

        for metric in to_flush:
            for metric_key in to_flush[metric]:
                self._flush_inc(metric, metric_key, to_flush)

    def _lock_all(self):
        for metric_name in self.locks:
            self.locks[metric_name].acquire()

    def _release_all(self):
        for metric_name in self.locks:
            self.locks[metric_name].release()

    def inc(self, metric: Metric, handler_name: str, handler_type: 'IOHandlerType', channel_id: str, value: int = 1):

        key = self._metric_key(handler_name, handler_type, channel_id)
        lock = self.locks[metric.value]

        lock.acquire()
        if key in self.buffered_counters[metric.value]:
            self.buffered_counters[metric.value][key] += value
        else:
            self.buffered_counters[metric.value][key] = value
        lock.release()

    def _metric_key(self, handler_name: str, handler_type: 'IOHandlerType', channel_id: str) -> str:
        if self.METRIC_KEY_DELIMITER in handler_name or self.METRIC_KEY_DELIMITER in handler_type.value or self.METRIC_KEY_DELIMITER in channel_id:
            raise RuntimeError(f'metric tags should not contain delimiter: {self.METRIC_KEY_DELIMITER}')
        return f'{handler_name}{self.METRIC_KEY_DELIMITER}{handler_type.value}{self.METRIC_KEY_DELIMITER}{channel_id}'

    def _parse_metric_key(self, key: str) -> Tuple[str, str, str]:
        s = key.split(self.METRIC_KEY_DELIMITER)
        return s[0], s[1], s[2]

    def _flush_inc(self, metric_name: str, key: str, to_flush: Dict):
        value = to_flush[metric_name][key]
        if value == 0:
            return
        handler_name, handler_type_str, channel_id = self._parse_metric_key(key)
        self.counters[metric_name].inc(value=value, tags={
            'handler_name': handler_name,
            'handler_type': handler_type_str,
            'channel_id': channel_id
        })


    # TODO asyncify this
    def latency(self, latency: float, handler_name: str, channel_id: str):
        self.latency_hist.observe(latency, tags={
            'handler_name': handler_name,
            'channel_id': channel_id
        })
