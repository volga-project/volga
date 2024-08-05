import enum
import fcntl
import time
from threading import Thread
from typing import Dict, Tuple

import msgpack
from ray.util.metrics import Counter

# TODO these should be moved to config and shared between py and rust
METRICS_PATH_PREFIX = '/tmp/volga/rust/metrics'
METRIC_KEY_DELIMITER = ';'


# TODO these should be in sync with rust
class Metric(enum.Enum):
    NUM_BUFFERS_SENT = 'volga_num_buffers_sent'
    NUM_BUFFERS_RECVD = 'volga_num_buffers_recvd'
    NUM_BUFFERS_RESENT = 'volga_num_buffers_resent'

    NUM_BYTES_SENT = 'volga_num_bytes_sent'
    NUM_BYTES_RECVD = 'volga_num_bytes_recvd'


class TagKeys(enum.Enum):
    JOB_NAME = 'job_name'
    HANDLER_NAME = 'handler_name'
    CHANNEL_OR_PEER_ID = 'channel_or_peer_id'


class MetricsRecorder:

    FLUSH_PERIOD_S = 1

    def __init__(self, handler_name: str, job_name: str):
        self._handler_name = handler_name
        self._job_name = job_name
        tag_keys = (TagKeys.JOB_NAME.value, TagKeys.HANDLER_NAME.value, TagKeys.CHANNEL_OR_PEER_ID.value)
        self.counters = {
            Metric.NUM_BUFFERS_SENT.value: Counter(
                Metric.NUM_BUFFERS_SENT.value,
                description='Number of buffers sent',
                tag_keys=tag_keys,
            ),
            Metric.NUM_BUFFERS_RESENT.value: Counter(
                Metric.NUM_BUFFERS_RESENT.value,
                description='Number of buffers re-sent',
                tag_keys=tag_keys,
            ),
            Metric.NUM_BUFFERS_RECVD.value: Counter(
                Metric.NUM_BUFFERS_RECVD.value,
                description='Number of buffers received',
                tag_keys=tag_keys,
            ),
            Metric.NUM_BYTES_SENT.value: Counter(
                Metric.NUM_BYTES_SENT.value,
                description='Number of bytes sent',
                tag_keys=tag_keys,
            ),
            Metric.NUM_BYTES_RECVD.value: Counter(
                Metric.NUM_BYTES_RECVD.value,
                description='Number of bytes received',
                tag_keys=tag_keys,
            ),
        }
        self.flusher_thread = Thread(target=self._flusher_loop)
        self.running = False

    def _flusher_loop(self):
        while self.running:
            self._read_and_flush()
            time.sleep(self.FLUSH_PERIOD_S)

    def _read_and_flush(self):
        filename = self._metrics_filename()
        try:
            with open(filename, "r+") as f:
                # Acquire file lock
                fd = f.fileno()
                fcntl.flock(fd, fcntl.LOCK_EX)
                b = f.read()
                metrics = msgpack.loads(b)
                f.truncate(0) # clean up
        except IOError:
            return

        self._record_metrics_to_prom(metrics)

    def _record_metrics_to_prom(self, metrics: Dict):
        for metric_key in metrics:
            metric_name, channel_or_peer_id = MetricsRecorder.parse_metric_key(metric_key)
            if metric_name not in self.counters:
                raise RuntimeError(f'Unknown metric: {metric_name}')
            value = metrics[metric_key]
            self.counters[metric_name].inc(value=value, tags={
                TagKeys.CHANNEL_OR_PEER_ID.value: channel_or_peer_id,
                TagKeys.JOB_NAME.value: self._job_name,
                TagKeys.HANDLER_NAME.value: self._handler_name
            })

    # TODO this should be in sync with rust
    @staticmethod
    def parse_metric_key(metric_key: str) -> Tuple[str, str]:
        s = metric_key.split(METRIC_KEY_DELIMITER)
        return s[0], s[1]

    # TODO this should be in sync with rust
    def _metrics_filename(self) -> str:
        return f'{METRICS_PATH_PREFIX}/{self._job_name}/{self._handler_name}_metrics.metrics'

    def start(self):
        self.running = True
        self.flusher_thread.start()

    def close(self):
        self.running = False
        self.flusher_thread.join(5)
        self._read_and_flush()
        # TODO delete metrics file
