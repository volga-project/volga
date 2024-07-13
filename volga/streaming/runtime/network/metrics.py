import enum

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

    @staticmethod
    def instance(job_name: str) -> 'MetricsRecorder':
        if MetricsRecorder._instance is None:
            MetricsRecorder._instance = MetricsRecorder(job_name)
        return MetricsRecorder._instance

    def __init__(self, job_name: str):
        self.counters = {
            Metric.NUM_BUFFERS_SENT: Counter(
                Metric.NUM_BUFFERS_SENT.value,
                description='Number of buffers sent',
                tag_keys=('job_name', 'handler_type', 'handler_name', 'channel_id'),
            ),
            Metric.NUM_BUFFERS_RESENT: Counter(
                Metric.NUM_BUFFERS_RESENT.value,
                description='Number of buffers re-sent',
                tag_keys=('job_name', 'handler_type', 'handler_name', 'channel_id'),
            ),
            Metric.NUM_BUFFERS_RCVD: Counter(
                Metric.NUM_BUFFERS_RCVD.value,
                description='Number of buffers received',
                tag_keys=('job_name', 'handler_type', 'handler_name', 'channel_id'),
            ),
            Metric.NUM_BUFFERS_DELIVERED: Counter(
                Metric.NUM_BUFFERS_DELIVERED.value,
                description='Number of buffers confirmed delivered',
                tag_keys=('job_name', 'handler_type', 'handler_name', 'channel_id'),
            ),
            Metric.NUM_RECORDS_SENT: Counter(
                Metric.NUM_RECORDS_SENT.value,
                description='Number of records sent',
                tag_keys=('job_name', 'handler_type', 'handler_name', 'channel_id'),
            ),
            Metric.NUM_RECORDS_RCVD: Counter(
                Metric.NUM_RECORDS_RCVD.value,
                description='Number of records received',
                tag_keys=('job_name', 'handler_type', 'handler_name', 'channel_id'),
            ),
            Metric.NUM_RECORDS_DELIVERED: Counter(
                Metric.NUM_RECORDS_DELIVERED.value,
                description='Number of records confirmed delivered',
                tag_keys=('job_name', 'handler_type', 'handler_name', 'channel_id'),
            )
        }

        for name in self.counters:
            self.counters[name].set_default_tags({'job_name': job_name})

        self.latency_hist = Histogram(
            Metric.LATENCY.value,
            description='Latency of roundtrip between two IOHandlers in ms',
            boundaries=[i + 1 for i in range(1000)],
            tag_keys=('job_name', 'handler_name', 'channel_id'),
        )
        self.latency_hist.set_default_tags({'job_name': job_name})

    def inc(self, metric: Metric, handler_name: str, handler_type: 'IOHandlerType', channel_id: str, value: int = 1):
        assert channel_id is not None
        self.counters[metric].inc(value=value, tags={
            'handler_name': handler_name,
            'handler_type': handler_type.value,
            'channel_id': channel_id
        })

    def latency(self, latency: float, handler_name: str, channel_id: str):
        self.latency_hist.observe(latency, tags={
            'handler_name': handler_name,
            'channel_id': channel_id
        })
