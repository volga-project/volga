from volga.streaming.api.operator.operator import SinkOperator
from volga.streaming.api.stream.stream import Stream


class StreamSink(Stream):
    def __init__(
        self,
        input_stream: Stream,
        sink_operator: SinkOperator
    ):
        super().__init__(input_stream=input_stream, stream_operator=sink_operator)
        self.streaming_context.add_sink(self)