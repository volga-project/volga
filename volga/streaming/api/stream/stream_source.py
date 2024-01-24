from volga.streaming.api.function.function import SourceFunction
from volga.streaming.api.operator.operator import SourceOperator
from volga.streaming.api.stream.data_stream import DataStream


class StreamSource(DataStream):

    def __init__(self, streaming_context: 'StreamingContext', source_function: SourceFunction):
        super().__init__(
            streaming_context=streaming_context,
            stream_operator=SourceOperator(source_function)
        )