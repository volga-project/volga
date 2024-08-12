from volga.streaming.api.function.function import SourceFunction
from volga.streaming.api.operators.operators import SourceOperator
from volga.streaming.api.operators.timestamp_assigner import TimestampAssigner
from volga.streaming.api.stream.data_stream import DataStream
from volga.streaming.runtime.master.source_splits.source_splits_manager import SourceSplitEnumerator


class StreamSource(DataStream):

    def __init__(self, streaming_context: 'StreamingContext', source_function: SourceFunction):
        super().__init__(
            streaming_context=streaming_context,
            stream_operator=SourceOperator(source_function)
        )

    def timestamp_assigner(self, timestamp_assigner: TimestampAssigner) -> 'StreamSource':
        assert isinstance(self.stream_operator, SourceOperator)
        self.stream_operator.set_timestamp_assigner(timestamp_assigner=timestamp_assigner)
        return self

    def split_enumerator(self, split_enumerator: SourceSplitEnumerator) -> 'StreamSource':
        assert isinstance(self.stream_operator, SourceOperator)
        self.stream_operator.set_split_enumerator(split_enumerator=split_enumerator)
        return self
