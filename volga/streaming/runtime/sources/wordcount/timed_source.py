from typing import List, Optional, Any

from volga.streaming.api.function.function import SourceFunction, SourceContext
from volga.streaming.api.operators.operators import SourceOperator
from volga.streaming.api.stream.stream_source import StreamSource
from volga.streaming.common.utils import now_ts_ms


# regular source where each worker is independent (not synced by split coordinator/manager)
class WordCountSource(StreamSource):

    def __init__(
        self,
        streaming_context: 'StreamingContext',
        dictionary: List[str],
        run_for_s: int
    ):
        super().__init__(
            streaming_context=streaming_context,
            source_function=WordCountTimedSourceFunction(dictionary, run_for_s)
        )


class WordCountTimedSourceFunction(SourceFunction):

    def __init__(
        self,
        dictionary: List[str],
        run_for_s: int
    ):
        self.dictionary = dictionary
        self._run_for_s = run_for_s
        self.num_sent_per_word = {}
        self._cur_index = 0
        self._started_at_ms = None

    def init(self, parallel: int, index: int):
        pass

    def fetch(self, ctx: SourceContext):
        if self._started_at_ms is None:
            self._started_at_ms = now_ts_ms()

        # TODO there is a possibility of race condition between this logic and stuff in SourceContextImpl
        # TODO we should implement a proper job shutdown
        if now_ts_ms() - self._started_at_ms > self._run_for_s * 1000:
            ctx.collect(SourceOperator.TERMINAL_MESSAGE)
            return

        word = self.dictionary[self._cur_index]
        ctx.collect(word)
        if word in self.num_sent_per_word:
            self.num_sent_per_word[word] += 1
        else:
            self.num_sent_per_word[word] = 1
        self._cur_index = (self._cur_index + 1)%len(self.dictionary)

    def get_num_sent(self) -> Any:
        return self.num_sent_per_word
