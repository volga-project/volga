from typing import List, Optional, Any

from volga.streaming.api.function.function import SourceFunction, SourceContext
from volga.streaming.api.stream.stream_source import StreamSource


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

    def init(self, parallel: int, index: int):
        pass

    def fetch(self, ctx: SourceContext):
        word = self.dictionary[self._cur_index]
        ctx.collect(word)
        if word in self.num_sent_per_word:
            self.num_sent_per_word[word] += 1
        else:
            self.num_sent_per_word[word] = 1
        self._cur_index = (self._cur_index + 1)%len(self.dictionary)

    def get_num_sent(self) -> Any:
        return self.num_sent_per_word

    def run_for_s(self) -> int:
        return self._run_for_s
