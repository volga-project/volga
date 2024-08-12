from typing import Optional, List, Dict

from volga.streaming.api.function.function import SourceFunction, SourceContext
from volga.streaming.api.stream.stream_source import StreamSource
from volga.streaming.runtime.master.source_splits.source_splits_manager import SourceSplitEnumerator, SourceSplit, \
    SourceSplitType


# this lives on job_master
class WordCountSourceSplitEnumerator(SourceSplitEnumerator):

    def __init__(self, count_per_word: Optional[int], num_msgs_per_split: int, dictionary: List[str]):
        self.counts_per_word = {word: count_per_word for word in dictionary}
        self.num_msgs_per_split = num_msgs_per_split
        self.done = False

    def _all_done(self) -> bool:
        res = True
        for w in self.counts_per_word:
            if self.counts_per_word[w] != 0:
                res = False
        return res

    def poll_next_split(self, task_id: int) -> SourceSplit:
        if self._all_done():
            return SourceSplit(type=SourceSplitType.END_OF_INPUT, data=None)
        words = {}
        num_msgs = 0
        index = 0
        while num_msgs != self.num_msgs_per_split and not self._all_done():
            all_words = list(self.counts_per_word.keys())
            index = index%len(all_words)
            word = all_words[index]
            count = self.counts_per_word[word]
            if count == 0:
                index += 1
                continue
            if word in words:
                words[word] += 1
            else:
                words[word] = 1
            self.counts_per_word[word] -= 1
            index += 1
            num_msgs += 1

        return SourceSplit(type=SourceSplitType.MORE_AVAILABLE, data=words)


# this lives on different job_workers
class WordCountSourceFunction(SourceFunction):

    def init(self, parallel: int, index: int):
        pass

    def fetch(self, ctx: SourceContext):
        split = ctx.get_current_split()
        if split.type == SourceSplitType.END_OF_INPUT:
            return
        words = split.data
        for word in words:
            count = words[word]
            for _ in range(count):
                ctx.collect(word)
        ctx.poll_next_split()


class WordCountSource(StreamSource):

    def __init__(self, streaming_context: 'StreamingContext', parallelism: int, count_per_word: Optional[int], num_msgs_per_split: int, dictionary: List[str]):
        super().__init__(
            streaming_context=streaming_context,
            source_function=WordCountSourceFunction()
        )

        self.split_enumerator(WordCountSourceSplitEnumerator(
            count_per_word=count_per_word,
            num_msgs_per_split=num_msgs_per_split,
            dictionary=dictionary
        ))
        self.set_parallelism(parallelism)
