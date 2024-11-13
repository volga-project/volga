from typing import Optional, List, Dict

from volga.streaming.api.function.function import SourceFunction, SourceContext
from volga.streaming.api.stream.stream_source import StreamSource
from volga.streaming.common.utils import now_ts_ms
from volga.streaming.runtime.sources.source_splits_manager import SourceSplitEnumerator, SourceSplit, \
    SourceSplitType


# this lives on job_master
class WordCountSourceSplitEnumerator(SourceSplitEnumerator):

    def __init__(self, dictionary: List[str], split_size: int, count_per_word: Optional[int] = None, run_for_s: Optional[int] = None):
        if run_for_s is None and count_per_word is None:
            raise RuntimeError('Need to specify either count_per_word or run_for_s')
        self.dictionary = dictionary
        self.count_per_word = count_per_word
        self.num_sent_per_word = {word: 0 for word in dictionary}
        self.run_for_s = run_for_s
        self.split_size = split_size
        self.done = False
        self.started_at_ms = None

    def _all_done(self) -> bool:
        if self.run_for_s:
            # we are bounded by time , done when time is finished
            if self.started_at_ms is None:
                return False
            return now_ts_ms() - self.started_at_ms > self.run_for_s * 1000

        # bounded by num messages
        res = True
        for w in self.num_sent_per_word:
            if self.num_sent_per_word[w] != self.count_per_word:
                res = False
        return res

    def poll_next_split(self, task_id: int) -> SourceSplit:
        if self.started_at_ms is None:
            self.started_at_ms = now_ts_ms()

        if self._all_done():
            return SourceSplit(type=SourceSplitType.END_OF_INPUT, data=None)
        words = {}
        num_msgs = 0
        index = 0
        while num_msgs != self.split_size and not self._all_done():
            index = index%len(self.dictionary)
            word = self.dictionary[index]
            if self.run_for_s is not None:
                # limited by time
                self.num_sent_per_word[word] += 1
            else:
                # limited by num messages
                num_sent = self.num_sent_per_word[word]
                if num_sent == self.count_per_word:
                    index += 1
                    continue
                else:
                    self.num_sent_per_word[word] += 1
            if word in words:
                words[word] += 1
            else:
                words[word] = 1
            index += 1
            num_msgs += 1

        return SourceSplit(type=SourceSplitType.MORE_AVAILABLE, data=words)

    def get_num_sent(self) -> Dict[str, int]:
        return self.num_sent_per_word


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

    def __init__(
        self,
        streaming_context: 'StreamingContext',
        parallelism: int,
        dictionary: List[str],
        split_size: int,
        # specify either count_per_word for limited num of sent words or run_for_s for limited time to run
        count_per_word: Optional[int] = None,
        run_for_s: Optional[int] = None
    ):
        super().__init__(
            streaming_context=streaming_context,
            source_function=WordCountSourceFunction()
        )

        self.split_enumerator(WordCountSourceSplitEnumerator(
            dictionary=dictionary,
            split_size=split_size,
            count_per_word=count_per_word,
            run_for_s=run_for_s
        ))
        self.set_parallelism(parallelism)
