from typing import Optional, List

from volga.streaming.api.function.function import SourceFunction, SourceContext
from volga.streaming.runtime.master.source_splits.source_splits_manager import SourceSplitEnumerator, SourceSplit


# this lives on job_master
class WordCountSourceSplitEnumerator(SourceSplitEnumerator):

    def __init__(self, parallelism: int, num_msgs: Optional[int], num_msgs_per_split: int, words: List[str]):
        pass

    def poll_next_split(self, task_id: int) -> SourceSplit:
        pass


# this lives on different job_workers
class WordCountSourceFunction(SourceFunction):
    def init(self, parallel: int, index: int):
        pass

    def fetch(self, ctx: SourceContext):
        pass