import enum
from abc import ABC, abstractmethod
from typing import Dict, Any

from pydantic import BaseModel

# from volga.streaming.runtime.sources.wordcount.source import WordCountSourceSplitEnumerator


class SourceSplitType(enum.Enum):
    MORE_AVAILABLE = 0
    END_OF_INPUT = 1


class SourceSplit(BaseModel):
    type: SourceSplitType
    data: Any


class SourceSplitEnumerator(ABC):

    @abstractmethod
    def poll_next_split(self, task_id: int) -> SourceSplit:
        raise NotImplementedError()


class SourceSplitManager:
    def __init__(self, split_enumerators: Dict[int, SourceSplitEnumerator]):
        self.split_enumerators = split_enumerators

    def poll_next_split(self, operator_id: int, task_id: int) -> SourceSplit:
        if operator_id not in self.split_enumerators:
            raise RuntimeError(f'No split enumerator for operator_id {operator_id}, len: {len(self.split_enumerators)}')
        split_enumerator = self.split_enumerators[operator_id]
        return split_enumerator.poll_next_split(task_id)

    def get_num_sent(self) -> Any:
        # for split_enumerator in self.split_enumerators:
        #     # we can generalize this later
        #     if not isinstance(split_enumerator, WordCountSourceSplitEnumerator):
        #         raise RuntimeError('Getting aggregated number of sent messages is implemented only for WordCountSource')

        if len(self.split_enumerators) != 1:
            # we can generalize this later
            raise RuntimeError('WordCountSource exepcts exactly 1 split enumerator to aggregated number of sent messages')

        split_enumerator = list(self.split_enumerators.values())[0]
        # assert isinstance(split_enumerator, WordCountSourceSplitEnumerator)
        return split_enumerator.get_num_sent()
