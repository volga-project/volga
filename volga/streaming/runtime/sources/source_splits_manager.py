import enum
from abc import ABC, abstractmethod
from typing import Dict, Any

from pydantic import BaseModel


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
        print(f'[SourceSplitManager] POLLED SPLIT operator_id: {operator_id} task_id: {task_id}')
        if operator_id not in self.split_enumerators:
            raise RuntimeError(f'No split enumerator for operator_id {operator_id}, len: {len(self.split_enumerators)}')
        split_enumerator = self.split_enumerators[operator_id]
        return split_enumerator.poll_next_split(task_id)
