from abc import ABC, abstractmethod
from typing import Callable
from decimal import Decimal

from volga.streaming.api.message.message import Record


class TimestampAssigner(ABC):

    @abstractmethod
    def assign_timestamp(self, record: Record) -> Record:
        pass


class EventTimeAssigner(TimestampAssigner):

    # TODO Decimal is not serializable - change type
    def __init__(self, func: Callable[[Record], Decimal]):
        self.func = func

    def assign_timestamp(self, record: Record) -> Record:
        ts = self.func(record)
        record.event_time = ts
        return record
