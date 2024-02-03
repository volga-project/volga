from decimal import Decimal
from typing import List, Optional

from volga.streaming.api.message.message import Record


class WindowAggregationType:
    MAX = 'max'
    MIN = 'min'
    AVG = 'avg'
    STDDEV = 'stddev'


class SlidingWindowConfig:
    name: str
    agg: WindowAggregationType
    length_s: Decimal


class SlidingWindow:
    records: List[Record]
    length_s: Decimal
    max_timestamp: Optional[Decimal]

    def __init__(self, length_s: Decimal):
        self.records = []
        self.length_s = length_s
        self.max_timestamp = None


class WindowAssigner:
    pass


class WindowTrigger:
    pass
