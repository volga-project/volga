from typing import Optional

from pydantic import BaseModel

from volga.data.api.dataset.window import Window


class AggregateType(BaseModel):
    window: Window
    # Name of the field the aggregate will  be assigned to
    into_field: str


class Count(AggregateType):
    of: Optional[str] = None
    unique: bool = False
    approx: bool = False


class Distinct(AggregateType):
    of: str
    unordered: bool


class Sum(AggregateType):
    of: str


class Average(AggregateType):
    of: str
    default: float = 0.0


class Max(AggregateType):
    of: str
    default: float

    def agg_type(self):
        return "max"


class Min(AggregateType):
    of: str
    default: float

    def agg_type(self):
        return "min"


class LastK(AggregateType):
    of: str
    limit: int
    dedup: bool


class Stddev(AggregateType):
    of: str
    default: float = -1.0
