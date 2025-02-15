from typing import Optional

from pydantic import BaseModel

from volga.common.time_utils import Duration
from volga.streaming.api.function.aggregate_function import AggregationType


# this is similar to AggregationType enum in streaming package, should we merge somehow?
class AggregateType(BaseModel):
    window: Duration
    # Name of the field the aggregate will be assigned to
    into: str

    # Name of the field to aggregate on
    on: Optional[str] = None

    def get_type(self):
        raise NotImplementedError()


class Count(AggregateType):

    def get_type(self):
        return AggregationType.COUNT


class Sum(AggregateType):

    def get_type(self):
        return AggregationType.SUM


class Avg(AggregateType):

    def get_type(self):
        return AggregationType.AVG


class Max(AggregateType):

    def get_type(self):
        return AggregationType.MAX


class Min(AggregateType):

    def get_type(self):
        return AggregationType.MIN
