from typing import Optional

from pydantic import BaseModel

from volga.data.api.dataset.window import Window


class AggregateType(BaseModel):
    window: Window
    # Name of the field the aggregate will be assigned to
    into: str


class Count(AggregateType):
    pass


class Sum(AggregateType):
    on: str


class Avg(AggregateType):
    on: str


class Max(AggregateType):
    on: str


class Min(AggregateType):
    on: str
