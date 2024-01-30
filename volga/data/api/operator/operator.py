from typing import Callable, Dict, Optional, Type, List

import pandas as pd

from volga.data.api.dataset.dataset import Node, Dataset

primitive_numeric_types = [int, float, pd.Int64Dtype, pd.Float64Dtype]


class Transform(Node):
    def __init__(self, node: Node, func: Callable, schema: Optional[Dict]):
        super().__init__()
        self.func = func
        self.node = node
        self.node.out_edges.append(self)
        self.new_schema = schema


class Assign(Node):
    def __init__(self, node: Node, column: str, output_type: Type, func: Callable):
        super().__init__()
        self.node = node
        self.node.out_edges.append(self)
        self.func = func
        self.column = column
        self.output_type = output_type


class Filter(Node):
    def __init__(self, node: Node, func: Callable):
        super().__init__()
        self.node = node
        self.node.out_edges.append(self)
        self.func = func


class AggregateType:
    pass


class Aggregate(Node):
    def __init__(
        self, node: Node, keys: List[str], aggregates: List[AggregateType]
    ):
        super().__init__()
        if len(keys) == 0:
            raise ValueError("Must specify at least one key")
        self.keys = keys
        self.aggregates = aggregates
        self.node = node
        self.node.out_edges.append(self)


class GroupBy:
    def __init__(self, node: Node, *args):
        super().__init__()
        self.keys = args
        self.node = node
        self.node.out_edges.append(self)

    def aggregate(self, aggregates: List[AggregateType]) -> Node:
        if len(aggregates) == 0:
            raise TypeError(
                "aggregate operator expects atleast one aggregation operation"
            )
        if len(self.keys) == 1 and isinstance(self.keys[0], list):
            self.keys = self.keys[0]  # type: ignore
        return Aggregate(self.node, list(self.keys), aggregates)

    def first(self) -> Node:
        if len(self.keys) == 1 and isinstance(self.keys[0], list):
            self.keys = self.keys[0]  # type: ignore
        return First(self.node, list(self.keys))  # type: ignore


class Dedup(Node):
    def __init__(self, node: Node, by: List[str]):
        super().__init__()
        self.node = node
        self.by = by
        self.node.out_edges.append(self)


class First(Node):
    def __init__(self, node: Node, keys: List[str]):
        super().__init__()
        self.keys = keys
        self.node = node
        self.node.out_edges.append(self)


class Join(Node):
    def __init__(
        self,
        node: Node,
        dataset: Dataset,
        on: Optional[List[str]] = None,
    ):
        super().__init__()
        self.node = node
        self.dataset = dataset
        self.on = on
        self.node.out_edges.append(self)


class Union(Node):
    def __init__(self, node: Node, other: Node):
        super().__init__()
        self.nodes = [node, other]
        node.out_edges.append(self)
        other.out_edges.append(self)


class Rename(Node):
    def __init__(self, node: Node, columns: Dict[str, str]):
        super().__init__()
        self.node = node
        self.column_mapping = columns
        self.node.out_edges.append(self)


class Drop(Node):
    def __init__(self, node: Node, columns: List[str], name="drop"):
        super().__init__()
        self.node = node
        self.columns = columns
        self.__name = name
        self.node.out_edges.append(self)


class DropNull(Node):
    def __init__(self, node: Node, columns: List[str]):
        super().__init__()
        self.node = node
        self.columns = columns
        self.node.out_edges.append(self)