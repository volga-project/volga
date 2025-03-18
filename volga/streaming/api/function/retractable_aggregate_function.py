from abc import abstractmethod
from typing import Any, Dict, Optional, Callable
from enum import Enum

from volga.streaming.api.function.function import Function
from volga.streaming.api.message.message import Record


class AggregationType(str, Enum):
    MAX = 'max'
    MIN = 'min'
    COUNT = 'count'
    SUM = 'sum'
    AVG = 'avg'


class RetractableAggregateFunction(Function):
    """
    An aggregate function that supports adding and retracting elements.
    This is essential for sliding windows where elements can leave the window.
    """

    @abstractmethod
    def create_accumulator(self) -> Any:
        """Creates a new empty accumulator."""
        pass

    @abstractmethod
    def add(self, value: Any, accumulator: Any) -> Any:
        """Adds a value to the accumulator."""
        pass

    @abstractmethod
    def retract(self, value: Any, accumulator: Any) -> Any:
        """Retracts a value from the accumulator."""
        pass

    @abstractmethod
    def get_result(self, accumulator: Any) -> Any:
        """Gets the result from the accumulator."""
        pass


class RetractableAggregateRegistry:
    """Registry of standard retractable aggregate functions."""
    
    @staticmethod
    def create(agg_type: AggregationType, agg_on_func: Callable) -> 'RetractableAggregateFunction':
        if agg_type == AggregationType.COUNT:
            return CountAggregate(agg_on_func)
        elif agg_type == AggregationType.SUM:
            return SumAggregate(agg_on_func)
        elif agg_type == AggregationType.AVG:
            return AvgAggregate(agg_on_func)
        elif agg_type == AggregationType.MIN:
            return MinAggregate(agg_on_func)
        elif agg_type == AggregationType.MAX:
            return MaxAggregate(agg_on_func)
        else:
            raise ValueError(f"Unsupported aggregation type: {agg_type}")


class CountAggregate(RetractableAggregateFunction):
    def __init__(self, agg_on_func: Callable):
        self.agg_on_func = agg_on_func
    
    def create_accumulator(self) -> Dict:
        return {"count": 0}
    
    def add(self, value: Any, accumulator: Dict) -> Dict:
        accumulator["count"] += 1
        return accumulator
    
    def retract(self, value: Any, accumulator: Dict) -> Dict:
        accumulator["count"] -= 1
        return accumulator
    
    def get_result(self, accumulator: Dict) -> Any:
        return accumulator["count"]


class SumAggregate(RetractableAggregateFunction):
    def __init__(self, agg_on_func: Callable):
        self.agg_on_func = agg_on_func
    
    def create_accumulator(self) -> Dict:
        return {"sum": 0}
    
    def add(self, value: Any, accumulator: Dict) -> Dict:
        accumulator["sum"] += self.agg_on_func(value)
        return accumulator
    
    def retract(self, value: Any, accumulator: Dict) -> Dict:
        accumulator["sum"] -= self.agg_on_func(value)
        return accumulator
    
    def get_result(self, accumulator: Dict) -> Any:
        return accumulator["sum"]


class AvgAggregate(RetractableAggregateFunction):
    def __init__(self, agg_on_func: Callable):
        self.agg_on_func = agg_on_func
    
    def create_accumulator(self) -> Dict:
        return {"sum": 0, "count": 0}
    
    def add(self, value: Any, accumulator: Dict) -> Dict:
        accumulator["sum"] += self.agg_on_func(value)
        accumulator["count"] += 1
        return accumulator
    
    def retract(self, value: Any, accumulator: Dict) -> Dict:
        accumulator["sum"] -= self.agg_on_func(value)
        accumulator["count"] -= 1
        return accumulator
    
    def get_result(self, accumulator: Dict) -> Any:
        if accumulator["count"] == 0:
            return 0
        return accumulator["sum"] / accumulator["count"]


class MinAggregate(RetractableAggregateFunction):
    def __init__(self, agg_on_func: Callable):
        self.agg_on_func = agg_on_func
    
    def create_accumulator(self) -> Dict:
        return {"min": None, "values": {}}
    
    def add(self, value: Any, accumulator: Dict) -> Dict:
        extracted = self.agg_on_func(value)
        
        # Track value frequency for proper retraction
        if extracted in accumulator["values"]:
            accumulator["values"][extracted] += 1
        else:
            accumulator["values"][extracted] = 1
        
        # Update min
        if accumulator["min"] is None or extracted < accumulator["min"]:
            accumulator["min"] = extracted
            
        return accumulator
    
    def retract(self, value: Any, accumulator: Dict) -> Dict:
        extracted = self.agg_on_func(value)
        
        # Update value frequency
        if extracted in accumulator["values"]:
            accumulator["values"][extracted] -= 1
            if accumulator["values"][extracted] == 0:
                del accumulator["values"][extracted]
                
                # If we removed the min value, find the new min
                if extracted == accumulator["min"]:
                    if accumulator["values"]:
                        accumulator["min"] = min(accumulator["values"].keys())
                    else:
                        accumulator["min"] = None
        
        return accumulator
    
    def get_result(self, accumulator: Dict) -> Any:
        return accumulator["min"]


class MaxAggregate(RetractableAggregateFunction):
    def __init__(self, agg_on_func: Callable):
        self.agg_on_func = agg_on_func
    
    def create_accumulator(self) -> Dict:
        return {"max": None, "values": {}}
    
    def add(self, value: Any, accumulator: Dict) -> Dict:
        extracted = self.agg_on_func(value)
        
        # Track value frequency for proper retraction
        if extracted in accumulator["values"]:
            accumulator["values"][extracted] += 1
        else:
            accumulator["values"][extracted] = 1
        
        # Update max
        if accumulator["max"] is None or extracted > accumulator["max"]:
            accumulator["max"] = extracted
            
        return accumulator
    
    def retract(self, value: Any, accumulator: Dict) -> Dict:
        extracted = self.agg_on_func(value)
        
        # Update value frequency
        if extracted in accumulator["values"]:
            accumulator["values"][extracted] -= 1
            if accumulator["values"][extracted] == 0:
                del accumulator["values"][extracted]
                
                # If we removed the max value, find the new max
                if extracted == accumulator["max"]:
                    if accumulator["values"]:
                        accumulator["max"] = max(accumulator["values"].keys())
                    else:
                        accumulator["max"] = None
        
        return accumulator
    
    def get_result(self, accumulator: Dict) -> Any:
        return accumulator["max"] 