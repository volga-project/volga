from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
from decimal import Decimal

from volga.streaming.api.function.function import SinkFunction


class ColdStorage(ABC):

    @abstractmethod
    def gen_sink_function(self, *args, **kwargs) -> SinkFunction:
        pass

    @abstractmethod
    def get_data(self, dataset_name: str, keys: Optional[List[Dict[str, Any]]], start_ts: Optional[Decimal], end_ts: Optional[Decimal]) -> List[Any]:
        pass
