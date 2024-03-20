from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from decimal import Decimal

from volga.streaming.api.function.function import SinkFunction


class ColdStorage(ABC):

    @abstractmethod
    def gen_sink_function(self) -> SinkFunction:
        pass

    @abstractmethod
    def get_data(self, dataset_name: str, keys: Dict[str, Any], start_ts: Optional[Decimal], end_ts: Optional[Decimal]) -> List[Any]:
        pass