from abc import ABC, abstractmethod
from typing import Dict, Any, List

from volga.streaming.api.function.function import SinkFunction


class HotStorage(ABC):

    @abstractmethod
    def gen_sink_function(self, *args, **kwargs) -> SinkFunction:
        pass

    @abstractmethod
    def get_latest_data(self, dataset_name: str, keys: List[Dict[str, Any]]) -> Any:
        pass