from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, Any

from volga.streaming.api.stream.stream_sink import StreamSink


class HotStorage(ABC):

    @abstractmethod
    def get_stream_sink(self) -> StreamSink:
        pass

    @abstractmethod
    def get_latest_data(self, dataset_name: str, keys: Dict[str, Any]) -> Any:
        pass