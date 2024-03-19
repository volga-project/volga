from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, Any, Optional

from volga.streaming.api.stream.stream_sink import StreamSink


class ColdStorage(ABC):

    @abstractmethod
    def get_stream_sink(self) -> StreamSink:
        pass

    @abstractmethod
    def get_data(self, dataset_name: str, keys: Dict[str, Any], start_date: Optional[datetime], end_date: Optional[datetime]) -> Any:
        pass