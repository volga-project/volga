from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any, Callable, Dict, List, NamedTuple, Optional
import uuid

from pydantic import BaseModel

from volga.common.time_utils import Duration
from volga.streaming.api.function.retractable_aggregate_function import AggregationType
from volga.streaming.api.message.message import Record


@dataclass
class EventRef:
    """Reference to an event stored in the shared event store."""
    event_id: str
    event_time: Decimal


@dataclass
class Window:
    """
    Represents a time window with start and end time.
    """
    start_time: Decimal
    end_time: Decimal
    name: str
    window_id: str = field(default_factory=lambda: str(uuid.uuid4()))


class SlidingWindowConfig(BaseModel):
    """Configuration for a sliding window."""
    duration: Duration
    agg_type: AggregationType
    agg_on_func: Optional[Callable]
    name: Optional[str] = None
    fixed_interval: Optional[Duration] = None
    allowed_lateness: Optional[Duration] = None


AggregationsPerWindow = Dict[str, Any]  # window name -> agg value
OutputWindowFunc = Callable[[AggregationsPerWindow, Record], Record]  # forms output record


class WindowEventType(Enum):
    OPEN = 0
    TRIGGER = 1
    CLOSE = 2