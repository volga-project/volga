from typing import Any, Callable, Dict, List, Optional, OrderedDict, Set, Tuple
from dataclasses import dataclass
import uuid
import time
from decimal import Decimal
from collections import defaultdict

@dataclass
class EventTimer:
    """Represents an event time timer."""
    timer_id: str        # Unique identifier
    timestamp: Decimal   # Event time when timer should fire
    key: Any             # Key associated with the timer
    namespace: Any       # Namespace for the timer (e.g., window, state identifier)
    
class EventTimeTimerService:
    """Event time timer service similar to Flink's design."""
    
    def __init__(self, timer_callback: Callable[[List[EventTimer]], None]):
        """Initialize with a callback function that receives a list of EventTimers."""
        self.timer_callback = timer_callback
        
        # Store timers sorted by timestamp for efficient retrieval
        # For each timestamp, maintain an OrderedDict to preserve registration order
        self.timers_by_time: Dict[Decimal, OrderedDict[str, None]] = {}
        
        # Store timer details by ID for efficient lookup
        self.timers_by_id: Dict[str, EventTimer] = {}  
        
        # Store timers by key and timestamp for efficient grouping and cancellation
        self.timers_by_key_time: Dict[Any, Dict[Decimal, Set[str]]] = {}  # key -> timestamp -> set of timer_ids
        
        # Current watermark
        self.current_watermark = Decimal('0')
    
    def register_timer(self, key: Any, namespace: Any, timestamp: Decimal) -> str:
        """Register a timer to fire at the given timestamp."""
        timer_id = str(uuid.uuid4())
        
        timer = EventTimer(
            timer_id=timer_id,
            timestamp=timestamp,
            key=key,
            namespace=namespace
        )
        
        self.timers_by_id[timer_id] = timer
        
        if timestamp not in self.timers_by_time:
            self.timers_by_time[timestamp] = OrderedDict()
        self.timers_by_time[timestamp][timer_id] = None
        
        if key not in self.timers_by_key_time:
            self.timers_by_key_time[key] = {}
        if timestamp not in self.timers_by_key_time[key]:
            self.timers_by_key_time[key][timestamp] = set()
        self.timers_by_key_time[key][timestamp].add(timer_id)
        
        return timer_id
    
    def delete_timer(self, timer_id: str) -> bool:
        """Delete a previously registered timer."""
        if timer_id not in self.timers_by_id:
            return False
        
        timer = self.timers_by_id[timer_id]
        
        del self.timers_by_id[timer_id]
        
        if timer.timestamp in self.timers_by_time:
            if timer_id in self.timers_by_time[timer.timestamp]:
                del self.timers_by_time[timer.timestamp][timer_id]
            if not self.timers_by_time[timer.timestamp]:
                del self.timers_by_time[timer.timestamp]
        
        if timer.key in self.timers_by_key_time and timer.timestamp in self.timers_by_key_time[timer.key]:
            self.timers_by_key_time[timer.key][timer.timestamp].discard(timer_id)
            if not self.timers_by_key_time[timer.key][timer.timestamp]:
                del self.timers_by_key_time[timer.key][timer.timestamp]
            if not self.timers_by_key_time[timer.key]:
                del self.timers_by_key_time[timer.key]
        
        return True
    
    def delete_timers_for_key(self, key: Any, timestamp: Optional[Decimal] = None) -> int:
        """Delete all timers for a specific key, optionally at a specific timestamp."""
        if key not in self.timers_by_key_time:
            return 0
        
        count = 0
        
        if timestamp is not None:
            if timestamp in self.timers_by_key_time[key]:
                timer_ids = list(self.timers_by_key_time[key][timestamp])
                for timer_id in timer_ids:
                    if self.delete_timer(timer_id):
                        count += 1
        else:
            timestamps = list(self.timers_by_key_time[key].keys())
            for ts in timestamps:
                timer_ids = list(self.timers_by_key_time[key][ts])
                for timer_id in timer_ids:
                    if self.delete_timer(timer_id):
                        count += 1
        
        return count
    
    def advance_watermark(self, watermark: Decimal) -> int:
        """Advance the watermark and fire eligible timers, grouped by key and timestamp."""
        if watermark <= self.current_watermark:
            return 0
        
        eligible_timestamps = [ts for ts in self.timers_by_time.keys() if ts <= watermark]
        eligible_timestamps.sort()
        
        num_fired = 0
        
        # Group timers by key and timestamp
        grouped_timers: Dict[Tuple[Any, Decimal], List[EventTimer]] = defaultdict(list)
        
        for timestamp in eligible_timestamps:
            # Get a copy of the timer_ids in registration order
            timer_ids = list(self.timers_by_time[timestamp].keys())
            
            for timer_id in timer_ids:
                if timer_id in self.timers_by_id:
                    timer = self.timers_by_id[timer_id]
                    grouped_timers[(timer.key, timer.timestamp)].append(timer)
                    num_fired += 1
        
        # Fire grouped timers
        for (key, timestamp), timers in grouped_timers.items():
            # Call the callback with the list of timers
            self.timer_callback(timers)
            # Delete all timers in the group
            for timer in timers:
                self.delete_timer(timer.timer_id)
        
        self.current_watermark = watermark
        
        return num_fired
    
    def get_current_watermark(self) -> Decimal:
        """Get the current watermark."""
        return self.current_watermark