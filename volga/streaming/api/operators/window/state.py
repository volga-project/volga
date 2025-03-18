from typing import Any, Dict, List, Optional, Set, Tuple
from decimal import Decimal

from volga.streaming.api.operators.window.models import Window, EventRef
from volga.streaming.api.message.message import Record

class WindowState:
    """
    Centralized state management for window operator.
    Stores events and manages window-event relationships.
    """
    def __init__(self):
        # Events per key: key -> event_id -> (event_time, event)
        self.events_per_key: Dict[Any, Dict[str, Record]] = {}

        # Windows per key: key -> window_name -> windows
        self.windows_per_key: Dict[Any, Dict[str, List[Window]]] = {}
        
        # Event to windows mapping: key -> event_id -> set of window_ids
        self.event_to_windows: Dict[Any, Dict[str, Set[str]]] = {}
        
        # Windows to events mapping: key -> window_id -> set of (event_id, is_processed)
        self.window_to_events: Dict[Any, Dict[str, Set[Tuple[str, bool]]]] = {}
        
        # Accumulators: window_id -> accumulator
        self.accumulators: Dict[str, Any] = {}
    
    def add_event(self, key: Any, event_id: str, event: Record) -> EventRef:
        """Add an event to the state and return a reference."""
        # Add to events by key
        if key not in self.events_per_key:
            self.events_per_key[key] = {}
        self.events_per_key[key][event_id] = event
        
        # Initialize event to windows mapping
        if key not in self.event_to_windows:
            self.event_to_windows[key] = {}
        self.event_to_windows[key][event_id] = set()
        
        return EventRef(event_id=event_id, event_time=event.event_time)
    
    def assign_event_to_window(self, key: Any, event_id: str, window_id: str) -> None:
        # Add window to event's windows
        if key not in self.event_to_windows:
            self.event_to_windows[key] = {}
        if event_id not in self.event_to_windows[key]:
            self.event_to_windows[key][event_id] = set()
        self.event_to_windows[key][event_id].add(window_id)
        
        # Add event to window's events
        if key not in self.window_to_events:
            self.window_to_events[key] = {}
        if window_id not in self.window_to_events[key]:
            self.window_to_events[key][window_id] = set()
        self.window_to_events[key][window_id].add((event_id, False))

    def mark_event_processed_for_window(self, key: Any, window_id: str, event_id: str) -> None:
        if key in self.window_to_events and window_id in self.window_to_events[key]:
            self.window_to_events[key][window_id].discard((event_id, False))
            self.window_to_events[key][window_id].add((event_id, True))
    
    def get_events_for_window(self, key: Any, window_id: str) -> List[Tuple[str, Record, bool]]:
        result = []
        if key in self.window_to_events and window_id in self.window_to_events[key]:
            for event_id, is_processed in self.window_to_events[key][window_id]:
                if event_id in self.events_per_key[key]:
                    record = self.events_per_key[key][event_id]
                    result.append((event_id, record, is_processed))
        return result
    
    def remove_event(self, key: Any, event_id: str) -> None:
        """Remove an event and clean up its references."""
        # Remove from events by key
        if key in self.events_per_key and event_id in self.events_per_key[key]:
            del self.events_per_key[key][event_id]
            if not self.events_per_key[key]:
                del self.events_per_key[key]
        
        # Remove from event to windows mapping and window to events mapping
        if key in self.event_to_windows and event_id in self.event_to_windows[key]:
            window_ids = self.event_to_windows[key][event_id].copy()
            del self.event_to_windows[key][event_id]
            if not self.event_to_windows[key]:
                del self.event_to_windows[key]
            
            # Remove from window to events mapping
            for window_id in window_ids:
                if key in self.window_to_events and window_id in self.window_to_events[key]:
                    self.window_to_events[key][window_id].discard(event_id)
                    if not self.window_to_events[key][window_id]:
                        del self.window_to_events[key][window_id]
                        if not self.window_to_events[key]:
                            del self.window_to_events[key]
    
    def remove_window(self, key: Any, window_id: str) -> None:
        if window_id in self.windows_per_key[key]:
            del self.windows_per_key[key][window_id]
        
        # TODO this should be a separate function
        # TODO on window close, we should remove only previous window acum, not current
        # Remove from accumulators
        if window_id in self.accumulators:
            del self.accumulators[window_id]
        
        # Get all events referenced by this window and remove window references
        if key in self.window_to_events and window_id in self.window_to_events[key]:
            events_to_check = list(self.window_to_events[key][window_id])
            del self.window_to_events[key][window_id]
            if not self.window_to_events[key]:
                del self.window_to_events[key]
            
            # Remove window from event references
            for event_id in events_to_check:
                if key in self.event_to_windows and event_id in self.event_to_windows[key]:
                    self.event_to_windows[key][event_id].discard(window_id)
                    if not self.event_to_windows[key][event_id]:
                        del self.event_to_windows[key][event_id]
                        if not self.event_to_windows[key]:
                            del self.event_to_windows[key]
                        
                        # If event is no longer referenced by any window, remove it
                        self.remove_event(key, event_id)
    
    def add_window(self, key: Any, window: Window, accumulator: Any) -> None:
        self.accumulators[window.window_id] = accumulator
        if key not in self.windows_per_key:
            self.windows_per_key[key] = {}
        if window.name not in self.windows_per_key[key]:
            self.windows_per_key[key][window.name] = []
        self.windows_per_key[key][window.name].append(window)
    
    def get_accumulator(self, window_id: str) -> Any:
        return self.accumulators[window_id]
    
    def update_accumulator(self, window_id: str, accumulator: Any) -> None:
        self.accumulators[window_id] = accumulator

    def clear_state(self) -> None:
        self.events_per_key.clear()
        self.windows_per_key.clear()
        self.event_to_windows.clear()
        self.window_to_events.clear()
        self.accumulators.clear()
