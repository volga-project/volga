import logging
import uuid
from decimal import Decimal
from typing import Dict, List, Optional, Set, Tuple, Callable, Any

from volga.common.time_utils import duration_to_s
from volga.streaming.api.collector.collector import Collector
from volga.streaming.api.context.runtime_context import RuntimeContext
from volga.streaming.api.function.function import EmptyFunction
from volga.streaming.api.function.retractable_aggregate_function import AggregationType, RetractableAggregateRegistry
from volga.streaming.api.message.message import Record, KeyRecord
from volga.streaming.api.operators.operators import StreamOperator, OneInputOperator
from volga.streaming.api.operators.window.models import Window, SlidingWindowConfig, AggregationsPerWindow, OutputWindowFunc, EventRef, WindowEvent, WindowEventType
from volga.streaming.api.operators.window.state import WindowState

logger = logging.getLogger(__name__)

class WindowOperator(StreamOperator, OneInputOperator):
    
    def __init__(
        self,
        configs: List[SlidingWindowConfig],
        output_func: Optional[OutputWindowFunc] = None
    ):
        super().__init__(EmptyFunction())
        self.configs = configs
        self.output_func = output_func
        
        # Initialize state
        self.state = WindowState()
        
        # Current watermark
        self.current_watermark = Decimal('-inf')
        
        # Statistics
        self.late_events_dropped = 0
        self.late_but_allowed_events = 0
        
        # Precompute window configs
        self.window_configs_by_name = {}
        for config in configs:
            name = config.name
            if name is None:
                name = f'{config.agg_type}_{config.duration}'
            
            window_size = duration_to_s(config.duration)
            slide_interval = None
            if config.fixed_interval:
                slide_interval = duration_to_s(config.fixed_interval)
            
            allowed_lateness = None
            if config.allowed_lateness:
                allowed_lateness = duration_to_s(config.allowed_lateness)
            
            self.window_configs_by_name[name] = (config, window_size, slide_interval, allowed_lateness)
            logger.debug(f"Initialized window config: name={name}, size={window_size}, slide={slide_interval}")
    
    def open(self, collectors: List[Collector], runtime_context: RuntimeContext):
        super().open(collectors, runtime_context)
        logger.debug("WindowOperator opened")
    
    def _is_watermark(self, record: Record) -> bool:
        """Check if a record is a watermark."""
        is_wm = hasattr(record, 'is_watermark') and record.is_watermark
        if is_wm:
            logger.debug(f"Detected watermark with time {record.event_time}")
        return is_wm
    
    def process_element(self, record: Record):
        if self._is_watermark(record):
            self._process_watermark(record)
            return
            
        assert isinstance(record, KeyRecord), "WindowOperator requires KeyRecords"
        key = record.key
        event_time = record.event_time
        
        logger.debug(f"--------------------------------")
        logger.debug(f"Processing element: key={key}, value={record.value}, time={event_time}")
        
        # Check if the event is late (before current watermark)
        if event_time < self.current_watermark:
            # Check if we can still accept this late event
            can_accept = False
            for name, (_, _, _, allowed_lateness) in self.window_configs_by_name.items():
                if allowed_lateness and event_time >= (self.current_watermark - allowed_lateness):
                    can_accept = True
                    self.late_but_allowed_events += 1
                    logger.debug(f"Accepting late event with time {event_time} within allowed lateness")
                    break
            
            if not can_accept:
                self.late_events_dropped += 1
                logger.debug(f"Dropping late event with time {event_time}, current watermark: {self.current_watermark}")
                return
        
        windows_to_update = {}  # name -> list of window_ids
        for name, (config, window_size, slide_interval, _) in self.window_configs_by_name.items():
            windows = self._get_or_create_windows_for_event(key, name, event_time)
            windows_to_update[name] = windows
            logger.debug(f"Found/created {len(windows)} windows for event: key={key}, name={name}, time={event_time}")
        
        # Now store the event
        event_id = str(uuid.uuid4())
        self.state.add_event(key, event_id, record)
        logger.debug(f"Added event: id={event_id}, key={key}, time={event_time}")
        
        # Assign event to windows
        for _, windows in windows_to_update.items():
            for window in windows:
                assert window.start_time <= event_time < window.end_time
                self.state.assign_event_to_window(key, event_id, window.window_id)
                logger.debug(f"Assigned event {event_id} to window {window.window_id} [{window.start_time}, {window.end_time})")
    
    def _process_watermark(self, watermark: Record):
        """Process a watermark record."""
        watermark_time = watermark.event_time
        prev_watermark = self.current_watermark
        self.current_watermark = watermark_time
        logger.debug(f"--------------------------------")
        logger.debug(f"Processing watermark: time={watermark_time}")
        
        all_window_events = []
        for key in self.state.windows_per_key:
            window_events = self._generate_window_events(key, prev_watermark, watermark_time)
            
            if window_events:
                logger.debug(f"Generated {len(window_events)} window events for key={key}")
                all_window_events.extend(window_events)

        all_window_events.sort(key=lambda e: e.timestamp)

        for window_event in all_window_events:
            self._process_window_event(key, window_event, watermark)
        
        logger.debug(f"Forwarding watermark: time={watermark_time}")
        self.collect(watermark)
    
    def _generate_window_events(self, key: Any, prev_watermark: Decimal, current_watermark: Decimal) -> List[WindowEvent]:
        window_events = []
        
        # Check all windows for this key
        for name, windows in self.state.windows_per_key[key].items():
            _, _, _, allowed_lateness = self.window_configs_by_name[name]
            
            for window in windows:
                # Add an OPEN event if window start is within watermark range
                if prev_watermark < window.start_time <= current_watermark:
                    window_events.append(WindowEvent(
                        window=window,
                        event_type=WindowEventType.OPEN,
                        timestamp=window.start_time
                    ))
                    logger.debug(f"--------------------------------")
                    logger.debug(f"Generated OPEN event for key {key}, window [{window.start_time}, {window.end_time}) at time {window.start_time}")
                
                # Add TRIGGER event if window end is within watermark range
                if prev_watermark < window.end_time <= current_watermark:
                    window_events.append(WindowEvent(
                        window=window,
                        event_type=WindowEventType.TRIGGER,
                        timestamp=window.end_time
                    ))
                    logger.debug(f"--------------------------------")
                    logger.debug(f"Generated TRIGGER event for key {key}, window [{window.start_time}, {window.end_time}) at time {window.end_time}")
                
                # Add CLOSE event if window should be closed (past end + allowed lateness)
                max_allowed_time = window.end_time
                if allowed_lateness:
                    max_allowed_time += allowed_lateness
                
                if prev_watermark < max_allowed_time <= current_watermark:
                    window_events.append(WindowEvent(
                        window=window,
                        event_type=WindowEventType.CLOSE,
                        timestamp=max_allowed_time
                    ))
                    logger.debug(f"--------------------------------")
                    logger.debug(f"Generated CLOSE event for key {key}, window [{window.start_time}, {window.end_time}) at time {max_allowed_time}")
        
        return window_events
    
    def _process_window_event(self, key: Any, window_event: WindowEvent, watermark: Record):
        window_id = window_event.window.window_id
        timestamp = window_event.timestamp
        window_start = window_event.window.start_time
        window_end = window_event.window.end_time
        
        if window_event.event_type == WindowEventType.OPEN:
            # Window is opening - initialize its accumulator
            logger.debug(f"--------------------------------")
            logger.debug(f"Processing OPEN event for window [{window_start}, {window_end}) at time {timestamp}")
            self._initialize_window_accumulator(key, window_event.window, timestamp)
            
        elif window_event.event_type == WindowEventType.TRIGGER:
            # Window is triggering - update accumulator with new events
            logger.debug(f"--------------------------------")
            logger.debug(f"Processing TRIGGER event for window [{window_start}, {window_end}) at time {timestamp}")
            self._update_window_accumulator(key, window_event.window, timestamp)
            
            # Emit results immediately after updating the accumulator
            self._emit_window_results(key, timestamp, watermark)
            
        elif window_event.event_type == WindowEventType.CLOSE:
            logger.debug(f"--------------------------------")
            logger.debug(f"Processing CLOSE event for window [{window_start}, {window_end}) at time {timestamp}")
            
            # Remove from state
            self.state.remove_window(key, window_id)
                    
    def _initialize_window_accumulator(self, key: Any, window: Window, timestamp: Decimal):
        config, _, _, _ = self.window_configs_by_name[window.name]
        
        agg_func = RetractableAggregateRegistry.create(config.agg_type, config.agg_on_func)
        accumulator = agg_func.create_accumulator()
        
        event_count = 0
        for event_id, record, is_processed in self.state.get_events_for_window(key, window.window_id):
            if window.start_time <= record.event_time < window.end_time and record.event_time <= timestamp and not is_processed:
                agg_func.add(record.value, accumulator)
                event_count += 1
                self.state.mark_event_processed_for_window(key, window.window_id, event_id)
        
        self.state.update_accumulator(window.window_id, accumulator)
        logger.debug(f"Initialized accumulator for window {window.window_id} with {event_count} events at time {timestamp}, "
                    f"value: {agg_func.get_result(accumulator)}")
    
    def _update_window_accumulator(self, key: Any, window: Window, timestamp: Decimal):
        config, _, _, _ = self.window_configs_by_name[window.name]
        
        # Get current accumulator
        accumulator = self.state.get_accumulator(window.window_id)
        assert accumulator is not None
        
        # Get aggregate function
        agg_func = RetractableAggregateRegistry.create(config.agg_type, config.agg_on_func)
        
        event_count = 0
        for event_id, record, is_processed in self.state.get_events_for_window(key, window.window_id):
            if window.start_time <= record.event_time < window.end_time and record.event_time <= timestamp and not is_processed:
                agg_func.add(record.value, accumulator)
                event_count += 1
                self.state.mark_event_processed_for_window(key, window.window_id, event_id)
        
        # Update the accumulator
        self.state.update_accumulator(window.window_id, accumulator)
        logger.debug(f"Updated accumulator for window {window.window_id} with {event_count} new events at time {timestamp}, "
                    f"value: {agg_func.get_result(accumulator)}")
    
    def _emit_window_results(self, key: Any, emit_time: Decimal, watermark: Record):
        # Collect results from all configured windows at this emit time
        window_results = {}
        
        for name, (config, _, _, _) in self.window_configs_by_name.items():
            # Find all windows for this config that are relevant at this emit time
            relevant_windows = []
            
            if name in self.state.windows_per_key[key]:
                for window in self.state.windows_per_key[key][name]:
                    assert window is not None
                    
                    if window.start_time < emit_time <= window.end_time:
                        relevant_windows.append(window.window_id)
                        logger.debug(f"Window {window.window_id} [{window.start_time}, {window.end_time}) is relevant for emit time {emit_time}")
            
            # If we found relevant windows, get their aggregated result
            if len(relevant_windows) > 0:
                window_id = relevant_windows[0]
                accumulator = self.state.get_accumulator(window_id)
                assert accumulator is not None

                agg_func = RetractableAggregateRegistry.create(config.agg_type, config.agg_on_func)
                result = agg_func.get_result(accumulator)
                window_results[name] = result
                logger.debug(f"Using result from window {window_id} for {name} at emit time {emit_time}: {result}")
            else:
                # No relevant windows found, use default value (0 for numeric aggregates)
                if config.agg_type in [AggregationType.SUM, AggregationType.COUNT, AggregationType.AVG]:
                    window_results[name] = 0
                    logger.debug(f"No relevant windows for {name} at emit time {emit_time}, using default value 0")
                elif config.agg_type in [AggregationType.MIN, AggregationType.MAX]:
                    window_results[name] = None
                    logger.debug(f"No relevant windows for {name} at emit time {emit_time}, using default value None")
                else:
                    raise ValueError(f"Unsupported aggregate type: {config.agg_type}")
        
        # Create output record with the emission time
        if self.output_func is None:
            output_record = Record(
                value=window_results, 
                event_time=emit_time,
                source_emit_ts=watermark.source_emit_ts
            )
        else:
            # Create a copy of the most recent record with the emission time
            template_record = Record(
                value=window_results,
                event_time=emit_time,
                source_emit_ts=watermark.source_emit_ts
            )
            output_record = self.output_func(window_results, template_record)
        
        # TODO proper set stream name
        output_record.set_stream_name(watermark.stream_name)
        logger.debug(f"Emitting result record for key={key} at time={emit_time} with value: {output_record.value}")
        self.collect(output_record)
    
    def _create_window(self, key: Any, name: str, start_time: Decimal, end_time: Decimal) -> Window:
        # Create the window
        window = Window(
            start_time=start_time,
            end_time=end_time,
            name=name
        )
        logger.debug(f"Creating window: key={key}, name={name}, id={window.window_id}, [{start_time}, {end_time})")
        
        # Create empty accumulator
        config = next((c for n, (c, _, _, _) in self.window_configs_by_name.items() if n == name), None)
        if not config:
            logger.error(f"Cannot find config for window name: {name}")
            return None
            
        agg_func = RetractableAggregateRegistry.create(config.agg_type, config.agg_on_func)
        accumulator = agg_func.create_accumulator()
        
        # Add to state
        self.state.add_window(key, window, accumulator)
        
        return window

    def _get_or_create_windows_for_event(self, key: Any, name: str, event_time: Decimal) -> List[Window]:
        _, window_size, slide_interval, _ = self.window_configs_by_name[name]
        windows = []
        
        if slide_interval:
            # Check existing windows
            existing_windows = self.state.windows_per_key.get(key, {}).get(name, [])
            existing_starts = {}
            
            for window in existing_windows:
                existing_starts[window.start_time] = window

            assert len(existing_starts) == len(existing_windows)

            logger.debug(f"Found {len(existing_starts)} existing windows for key={key}, name={name}, event_time={event_time}: {existing_starts}")

            if len(existing_starts) == 0:
                # No exisitng windows, align to the interval grid
                window_start = (event_time // slide_interval) * slide_interval
            else:
                window_start = min(existing_starts.keys())
            
            window_end = window_start + window_size
            
            # Create or find windows that contain this event
            while window_start <= event_time:
                if window_end > event_time:
                    if window_start in existing_starts:
                        window = existing_starts[window_start]
                        windows.append(window)
                        logger.debug(f"Using existing window: id={window.window_id}, [{window.start_time}, {window.end_time}), event_time={event_time}")
                    else:
                        # Create a new window
                        window = self._create_window(key, name, window_start, window_end)
                        windows.append(window)
                        logger.debug(f"Created new window: id={window.window_id}, [{window.start_time}, {window.end_time}), event_time={event_time}")
                else:
                    logger.debug(f"Skipping window [{window_start}, {window_end}) as it doesn't contain event time {event_time}")
                
                window_start += slide_interval
                window_end = window_start + window_size
        else:
            # TODO what if two events with same event_time?
            # For event-based sliding, create a window starting at this event
            window_end = event_time + window_size
            logger.debug(f"Event-based window: event_time={event_time}, window=[{event_time}, {window_end})")
            window = self._create_window(key, name, event_time, window_end)
            windows.append(window)
        
        logger.debug(f"Returning {len(windows)} windows for event: key={key}, event_time={event_time}")
        return windows

    def close(self):
        super().close()
        logger.info(f"Closing WindowOperator. Stats: late_events_dropped={self.late_events_dropped}, "
                   f"late_but_allowed_events={self.late_but_allowed_events}")
        self.state.clear_state()
