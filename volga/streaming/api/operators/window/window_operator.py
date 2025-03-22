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
from volga.streaming.api.operators.window.models import Window, SlidingWindowConfig, AggregationsPerWindow, OutputWindowFunc, WindowEventType
from volga.streaming.api.operators.window.state import WindowState
from volga.streaming.api.operators.window.timer.event_time.event_time_timer_service import EventTimeTimerService, EventTimer

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
        
        self.state = WindowState()

        self.last_updated_accum_per_key = {} # should this be part of state?

        self.timer_service = EventTimeTimerService(self._on_event_timer)
        
        self.current_watermark = Decimal('-inf')
        
        self.late_events_dropped = 0
        self.late_but_allowed_events = 0
        
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
        
        logger.debug(f"Found/created total {sum(len(windows) for windows in windows_to_update.values())} windows for event: key={key}, time={event_time}")
        
        event_id = str(uuid.uuid4())
        self.state.add_event(key, event_id, record)
        logger.debug(f"Added event: key={key}, value={record.value}, time={event_time}")
        
        for _, windows in windows_to_update.items():
            for window, is_new in windows:
                config, _, _, allowed_lateness = self.window_configs_by_name[window.name]
        
                # agg_func = RetractableAggregateRegistry.create(config.agg_type, config.agg_on_func)
                # assert window.start_time <= event_time < window.end_time

                # if is_new:
                #     accumulator = agg_func.create_accumulator()
                # else:
                #     accumulator = self.state.get_accumulator(window.window_id)
                #     assert accumulator is not None
                # agg_func.add(record.value, accumulator)
                # self.state.update_accumulator(window.window_id, accumulator)
                # if is_new:
                #     logger.debug(f"Initialized accumulator for key={key}, name={window.name} window [{window.start_time}, {window.end_time}) at time {event_time}, "
                #                 f"value: {agg_func.get_result(accumulator)}")
                # else:
                #     logger.debug(f"Updated accumulator for key={key}, name={window.name} window [{window.start_time}, {window.end_time}) at time {event_time}, "
                #                 f"value: {agg_func.get_result(accumulator)}")
                    
                if is_new:
                    # self.timer_service.register_timer(key, (window, WindowEventType.OPEN), window.start_time)
                    self.timer_service.register_timer(key, (window, WindowEventType.TRIGGER), window.end_time)

                    close_time = window.end_time
                    if allowed_lateness:
                        close_time += duration_to_s(allowed_lateness)
                    self.timer_service.register_timer(key, (window, WindowEventType.CLOSE), close_time)
                
                self.state.assign_event_to_window(key, event_id, window.window_id)
                logger.debug(f"Assigned event key={key}, value={record.value}, time={event_time} to window [{window.start_time}, {window.end_time}), {window.window_id}")
    
    def _process_watermark(self, watermark: Record):
        watermark_time = watermark.event_time
        self.current_watermark = watermark_time
        logger.debug(f"--------------------------------")
        logger.debug(f"Processing watermark: time={watermark_time}")
        self.timer_service.advance_watermark(watermark_time)
        self.collect(watermark)

    def _update_last_updated_accum_per_key(self, key: Any, window: Window, timestamp: Decimal, accum: Any):
        if key not in self.last_updated_accum_per_key:
            self.last_updated_accum_per_key[key] = {}
        self.last_updated_accum_per_key[key][window.name] = (accum, timestamp)

    def _on_event_timer(self, timers: List[EventTimer]):
        key = timers[0].key
        timestamp = timers[0].timestamp

        # opens = [t for t in timers if t.namespace[1] == WindowEventType.OPEN]
        triggers = [t for t in timers if t.namespace[1] == WindowEventType.TRIGGER]
        closes = [t for t in timers if t.namespace[1] == WindowEventType.CLOSE]

        # record = None
        # process in order: opens, triggers, closes
        # for timer in opens:
        #     assert timer.key == key
        #     assert timer.timestamp == timestamp
        #     window, event_type = timer.namespace

        #     logger.debug(f"--------------------------------")
        #     logger.debug(f"Processing event timer: key={key}, timestamp={timestamp}, window={window}, event_type={event_type}")
        #     accum = self._update_accumulator(key, window, timestamp, create=True)
        #     self._update_last_updated_accum_per_key(key, window, timestamp, accum)
        for timer in triggers:
            assert timer.key == key
            assert timer.timestamp == timestamp
            window, event_type = timer.namespace
            logger.debug(f"--------------------------------")
            logger.debug(f"Processing event timer: key={key}, timestamp={timestamp}, window={window}, event_type={event_type}")
            
            _, _, _, allowed_lateness = self.window_configs_by_name[window.name]
            if allowed_lateness is None:
                allowed_lateness = 0
            
                # config, _, _, _ = self.window_configs_by_name[window.name]

                # update all relevant window accums for this key until this timestamp
            windows_per_name = self.state.get_windows(key)
            windows = [w for w in windows_per_name.values() for w in w]
            # print(f"windows: {windows}")
            last_updated_accum = None
            for _window in windows:
                # print(f"1 updating accum for window [{_window.start_time}, {_window.end_time})")  
                if _window.start_time <= timestamp <= _window.end_time:
                    # print(f"2updating accum for window [{_window.start_time}, {_window.end_time})")  
                    accum = self._update_accumulator(key, _window, timestamp, create=False)
                    if _window.window_id == window.window_id:
                        last_updated_accum = accum

            # update last_updated_accum_per_key for this window name
            if last_updated_accum is not None:
                self._update_last_updated_accum_per_key(key, window, timestamp, last_updated_accum)

        record = self._gen_record_to_emit(key, timestamp)
        for timer in closes:
            assert timer.key == key
            assert timer.timestamp == timestamp
            window, event_type = timer.namespace
            logger.debug(f"--------------------------------")
            logger.debug(f"Processing event timer: key={key}, timestamp={timestamp}, window={window}, event_type={event_type}")
            
            # Window is closing
            self.state.remove_window(key, window.window_id)
            self.state.remove_accumulator(window.window_id)

            # TODO figure out how to cleanup last_updated_accum_per_key, considering we may use this for next window init
            logger.debug(f"Removed window: key={key}, window={window}")

        if record is not None:
            self.collect(record)
            logger.debug(f"Emitting result record for key={key} with event_time={record.event_time} with value {record.value}")
        else:
            logger.debug(f"Result record is None")
        
    def _update_accumulator(self, key: Any, window: Window, timestamp: Decimal, create: bool) -> Any:
        config, _, _, _ = self.window_configs_by_name[window.name]
         
        # Get aggregate function
        agg_func = RetractableAggregateRegistry.create(config.agg_type, config.agg_on_func)
         
        # Get current accumulator
        # if create:
        #     accumulator = agg_func.create_accumulator()
        # else:
        accumulator = self.state.get_accumulator(window.window_id)
        if accumulator is None:
            accumulator = agg_func.create_accumulator()
         
        event_count = 0
        events = self.state.get_events_for_window(key, window.window_id)
        for event_id, record, is_processed in events:
            if window.start_time <= record.event_time < window.end_time and record.event_time <= timestamp and not is_processed:
                agg_func.add(record.value, accumulator)
                event_count += 1
                self.state.mark_event_processed_for_window(key, window.window_id, event_id)
        
        # Update the accumulator
        self.state.update_accumulator(window.window_id, accumulator)
        s = 'Created' if create else 'Updated'
        logger.debug(f"{s} accumulator for window [{window.start_time}, {window.end_time}) with {event_count} new events from {len(events)} events at time {timestamp}, "
                    f"value: {accumulator}")
        
        return accumulator # TODO return result instead of accumulator
    
    def _gen_record_to_emit(self, key: Any, emit_time: Decimal) -> Record:
        window_results = {}
        windows_per_name = self.state.get_windows(key)
        for name, (config, _, _, allowed_lateness) in self.window_configs_by_name.items():

            agg_func = RetractableAggregateRegistry.create(config.agg_type, config.agg_on_func)
            
            # found = False
            # if name in self.last_updated_accum_per_key[key]:
            #     last_emitted_ts = self.last_updated_accum_per_key[key][name][1]
            #     acum = self.last_updated_accum_per_key[key][name][0]

            #     threshold = last_emitted_ts + duration_to_s(config.duration)
            #     if allowed_lateness is not None:
            #         threshold += allowed_lateness
                
            #     if threshold >= emit_time:
            #         window_results[name] = agg_func.get_result(acum)
            #         found = True

            # TODO check threshold
            # find earliest open for this emit_time
            found = False
            windows = windows_per_name.get(name, [])
            windows.sort(key=lambda x: x.start_time)
            window = None
            for _window in windows:
                if _window.start_time <= emit_time <= _window.end_time:
                    window = _window
                    found = True
                    break

            if found:
                accum = self.state.get_accumulator(window.window_id)
                window_results[name] = agg_func.get_result(accum)
            else:
                if config.agg_type in [AggregationType.SUM, AggregationType.COUNT, AggregationType.AVG]:
                    window_results[name] = 0
                elif config.agg_type in [AggregationType.MIN, AggregationType.MAX]:
                    window_results[name] = None

        if self.output_func is None:
            output_record = Record(
                value=window_results, 
                event_time=emit_time,
                source_emit_ts=None, # TODO
            )
        else:
            # Create a copy of the most recent record with the emission time
            template_record = Record(
                value=window_results,
                event_time=emit_time,
                source_emit_ts=None, # TODO
            )
            output_record = self.output_func(window_results, template_record)
        
        # TODO proper set stream name
        output_record.set_stream_name("test")     
        return output_record
        # self.collect(output_record)
    
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

    def _get_or_create_windows_for_event(self, key: Any, name: str, event_time: Decimal) -> List[Tuple[Window, bool]]:
        _, window_size, slide_interval, _ = self.window_configs_by_name[name]
        windows = []
        
        if slide_interval:
            # Check existing windows
            existing_windows = self.state.windows_per_key.get(key, {}).get(name, [])
            existing_starts = {}
            
            for window in existing_windows:
                existing_starts[window.start_time] = window

            assert len(existing_starts) == len(existing_windows)

            logger.debug(f"Found {len(existing_starts)} existing windows for key={key}, name={name}, event_time={event_time}")

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
                        windows.append((window, False))
                        logger.debug(f"Using existing window for key={key}, name={name}: [{window.start_time}, {window.end_time}), event_time={event_time}")
                    else:
                        # Create a new window
                        window = self._create_window(key, name, window_start, window_end)
                        windows.append((window, True))
                        logger.debug(f"Created new window for key={key}, name={name}: [{window.start_time}, {window.end_time}), event_time={event_time}")
                else:
                    logger.debug(f"Skipping window for key={key}, name={name} [{window_start}, {window_end}) as it doesn't contain event time {event_time}")
                
                window_start += slide_interval
                window_end = window_start + window_size
        else:
            # TODO what if two events with same event_time?
            # For event-based sliding, create a window starting at this event
            window_end = event_time + window_size
            logger.debug(f"Event-based window for key={key}, name={name}: event_time={event_time}, window=[{event_time}, {window_end})")
            window = self._create_window(key, name, event_time, window_end)
            windows.append((window, True))
        
        logger.debug(f"Returning {len(windows)} windows name: {name}for event: key={key}, event_time={event_time}")
        return windows

    def close(self):
        super().close()
        logger.info(f"Closing WindowOperator. Stats: late_events_dropped={self.late_events_dropped}, "
                   f"late_but_allowed_events={self.late_but_allowed_events}")
        self.state.clear()
