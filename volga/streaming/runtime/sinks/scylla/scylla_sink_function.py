import asyncio
import json
import threading
from typing import Optional, List, Callable, Dict

from aiologic import Lock

from volga.storage.scylla.api import ScyllaPyHotFeatureStorageApi
from volga.streaming.api.context.runtime_context import RuntimeContext
from volga.streaming.api.function.function import SinkFunction


class ScyllaHotFeatureSinkFunction(SinkFunction):

    def __init__(self, key_value_extractor: Callable, max_in_flight_writes: int = 60000, max_retries: int = 4, contact_points: Optional[List[str]] = None):
        self.session = None
        self.key_value_extractor = key_value_extractor
        self.write_tasks_state = {} # writes per key
        self.max_in_flight_writes = max_in_flight_writes
        self.max_retries = max_retries

        self.api = ScyllaPyHotFeatureStorageApi(contact_points)
        self.scheduler_thread = None
        self.async_writer_thread = None
        self.event_loop = None

        self.running = False

        self.has_capacity = threading.Event()
        self.has_capacity.set()

        self.has_schedulable = threading.Event()
        self.has_schedulable.set()

        self.lock = Lock()

    def open(self, runtime_context: RuntimeContext):
        # start event loop thread, run api init
        # asyncio.run(self.api.init())
        pass

    def _schedule_loop(self):
        pass

    def sink(self, value):
        while self.running:
            self.has_capacity.wait(timeout=1)
        if not self.running:
            return

        keys, values = self.key_value_extractor(value)
        keys_json = json.dumps(keys)
        self.lock.green_acquire()
        # update write state

        capacity = self._get_num_in_flight()
        assert capacity <= self.max_in_flight_writes
        if capacity == self.max_in_flight_writes:
            self.has_capacity.clear()
        self.lock.green_release()

    def _get_num_in_flight(self) -> int:
        return 0


    def close(self):
        asyncio.run(self.api.close())