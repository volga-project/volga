import asyncio
import json
import threading
import time
from typing import Optional, List, Callable, Dict

from aiologic import Lock

from volga.storage.scylla.api import AcsyllaHotFeatureStorageApi
from volga.streaming.api.context.runtime_context import RuntimeContext
from volga.streaming.api.function.function import SinkFunction

from cassandra.timestamps import MonotonicTimestampGenerator


class ScyllaHotFeatureSinkFunction(SinkFunction):

    # TODO implement retries
    def __init__(self, feature_name: str, key_value_extractor: Callable, max_in_flight: int = 1000, max_retries: int = 4, contact_points: Optional[List[str]] = None):
        self.session = None
        self.ts_gen = None
        self.feature_name = feature_name
        self.key_value_extractor = key_value_extractor
        self.in_flight = {} # writes per key
        self.max_in_flight = max_in_flight
        self.max_retries = max_retries

        self.api = AcsyllaHotFeatureStorageApi(contact_points) # TODO configure num io threads
        self.async_writer_thread = None
        self.event_loop = None

        self.running = False
        self.has_capacity = None

    def open(self, runtime_context: RuntimeContext):
        # start event loop thread, run api init
        self.async_writer_thread = threading.Thread(target=self._run_event_loop)
        self.ts_gen = MonotonicTimestampGenerator()
        self.has_capacity = threading.Event()
        self.has_capacity.set()
        self.running = True
        self.async_writer_thread.start()
        time.sleep(0.5)
        assert self.event_loop is not None
        f = asyncio.run_coroutine_threadsafe(self.api.init(), self.event_loop)
        try:
            f.result(timeout=5)
        except TimeoutError:
            raise RuntimeError(f'Unable to init async api')

    def _run_event_loop(self):
        loop = asyncio.new_event_loop()
        self.event_loop = loop
        self.event_loop.run_forever()

    def _insert_done_callback(self, _fut):
        del self.in_flight[_fut]
        if len(self.in_flight) < self.max_in_flight:
            self.has_capacity.set()

    def sink(self, value):
        self.has_capacity.wait() # TODO put in a while loop with timeout
        if not self.running:
            return

        keys, values = self.key_value_extractor(value)
        ts = self.ts_gen()
        coro = self.api.insert(self.feature_name, keys, values, ts)
        f = asyncio.run_coroutine_threadsafe(coro, self.event_loop)
        self.in_flight[f] = ts
        f.add_done_callback(self._insert_done_callback)
        num_in_flight = len(self.in_flight)
        assert num_in_flight <= self.max_in_flight
        if num_in_flight == self.max_in_flight:
            self.has_capacity.clear()

    def close(self):
        self.running = False
        self.event_loop.call_soon_threadsafe(self.event_loop.stop)
        self.async_writer_thread.join(timeout=5)