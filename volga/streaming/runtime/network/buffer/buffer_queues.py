import threading
import time
from collections import deque
from typing import List, Dict, Optional, Tuple

import simplejson

from volga.streaming.runtime.network.buffer.buffer_memory_tracker import BufferMemoryTracker
from volga.streaming.runtime.network.buffer.buffer import serialize, append_to_buffer, Buffer, get_buffer_id
from volga.streaming.runtime.network.buffer.buffering_policy import BufferingPolicy, PeriodicPartialFlushPolicy, \
    BufferPerMessagePolicy
from volga.streaming.runtime.network.channel import Channel, ChannelMessage


class BufferQueues:
    def __init__(
        self,
        channels: List[Channel],
        buffer_memory_tracker: Optional[BufferMemoryTracker],
        buffer_size: int,
        buffering_policy: BufferingPolicy
    ):
        self._buffer_size = buffer_size
        self._msg_id_seq = {c.channel_id: 0 for c in channels}
        self._buffer_id_seq = {c.channel_id: 0 for c in channels}
        self._buffer_memory_tracker = buffer_memory_tracker
        self._buffer_queues: Dict[str, deque] = {c.channel_id: deque() for c in channels}

        self._locks = {c.channel_id: threading.Lock() for c in channels}

        # indicates when the queue was last updated
        self._last_update_ts = {c.channel_id: None for c in channels}
        self._buffering_policy = buffering_policy

        # index to schedule buffer
        self._schedule_index = {c.channel_id: 0 for c in channels}

        # if a buffer is flushed due to timeout, we need to indicate that next messages should go into new buffer
        self._should_cut_buffer = {c.channel_id: False for c in channels}

        # keeps track of pop requests (in case they come out of order)
        self._pops = {c.channel_id: {} for c in channels}

    def append_msg(self, message: ChannelMessage, channel_id: str) -> Optional[Tuple[int, int]]: # success or backpressure
        lock = self._locks[channel_id]
        with lock:
            msg_str = simplejson.dumps(message)
            buffer_queue = self._buffer_queues[channel_id]
            msg_id = self._msg_id_seq[channel_id]
            buffer_id = self._buffer_id_seq[channel_id]

            msg_payload = serialize(channel_id, buffer_id, msg_str, msg_id, self._buffer_size, with_header=False)
            needs_new_buffer = len(buffer_queue) == 0 \
                       or isinstance(self._buffering_policy, BufferPerMessagePolicy) \
                       or self._buffer_size - len(buffer_queue[-1]) < len(msg_payload)

            cut_buffer = self._should_cut_buffer[channel_id]

            if not needs_new_buffer and not cut_buffer:
                last_buffer = buffer_queue[-1]
                updated_last_buffer = append_to_buffer(last_buffer, msg_payload, self._buffer_size)
                buffer_queue[-1] = updated_last_buffer
                res = (self._msg_id_seq[channel_id], self._buffer_id_seq[channel_id])
                self._msg_id_seq[channel_id] += 1
                self._last_update_ts[channel_id] = time.time()
                return res

            # check available memory
            has_mem = self._buffer_memory_tracker is None or self._buffer_memory_tracker.try_acquire(channel_id)
            if not has_mem:
                return None

            buffer = serialize(channel_id, buffer_id, msg_str, msg_id, self._buffer_size, with_header=True)
            buffer_queue.append(buffer)
            res = (self._msg_id_seq[channel_id], self._buffer_id_seq[channel_id])
            self._msg_id_seq[channel_id] += 1
            self._buffer_id_seq[channel_id] += 1
            self._last_update_ts[channel_id] = time.time()
            self._should_cut_buffer[channel_id] = False
            return res

    # schedule next element in queue for send without popping.
    def schedule_next(self, channel_id: str) -> Optional[Buffer]:
        lock = self._locks[channel_id]
        with lock:
            buffer_queue = self._buffer_queues[channel_id]
            num_schedulable = len(buffer_queue) - self._schedule_index[channel_id]
            if num_schedulable < 0:
                raise RuntimeError('buffer queue schedule index can not be > queue length')
            if num_schedulable == 0:
                return None

            if num_schedulable == 1 and isinstance(self._buffering_policy, PeriodicPartialFlushPolicy):
                if time.time() - self._last_update_ts[channel_id] >= self._buffering_policy.flush_period_s:
                    buffer = buffer_queue[self._schedule_index[channel_id]]
                    self._schedule_index[channel_id] += 1
                    self._should_cut_buffer[channel_id] = True
                    return buffer
                else:
                    return None

            buffer = buffer_queue[self._schedule_index[channel_id]]
            self._schedule_index[channel_id] += 1
            return buffer

    # remove element from queue and release memory
    # this records pop request first and performs actual popping only if there are requests in-order
    def pop(self, channel_id: str, buffer_id: int):
        lock = self._locks[channel_id]
        with lock:
            self._pops[channel_id][buffer_id] = True
            buffer_queue = self._buffer_queues[channel_id]
            if len(buffer_queue) == 0:
                raise RuntimeError('Can not pop empty queue')
            while len(buffer_queue) != 0 and get_buffer_id(buffer_queue[0]) in self._pops[channel_id]:
                buffer = buffer_queue.popleft()
                self._schedule_index[channel_id] -= 1
                if self._schedule_index[channel_id] < 0:
                    raise RuntimeError('buffer queue schedule index can not be < 1')
                if self._buffer_memory_tracker is not None:
                    self._buffer_memory_tracker.release(channel_id)
                del self._pops[channel_id][get_buffer_id(buffer)]

