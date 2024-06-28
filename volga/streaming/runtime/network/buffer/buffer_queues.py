import threading
import time
from collections import deque
from typing import List, Dict, Optional

import simplejson

from volga.streaming.runtime.network.buffer.buffer_pool import BufferPool
from volga.streaming.runtime.network.buffer.buffer import serialize, append_to_buffer, Buffer
from volga.streaming.runtime.network.buffer.buffering_policy import BufferingPolicy, PeriodicPartialFlushPolicy, \
    BufferPerMessagePolicy
from volga.streaming.runtime.network.channel import Channel, ChannelMessage


# TODO test different buffering policies
class BufferQueues:
    def __init__(
        self,
        channels: List[Channel],
        buffer_pool: Optional[BufferPool],
        buffer_size: int,
        buffering_policy: BufferingPolicy
    ):
        self._buffer_size = buffer_size
        self._msg_id_seq = {c.channel_id: 0 for c in channels}
        self._buffer_id_seq = {c.channel_id: 0 for c in channels}
        self._buffer_pool = buffer_pool
        self._buffer_queues: Dict[str, deque] = {c.channel_id: deque() for c in channels}
        self._locks = {c.channel_id: threading.Lock() for c in channels}
        self._last_update_ts = {c.channel_id: None for c in channels}
        self._buffering_policy = buffering_policy

    def append_msg(self, message: ChannelMessage, channel_id: str) -> bool: # backpressure or not
        msg_str = simplejson.dumps(message)
        lock = self._locks[channel_id]
        lock.acquire()
        msg_id = self._msg_id_seq[channel_id]
        buffer_id = self._buffer_id_seq[channel_id]
        buffer_queue = self._buffer_queues[channel_id]

        buffer = None
        if len(buffer_queue) == 0 or isinstance(self._buffering_policy, BufferPerMessagePolicy):
            buffer = serialize(channel_id, buffer_id, msg_str, msg_id, self._buffer_size, with_header=True)
        else:
            last_buffer = buffer_queue[-1]
            msg_payload = serialize(channel_id, buffer_id, msg_str, msg_id, self._buffer_size, with_header=False)
            if self._buffer_pool is not None and not self._buffer_pool.try_acquire(len(msg_payload)):
                # not enough memory for buffering
                return False

            # append to existing buffer if enough space or create new buffer
            if self._buffer_size - len(last_buffer) >= len(msg_payload):
                updated_last_buffer = append_to_buffer(last_buffer, msg_payload, self._buffer_size)
                buffer_queue[-1] = updated_last_buffer
            else:
                buffer = serialize(channel_id, buffer_id, msg_str, msg_id, self._buffer_size, with_header=True)

        if buffer is not None:
            # new buffer
            # acquire memory only on new buffer
            if self._buffer_pool is None or self._buffer_pool.try_acquire(len(buffer)):
                buffer_queue.append(buffer)
                self._msg_id_seq[channel_id] += 1
                self._buffer_id_seq[channel_id] += 1
                self._last_update_ts[channel_id] = time.time()
                lock.release()
                return True
            else:
                lock.release()
                return False
        else:
            # append to existing buffer, already checked memory capacity
            self._msg_id_seq[channel_id] += 1
            self._last_update_ts[channel_id] = time.time()
            lock.release()
            return True

    def pop(self, channel_id) -> Optional[Buffer]:
        lock = self._locks[channel_id]
        lock.acquire()
        buffer_queue = self._buffer_queues[channel_id]
        if len(buffer_queue) == 0:
            lock.release()
            return None

        # make sure PeriodicPartialFlushPolicy executes properly
        if isinstance(self._buffering_policy, PeriodicPartialFlushPolicy) \
                and len(buffer_queue) == 1 \
                and time.time() - self._last_update_ts[channel_id] < self._buffering_policy.flush_period_s:
            lock.release()
            return None

        buffer = buffer_queue.popleft()
        lock.release()
        return buffer
