from pydantic import BaseModel

from volga.streaming.runtime.network_deprecated.buffer.buffer import DEFAULT_BUFFER_SIZE
from volga.streaming.runtime.network_deprecated.buffer.buffer_memory_tracker import DEFAULT_CAPACITY_PER_IN_CHANNEL, \
    DEFAULT_CAPACITY_PER_OUT


class BufferingConfig(BaseModel):
    buffer_size: int = DEFAULT_BUFFER_SIZE
    capacity_per_in_channel = DEFAULT_CAPACITY_PER_IN_CHANNEL
    capacity_per_out = DEFAULT_CAPACITY_PER_OUT
