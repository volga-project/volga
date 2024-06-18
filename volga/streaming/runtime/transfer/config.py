from typing import Optional

from pydantic import BaseModel


class ZMQConfig(BaseModel):
    LINGER: Optional[int]
    SNDHWM: Optional[int]
    RCVHWM: Optional[int]
    SNDBUF: Optional[int]
    RCVBUF: Optional[int]


DEFAULT_ZMQ_CONFIG = ZMQConfig(
    LINGER=0,
    SNDHWM=1000,
    RCVHWM=1000,
    SNDBUF=32 * 1024,
    RCVBUF=32 * 1024,
)

