from typing import Optional

import zmq

from volga.streaming.runtime.network.config import NetworkConfig, ZMQConfig


def str_to_bytes(s: str, pad_to_size: Optional[int] = None) -> bytes:
    b = s.encode('utf-8')
    if pad_to_size is None:
        return b
    diff = pad_to_size - len(b)
    if diff < 0:
        raise ValueError('Unable to pad string')
    b += diff * b' '
    return b


def bytes_to_str(b: bytes, strip_padding: bool = False) -> str:
    s = b.decode('utf-8')
    return s.strip() if strip_padding else s


def int_to_bytes(i: int, buff_size: int) -> bytes:
    return i.to_bytes(buff_size, 'big')


def bytes_to_int(b: bytes) -> int:
    return int.from_bytes(b, 'big')


def configure_socket(socket: zmq.Socket, zmq_config: ZMQConfig):
    if zmq_config.LINGER is not None:
        socket.setsockopt(zmq.LINGER, zmq_config.LINGER)
    if zmq_config.SNDHWM is not None:
        socket.setsockopt(zmq.SNDHWM, zmq_config.SNDHWM)
    if zmq_config.RCVHWM is not None:
        socket.setsockopt(zmq.RCVHWM, zmq_config.RCVHWM)
    if zmq_config.SNDBUF is not None:
        socket.setsockopt(zmq.SNDBUF, zmq_config.SNDBUF)
    if zmq_config.RCVBUF is not None:
        socket.setsockopt(zmq.RCVBUF, zmq_config.RCVBUF)
