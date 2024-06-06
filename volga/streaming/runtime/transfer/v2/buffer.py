from typing import List

Buffer = bytes


def msg_to_buffers(s: str) -> List[Buffer]:
    return [s.encode('utf-8')]


def buffer_id(buffer: Buffer) -> int:
    return 0


def data_length(buffer: Buffer) -> int:
    return 0


def msg_id(buffer: Buffer) -> int:
    return 0
