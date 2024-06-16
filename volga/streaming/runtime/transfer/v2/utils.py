from typing import Optional


def str_to_bytes(s: str, pad_to_size: Optional[int] = None) -> bytes:
    if pad_to_size is None:
        return s.encode('utf-8')
    diff = pad_to_size - len(s) # 1 char is 1 byte
    if diff < 0:
        raise ValueError('Unable to pad string')
    s += diff * ' '
    return s.encode('utf-8')


def bytes_to_str(b: bytes, strip_padding: bool = False) -> str:
    s = b.decode('utf-8')
    return s.strip() if strip_padding else s


def int_to_bytes(i: int, buff_size: int) -> bytes:
    return i.to_bytes(buff_size, 'big')


def bytes_to_int(b: bytes) -> int:
    return int.from_bytes(b, 'big')
