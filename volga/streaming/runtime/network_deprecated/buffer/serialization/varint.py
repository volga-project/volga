from typing import Tuple


def _byte(b):
    return bytes((b,))


def encode_int(number) -> bytes:
    buf = b''
    while True:
        towrite = number & 0x7f
        number >>= 7
        if number:
            buf += _byte(towrite | 0x80)
        else:
            buf += _byte(towrite)
            break
    return buf


def decode_bytes(b: bytes, start: int) -> Tuple[int, int]:
    shift = 0
    result = 0
    pos = start
    while True:
        i = ord(b[pos: pos + 1])
        result |= (i & 0x7f) << shift
        shift += 7
        pos += 1
        if not (i & 0x80):
            break

    return result, pos
