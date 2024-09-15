from math import ceil
from typing import Collection
import time


def collection_chunk_at_index(vals: Collection, num_chunks, chunk_index) -> Collection:
    length = ceil(len(vals) / num_chunks)
    start = chunk_index * length
    end = min(len(vals), start + length)
    return vals[start: end]


def now_ts_ms() -> int:
    ns = time.time_ns()
    ms = int(ns /(10 ** 6))
    return ms


def ms_to_s(ms: int) -> int:
    return int(ms/1000)
