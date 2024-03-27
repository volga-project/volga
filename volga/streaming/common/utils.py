from math import ceil
from typing import Collection


def collection_chunk_at_index(vals: Collection, num_chunks, chunk_index) -> Collection:
    length = ceil(len(vals) / num_chunks)
    start = chunk_index * length
    end = min(len(vals), start + length)
    return vals[start: end]