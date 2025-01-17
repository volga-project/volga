import time

from numpy.linalg import norm
import numpy as np


def cosine_similarity(a: np.ndarray, b: np.ndarray):
    if a is None or b is None:
        return -1.0

    return np.dot(a, b) / (norm(a) * norm(b))

n = 10
v_size = 1024*1024*100

vs1 = [np.random.uniform(low=-1, high=1, size=(v_size,)) for _ in range(n)]
vs2 = [np.random.uniform(low=-1, high=1, size=(v_size,)) for _ in range(n)]

start_ts = time.time()
for i in range(n):
    v1 = vs1[i]
    v2 = vs2[i]
    c = cosine_similarity(v1, v2)
end_ts = time.time()

dur = end_ts - start_ts
p = dur/n
print(f'Finished in {dur}s, {p}s per iter')