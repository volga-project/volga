import collections
from typing import List, Optional, Dict

import numpy as np


class Hist:

    def __init__(self, buckets: List, hist: Optional[Dict] = None):
        assert sorted(buckets) == buckets
        self.buckets = buckets
        if hist is None:
            self.hist = collections.OrderedDict()
            for b in buckets:
                self.hist[b] = 0
        else:
            self.hist = hist

    def observe(self, value):
        if value < 0:
            raise RuntimeError('Values are expected to be greater than zero')
        if value == 0:
            self.hist[self.buckets[0]] += 1
            return

        for i in range(len(self.buckets)):
            if i == 0:
                prev = 0
            else:
                prev = self.buckets[i - 1]
            cur = self.buckets[i]
            if prev < value <= cur:
                self.hist[cur] += 1
                return

            if i == len(self.buckets) - 1 and value > cur:
                self.hist[cur] += 1
                return

        raise RuntimeError(f'Unable to find a bucket for value {value}')

    @staticmethod
    def merge(hists: List['Hist']) -> 'Hist':
        h = hists[0].hist
        buckets = hists[0].buckets
        for i in range(1, len(hists)):
            hist = hists[i]
            assert buckets == hist.buckets
            for b in buckets:
                h[b] += hist.hist[b]
        return Hist(buckets, h)

    def percentiles(self, percentiles: List[float]) -> List[int]:
        res = []
        vals = list(self.hist.values())
        b = np.cumsum(vals)/np.sum(vals) * 100
        for p in percentiles:
            bucket_index = len(b[b <= p])
            bucket = self.buckets[bucket_index]
            res.append(bucket)
        return res

    def avg(self) -> float:
        s = sum(list(self.hist.values()))
        w = 0
        for b in self.hist:
            w += b * self.hist[b]

        return w/s
