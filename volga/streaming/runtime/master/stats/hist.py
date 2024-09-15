from typing import List, Optional, Dict

import numpy as np


class Hist:

    def __init__(self, buckets: List, hist: Optional[Dict] = None):
        assert sorted(buckets) == buckets
        self.buckets = buckets
        if hist is None:
            self.hist = {b: 0 for b in buckets}
        else:
            self.hist = hist

    def observe(self, value):
        for b in self.buckets:
            if value >= b:
                self.hist += 1

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
        for p in percentiles:
            v = np.percentile(list(self.hist.values()), p)
            bucket = list(self.hist.keys())[list(self.hist.values()).index(v)]
            res.append(bucket)
        return res
