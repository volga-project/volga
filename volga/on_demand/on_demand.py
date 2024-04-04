from typing import List, Callable

from volga.data.api.dataset.dataset import Dataset


# decorator
def on_demand(
    deps: List[Dataset]
) -> Callable:
    pass