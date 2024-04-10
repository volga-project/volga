from typing import List, Callable

from volga.api.dataset.dataset import Dataset


# decorator
def on_demand(
    deps: List[Dataset]
) -> Callable:
    pass