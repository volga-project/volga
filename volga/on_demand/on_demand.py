import json
from typing import List, Callable, Tuple, Dict, Optional, Any

from pydantic import BaseModel

from volga.api.dataset.dataset import Dataset


class OnDemandArgs(BaseModel):
    feature_name: str
    serve_or_udf: bool

    # if serve
    keys: Optional[Dict[str, Any]]

    # if udf
    dep_features_keys: Optional[List[Tuple[str, Dict[str, Any]]]] # list of (dep_feature_name, dep_feature_keys)
    udf_args: Optional[Dict]


class OnDemandRequest(BaseModel):
    args: List[OnDemandArgs]


class OnDemandSpec(BaseModel):
    feature_name: str
    udf: Callable
    udf_dep_features_args_names: Dict[str, str] # how udf arg name maps to feature name
    udf_args_names: Optional[List[str]] # args provided by user request


class FeatureValue(BaseModel):
    keys: Dict[str, Any]
    values: Dict[str, Any]

    @staticmethod
    def from_raw(v: Dict) -> 'FeatureValue':
        keys = {}
        values = {}

        if len(v) != 0:
            keys = json.loads(v['keys_json'])
            values = json.loads(v['values_json'])

        return FeatureValue(
            keys=keys,
            values=values,
        )


class OnDemandResponse(BaseModel):
    feature_values: Dict[str, FeatureValue]
    server_id: int


# decorator
def on_demand(
    deps: List[Dataset]
) -> Callable:
    pass