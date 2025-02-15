import json
from abc import ABC
from typing import List, Callable, Tuple, Dict, Optional, Any, Type

from pydantic import BaseModel


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


# TODO rename to DataConnector?
# class OnDemandComputeModule(ABC):

#     async def init(self):
#         pass

#     async def close(self):
#         pass

#     def parse_request(self):
#         pass

#     def db_query_map(self) -> Dict[str, Callable]:
#         return {
#             'latest': self._fetch_latest,
#             'range': self._fetch_range,
#             'ann': self._ann
#         }

#     async def _fetch_latest(self, *args, **kwargs) -> Entity:
#         return Entity()

#     async def _fetch_range(self, *args, **kwargs) -> List[Entity]:
#         return [Entity()]

#     async def _ann(self, *args, **kwargs) -> List[Entity]:
#         return [Entity()]

# # decorator
# def on_demand(
#     entity: Type[Entity],
#     deps: Dict[str, Tuple[Entity, str, List[str]]],
# ) -> OnDemandSpec:
#     return OnDemandSpec()


# class SampleEntity1(Entity):
#     pass

# class SampleEntity2(Entity):
#     pass

# class SampleEntity3(Entity):
#     pass

# @on_demand(
#     entity=SampleEntity3,
#     deps={},
# )
# def two_input_on_demand_feature(first: Entity, second: Entity) -> Entity:
#     return Entity()