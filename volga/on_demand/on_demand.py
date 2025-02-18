import json
from abc import ABC
from typing import List, Callable, Tuple, Dict, Optional, Any, Type

from pydantic import BaseModel


class OnDemandArgs(BaseModel):
    feature_name: str
    keys: Dict[str, Any]
    udf_args: Optional[Dict[str, Any]] = None


class OnDemandRequest(BaseModel):
    args: List[OnDemandArgs]

    def get_feature_keys(self) -> Dict[str, Dict[str, Any]]:
        """Get keys for all features in the request"""
        return {
            arg.feature_name: arg.keys
            for arg in self.args
        }

    def get_udf_args(self) -> Dict[str, Dict[str, Any]]:
        """Get UDF args for all features that have them"""
        return {
            arg.feature_name: arg.udf_args
            for arg in self.args
            if arg.udf_args is not None
        }

    def get_target_features(self) -> List[str]:
        """Get list of all feature names in the request"""
        return [arg.feature_name for arg in self.args]


class OnDemandResponse(BaseModel):
    feature_values: Dict[str, Any]
    server_id: int
