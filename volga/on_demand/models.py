import json
from abc import ABC
from typing import List, Callable, Tuple, Dict, Optional, Any, Type

from pydantic import BaseModel, Field  

from volga.api.on_demand import OnDemandFeature
from volga.api.pipeline import PipelineFeature

class OnDemandRequest(BaseModel):
    target_features: List[str] = Field(description="List of feature names to compute")
    feature_keys: Dict[str, List[Dict[str, Any]]] = Field(description="Keys for each feature, indexed by feature name")
    query_args: Optional[Dict[str, Dict[str, Any]]] = Field(default=None, description="Query arguments for pipeline features")
    udf_args: Optional[Dict[str, Dict[str, Any]]] = Field(default=None, description="UDF arguments for on-demand features")

    def validate_request(
        self,
        features: Dict[str, Any],
        query_params: Dict[str, List[str]]
    ) -> None:
        """
        Validate that all required dependencies and arguments are present.
        
        Args:
            features: Dictionary of all features from FeatureRepository
            query_params: Dictionary mapping query names to their required parameters
        """
        # Validate target features exist
        for feature_name in self.target_features:
            if feature_name not in features:
                raise ValueError(f"Feature {feature_name} not found")

        # Build complete set of required features (including dependencies)
        required_features = set()
        to_process = self.target_features.copy()
        
        while to_process:
            feature_name = to_process.pop()
            if feature_name in required_features:
                continue
                
            feature = features.get(feature_name)
            if feature is None:
                raise ValueError(f"Feature {feature_name} not found")
                
            required_features.add(feature_name)
            
            # Add dependencies to processing queue
            for dep_arg in feature.dep_args:
                dep_name = dep_arg.get_name()
                if dep_name not in required_features:
                    to_process.append(dep_name)

        # Validate feature keys
        if not self.feature_keys:
            raise ValueError("Feature keys are required")

        # Validate keys format and non-empty lists
        for feature_name, keys_list in self.feature_keys.items():
            if not isinstance(keys_list, list):
                raise ValueError(f"Keys for feature {feature_name} must be a list of dictionaries")
            if not keys_list:
                raise ValueError(f"Empty keys list provided for feature {feature_name}")
            if not all(isinstance(k, dict) for k in keys_list):
                raise ValueError(f"All keys for feature {feature_name} must be dictionaries")

        # Validate keys are provided for correct features
        for feature_name, keys_list in self.feature_keys.items():
            if feature_name not in features:
                raise ValueError(f"Keys provided for non-existent feature {feature_name}")
            
            feature = features[feature_name]
            
            # For pipeline features
            if isinstance(feature, PipelineFeature):
                has_ondemand_dependent = any(
                    isinstance(features[f], OnDemandFeature) and 
                    any(dep.get_name() == feature_name for dep in features[f].dep_args)
                    for f in required_features if isinstance(features[f], OnDemandFeature)
                )
                if has_ondemand_dependent:
                    raise ValueError(
                        f"Keys should not be provided for pipeline feature {feature_name} "
                        "that has on-demand dependents"
                    )
            
            # For on-demand features
            elif isinstance(feature, OnDemandFeature):
                has_pipeline_dep = any(
                    dep.get_name() in features and 
                    isinstance(features[dep.get_name()], PipelineFeature)
                    for dep in feature.dep_args
                )
                if not has_pipeline_dep:
                    raise ValueError(
                        f"Keys should not be provided for on-demand feature {feature_name} "
                        "that does not directly depend on pipeline features"
                    )

        # Validate all required keys are present
        for feature_name in required_features:
            feature = features[feature_name]
            # if isinstance(feature, PipelineFeature):
            # TODO validate at least on dependent on-demand has keys
            #     has_ondemand_dependent = any(
            #         isinstance(features[f], OnDemandFeature) and 
            #         any(dep.get_name() == feature_name for dep in features[f].dep_args)
            #         for f in required_features if isinstance(features[f], OnDemandFeature)
            #     )
            #     if not has_ondemand_dependent and feature_name not in self.feature_keys:
            #         raise ValueError(f"Missing keys for pipeline feature {feature_name}")
            
            if isinstance(feature, OnDemandFeature):
                has_pipeline_dep = any(
                    dep.get_name() in features and 
                    isinstance(features[dep.get_name()], PipelineFeature)
                    for dep in feature.dep_args
                )
                if has_pipeline_dep and feature_name not in self.feature_keys:
                    raise ValueError(
                        f"Missing keys for on-demand feature {feature_name} "
                        "that directly depends on pipeline features"
                    )

        # Validate query args
        if self.query_args:
            for feature_name, args in self.query_args.items():
                if feature_name not in features:
                    raise ValueError(f"Query args provided for non-existent feature {feature_name}")
                
                feature = features[feature_name]
                if isinstance(feature, OnDemandFeature):
                    # Check if feature has any pipeline dependencies that need query args
                    has_pipeline_deps = any(
                        dep.get_name() in features and 
                        isinstance(features[dep.get_name()], PipelineFeature)
                        for dep in feature.dep_args
                    )
                    if not has_pipeline_deps:
                        raise ValueError(
                            f"Query args provided for feature without pipeline dependencies"
                        )
                    
                    # Validate required parameters for pipeline dependencies
                    for dep_arg in feature.dep_args:
                        dep_name = dep_arg.get_name()
                        if dep_name in features and isinstance(features[dep_name], PipelineFeature):
                            query_name = dep_arg.query_name or 'latest'
                            required_params = query_params.get(query_name, [])
                            if required_params:
                                missing_params = [
                                    param for param in required_params
                                    if param not in args
                                ]
                                if missing_params:
                                    raise ValueError(
                                        f"Missing required query parameters for feature {feature_name}: {missing_params}"
                                    )

        # Validate UDF args for on-demand features
        if self.udf_args:
            for feature_name, args in self.udf_args.items():
                if feature_name not in features:
                    raise ValueError(f"UDF args provided for non-existent feature {feature_name}")
                feature = features[feature_name]
                if not isinstance(feature, OnDemandFeature):
                    raise ValueError(f"UDF args provided for non-on-demand feature {feature_name}")
                
                # Validate all provided UDF args are valid
                if feature.udf_args_names:
                    invalid_args = set(args.keys()) - set(feature.udf_args_names)
                    if invalid_args:
                        raise ValueError(
                            f"Invalid UDF arguments for feature {feature_name}: {invalid_args}"
                        )
                elif args:
                    raise ValueError(f"UDF args provided for feature {feature_name} that takes no arguments")

class OnDemandResponse(BaseModel):
    results: Dict[str, List[Optional[List[Any]]]]
    server_id: int
