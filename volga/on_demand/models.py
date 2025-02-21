import json
from abc import ABC
from typing import List, Callable, Tuple, Dict, Optional, Any, Type

from pydantic import BaseModel, Field  

from volga.api.on_demand import OnDemandFeature
from volga.api.pipeline import PipelineFeature

class OnDemandRequest(BaseModel):
    target_features: List[str] = Field(description="List of feature names to compute")
    feature_keys: Dict[str, Dict[str, Any]] = Field(description="Keys for each feature, indexed by feature name")
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
        if self.feature_keys:
            for feature_name, keys in self.feature_keys.items():
                if feature_name not in features:
                    raise ValueError(f"Keys provided for non-existent feature {feature_name}")
                
                feature = features[feature_name]
                
                # For pipeline features
                if isinstance(feature, PipelineFeature):
                    # Check if it has any on-demand dependents
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
                    # Check if it directly depends on a pipeline feature
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

        # Validate required keys are present
        for feature_name in required_features:
            feature = features[feature_name]
            
            # Pipeline feature with no on-demand dependents needs keys
            if isinstance(feature, PipelineFeature):
                has_ondemand_dependent = any(
                    isinstance(features[f], OnDemandFeature) and 
                    any(dep.get_name() == feature_name for dep in features[f].dep_args)
                    for f in required_features if isinstance(features[f], OnDemandFeature)
                )
                if not has_ondemand_dependent and feature_name not in self.feature_keys:
                    raise ValueError(f"Missing keys for pipeline feature {feature_name}")
            
            # On-demand feature that directly depends on pipeline needs keys
            elif isinstance(feature, OnDemandFeature):
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

        # Validate query args for on-demand features with pipeline dependencies
        if self.query_args:
            for feature_name, args in self.query_args.items():
                if feature_name not in features:
                    raise ValueError(f"Query args provided for non-existent feature {feature_name}")
                feature = features[feature_name]
                if not isinstance(feature, OnDemandFeature):
                    raise ValueError(f"Query args can only be provided for on-demand features, got {feature_name}")
                
                # Check if this feature has any pipeline dependencies with queries that require params
                has_pipeline_deps = any(
                    dep_arg.get_name() in features and 
                    isinstance(features[dep_arg.get_name()], PipelineFeature) and
                    query_params.get(feature.query_names.get(dep_arg.get_name(), 'latest'), [])
                    for dep_arg in feature.dep_args
                )
                if not has_pipeline_deps:
                    raise ValueError(f"Query args provided for feature {feature_name} with no pipeline dependencies requiring parameters")

        # Validate required query args are present for features that need them
        for feature_name in required_features:
            feature = features[feature_name]
            if isinstance(feature, OnDemandFeature):
                # Check if feature has pipeline dependencies with queries that require params
                for dep_arg in feature.dep_args:
                    dep_name = dep_arg.get_name()
                    if (dep_name in features and 
                        isinstance(features[dep_name], PipelineFeature)):
                        query_name = feature.query_names.get(dep_name, 'latest')
                        required_params = query_params.get(query_name, [])
                        if required_params:
                            if not self.query_args or feature_name not in self.query_args:
                                raise ValueError(f"Missing required query args for feature {feature_name}")
                            missing_params = [
                                param for param in required_params
                                if param not in self.query_args[feature_name]
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
    results: Dict[str, Any]
    server_id: int
