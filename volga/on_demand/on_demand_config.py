from typing import Union, Type, Dict, Any
import importlib
from pydantic import BaseModel, validator
from volga.on_demand.data_connector import OnDemandDataConnector

class DataConnectorConfig(BaseModel):
    connector_class: Union[str, Type[OnDemandDataConnector]]
    connector_args: Dict[str, Any] = {}

    class Config:
        arbitrary_types_allowed = True

    @validator('connector_class')
    def validate_connector_class(cls, v):
        if isinstance(v, str):
            # Convert string to actual class
            try:
                module_path, class_name = v.rsplit('.', 1)
                module = importlib.import_module(module_path)
                connector_class = getattr(module, class_name)
                
                if not isinstance(connector_class, type) or not issubclass(connector_class, OnDemandDataConnector):
                    raise ValueError(f"Class {v} must be a subclass of OnDemandDataConnector")
                
                return connector_class  # Return the actual class, not the string
            except (ImportError, AttributeError, ValueError) as e:
                raise ValueError(f"Failed to load connector class {v}: {str(e)}")
        return v 