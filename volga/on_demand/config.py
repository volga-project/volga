from typing import Dict, Any, Union, Type
import importlib
from pydantic import BaseModel, validator
from volga.on_demand.storage.data_connector import OnDemandDataConnector
from volga.on_demand.storage.scylla import OnDemandScyllaConnector


class OnDemandDataConnectorConfig(BaseModel):
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

class OnDemandConfig(BaseModel):
    num_servers_per_node: int
    server_port: int # THIS SHOULD MATCH port in on-demand-service Kube service, 1122
    data_connector: OnDemandDataConnectorConfig

    class Config:
        arbitrary_types_allowed = True


DEFAULT_ON_DEMAND_CONFIG = OnDemandConfig(
    num_servers_per_node=1,
    server_port=1122,
    data_connector=OnDemandDataConnectorConfig(
        connector_class=OnDemandScyllaConnector,
        connector_args={
            'contact_endpoints': ['127.0.0.1']
        }
    )
)
