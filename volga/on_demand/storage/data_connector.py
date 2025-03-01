from abc import ABC, abstractmethod
from typing import Dict, Any, Callable, Optional, List
from inspect import signature, Parameter
import inspect

class OnDemandDataConnector(ABC):
    """Base class for all custom data connectors used by OnDemandExecutor."""
    
    @abstractmethod
    def query_dict(self) -> Dict[str, Callable]:
        """
        Returns a dictionary mapping query names to connector methods.
        
        Example:
        {
            'latest': self.fetch_latest,
            'range': self.fetch_range
        }
        """
        pass
    
    def get_query(self, query_name: str) -> Callable:
        """Get query function by name"""
        queries = self.query_dict()
        if query_name not in queries:
            raise ValueError(f"Query '{query_name}' not found in connector {self.__class__.__name__}")
        return queries[query_name]

    def get_query_param_names(self, query_name: str) -> set[str]:
        """Get parameter names for a query function"""
        query_func = self.get_query(query_name)
        sig = signature(query_func)
        # Exclude 'self', 'feature_name', and 'keys' parameters
        return {
            name for name, param in sig.parameters.items()
            if name not in ('self', 'feature_name', 'keys')
        }

    def query_params(self) -> Dict[str, List[str]]:
        """Return dictionary of query name to required parameter names"""
        query_dict = self.query_dict()
        params = {}
        
        for query_name, query_func in query_dict.items():
            # Get function signature
            sig = inspect.signature(query_func)
            
            # Get required parameters (excluding self, feature_name, and keys)
            required_params = [
                name for name, param in sig.parameters.items()
                if (name not in ['self', 'feature_name', 'keys'] and 
                    param.default == param.empty)
            ]
            
            params[query_name] = required_params
            
        return params

    @abstractmethod
    async def init(self):
        """Initialize the connector"""
        pass

    @abstractmethod
    async def close(self):
        """Close the connector"""
        pass