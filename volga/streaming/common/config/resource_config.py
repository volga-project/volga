from typing import Dict, Optional

from pydantic import BaseModel


class ResourceConfig(BaseModel):
    default_worker_resources: Dict[str, str]

    # per operator-type resources
    # {
    #     'Source': {
    #         'CPU': '1',
    #         'MEM': '1000',
    #         'GPU': '0.5'
    #     },
    #     'Filter': {...},
    #     'Map': {...}
    # }
    proposed_operator_resources: Optional[Dict[str, Dict[str, str]]]

    # per operator-instance resources
    # {
    #     '10-SourceOperator': {
    #         'CPU': '1',
    #         'MEM': '1000',
    #         'GPU': '0.5'
    #     },
    #     '21-FilterOperator': {...},
    # custom_operator_resources: Optional[Dict[str, Dict[str, str]]]