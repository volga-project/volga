from typing import Dict, Optional

from ray.actor import ActorHandle


# Encapsulate the runtime information of a streaming task
class RuntimeContext:
    def __init__(
        self,
        task_id: int,
        task_index: int,
        parallelism: int,
        operator_id: int,
        operator_name: str,
        job_config: Optional[Dict] = None
    ):
        self.task_id = task_id
        self.task_index = task_index
        self.parallelism = parallelism
        self.operator_id = operator_id
        self.operator_name = operator_name
        if job_config is None:
            job_config = {}
        self.job_config = job_config
