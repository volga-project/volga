from volga.streaming.api.context.runtime_context import RuntimeContext
from volga.streaming.runtime.core.execution_graph.execution_graph import ExecutionVertex


class StreamingRuntimeContext(RuntimeContext):

    def __init__(
        self,
        execution_vertex: ExecutionVertex
    ):
        super().__init__(
            task_id=execution_vertex.execution_vertex_id,
            task_index=execution_vertex.execution_vertex_index,
            parallelism=execution_vertex.parallelism,
            operator_id=execution_vertex.job_vertex.vertex_id,
            operator_name=execution_vertex.job_vertex.get_name(),
            job_config=execution_vertex.job_config
        )