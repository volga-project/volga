import logging

from ray.actor import ActorHandle

from volga.streaming.runtime.master.context.job_master_runtime_context import JobMasterRuntimeContext
from volga.streaming.runtime.master.job_lifecycle.job_status import JobStatus
from volga.streaming.runtime.master.resource_manager.node_assign_strategy import NodeAssignStrategy
from volga.streaming.runtime.master.resource_manager.resource_manager import ResourceManager
from volga.streaming.runtime.master.stats.stats_manager import StatsManager
from volga.streaming.runtime.master.worker_lifecycle_controller import WorkerLifecycleController

# logger = logging.getLogger(__name__)
logger = logging.getLogger("ray")


class JobScheduler:

    def __init__(
        self,
        job_master: ActorHandle,
        resource_manager: ResourceManager,
        stats_manager: StatsManager,
        node_assign_strategy: NodeAssignStrategy,
        runtime_context: JobMasterRuntimeContext
    ):
        self.runtime_context = runtime_context
        self.worker_lifecycle_controller = WorkerLifecycleController(
            job_master, resource_manager, stats_manager, node_assign_strategy,
            runtime_context.streaming_config.worker_config
        )

    def schedule_job(self) -> bool:
        self._prepare_job_submission()
        self._do_job_submission()
        return True

    def _prepare_job_submission(self) -> bool:
        logger.info(f'Preparing job {self.runtime_context.job_graph.job_name}...')
        # create workers
        self.worker_lifecycle_controller.create_workers(self.runtime_context.execution_graph)

        # connect and init workers, update exec graph channels
        self.worker_lifecycle_controller.connect_and_init_workers(self.runtime_context.execution_graph)

        logger.info(f'Prepared job {self.runtime_context.job_graph.job_name}.')
        # TODO init master?

        return True

    def _do_job_submission(self) -> bool:
        logger.info(f'Submitting job {self.runtime_context.job_graph.job_name}...')
        # start workers
        self.worker_lifecycle_controller.start_workers(self.runtime_context.execution_graph)
        self.runtime_context.job_status = JobStatus.RUNNING
        logger.info(f'Submitted job {self.runtime_context.job_graph.job_name}.')
        return True

    def destroy_job(self) -> bool:
        self.worker_lifecycle_controller.delete_workers(
            list(self.runtime_context.execution_graph.execution_vertices_by_id.values())
        )
        return True
