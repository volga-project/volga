import logging
import time
from abc import ABC, abstractmethod
from threading import Thread
from typing import Optional, List, Any

from ray.actor import ActorHandle

from volga.streaming.api.job_graph.job_graph import VertexType
from volga.streaming.api.message.message import Record, record_from_channel_message
from volga.streaming.api.operators.chained import ChainedOperator
from volga.streaming.api.operators.operators import ISourceOperator, SourceOperator, SinkOperator
from volga.streaming.runtime.config.streaming_config import StreamingWorkerConfig
from volga.streaming.runtime.core.execution_graph.execution_graph import ExecutionVertex
from volga.streaming.runtime.master.stats.stats_manager import WorkerStatsUpdate
from volga.streaming.runtime.network.io_loop import IOLoop
from volga.streaming.runtime.network.local.data_reader import DataReader
from volga.streaming.runtime.network.local.data_writer import DataWriter
from volga.streaming.runtime.worker.task.streaming_runtime_context import StreamingRuntimeContext
from volga.streaming.runtime.core.collector.output_collector import OutputCollector
from volga.streaming.runtime.core.processor.processor import Processor, TwoInputProcessor, SourceProcessor, \
    StreamProcessor

logger = logging.getLogger("ray")


class StreamTask(ABC):

    def __init__(
        self,
        job_master: ActorHandle,
        processor: Processor,
        execution_vertex: ExecutionVertex,
        worker_config: StreamingWorkerConfig
    ):
        self.job_master = job_master
        self.processor = processor
        self.execution_vertex = execution_vertex
        self.thread = Thread(target=self.run, daemon=True)
        self.data_writer: Optional[DataWriter] = None
        self.data_reader: Optional[DataReader] = None
        self.running = True
        self.collectors = []
        self.worker_config = worker_config
        self.io_loop = IOLoop(f'io-loop-{execution_vertex.execution_vertex_id}', worker_config.network_config.zmq)

    @abstractmethod
    def run(self):
        pass

    def start_or_recover(self) -> Optional[str]:
        err = self._prepare_task()
        if err is not None:
            return err
        self.thread.start()
        return None

    def _prepare_task(self) -> Optional[str]:
        # writer
        if len(self.execution_vertex.output_edges) != 0:
            output_channels = self.execution_vertex.get_output_channels()
            assert len(output_channels) > 0
            assert output_channels[0] is not None
            if self.data_writer is not None:
                raise RuntimeError('Writer already inited')
            if self.execution_vertex.job_vertex.vertex_type != VertexType.SINK:
                # sinks do not pass data downstream so no writer
                self.data_writer = DataWriter(
                    handler_id=f'dw-{self.execution_vertex.execution_vertex_id}',
                    name=f'dw-{self.execution_vertex.execution_vertex_id}',
                    source_stream_name=str(self.execution_vertex.stream_operator.id),
                    job_name=self.execution_vertex.job_name,
                    channels=output_channels,
                    config=self.worker_config.network_config.data_writer
                )
                self.io_loop.register_io_handler(self.data_writer)

        # reader
        if len(self.execution_vertex.input_edges) != 0:
            input_channels = self.execution_vertex.get_input_channels()
            assert len(input_channels) > 0
            assert input_channels[0] is not None
            if self.data_reader is not None:
                raise RuntimeError('Reader already inited')
            if self.execution_vertex.job_vertex.vertex_type != VertexType.SOURCE:
                # sources do not read data from upstream so no reader
                self.data_reader = DataReader(
                    handler_id=f'dr-{self.execution_vertex.execution_vertex_id}',
                    name=f'dr-{self.execution_vertex.execution_vertex_id}',
                    job_name=self.execution_vertex.job_name,
                    channels=input_channels,
                    config=self.worker_config.network_config.data_reader
                )
                self.io_loop.register_io_handler(self.data_reader)

        return self._open_processor()

    def _open_processor(self) -> Optional[str]:
        execution_vertex = self.execution_vertex
        output_edges = execution_vertex.output_edges
        # grouped by each operator in target vertex
        grouped_channel_ids = {}
        grouped_partitions = {}

        for i in range(len(output_edges)):
            output_edge = output_edges[i]
            op_name = output_edge.target_execution_vertex.job_vertex.get_name()
            if op_name not in grouped_channel_ids:
                grouped_channel_ids[op_name] = []
            output_channel_ids = [ch.channel_id for ch in execution_vertex.get_output_channels()]
            grouped_channel_ids[op_name].append(output_channel_ids[i])
            grouped_partitions[op_name] = output_edge.partition

        for op_name in grouped_partitions:
            self.collectors.append(OutputCollector(
                data_writer=self.data_writer,
                output_channel_ids=grouped_channel_ids[op_name],
                partition=grouped_partitions[op_name]
            ))

        runtime_context = StreamingRuntimeContext(execution_vertex=execution_vertex)

        err = self.io_loop.connect_and_start() # TODO pass num io threads
        if err is not None:
            return err
        self.processor.open(
            collectors=self.collectors,
            runtime_context=runtime_context
        )
        if isinstance(self.processor, SourceProcessor):
            self.processor.set_master_handle(self.job_master)
        return None

    def close(self):
        # logger.info(f'Closing task {self.execution_vertex.execution_vertex_id}...')
        self.running = False
        self.processor.close()
        self.io_loop.stop()
        if self.thread is not None:
            self.thread.join(timeout=5)
        logger.info(f'Closed task {self.execution_vertex.execution_vertex_id}')

    def collect_stats(self) -> List[WorkerStatsUpdate]:
        if isinstance(self.processor, SourceProcessor):
            assert isinstance(self.processor.operator, ISourceOperator)
            source_context = self.processor.operator.get_source_context()
            assert isinstance(source_context, SourceOperator.SourceContextImpl)
            worker_throughput_stats = source_context.throughput_stats
            return [worker_throughput_stats.collect()]

        assert isinstance(self.processor, StreamProcessor)
        operator = self.processor.operator
        if isinstance(operator, ChainedOperator):
            operator = operator.tail_operator

        if isinstance(operator, SinkOperator):
            latency_stats = operator.latency_stats
            return [latency_stats.collect()]

        raise RuntimeError('Trying to collect stats from worker that does not implement it')


class SourceStreamTask(StreamTask):

    def run(self):
        while self.running:
            record = Record(value=None) # empty message, this will trigger sourceFunction.fetch()
            self.processor.process(record)

    def get_num_sent(self) -> Any:
        assert isinstance(self.processor, SourceProcessor)
        op = self.processor.operator
        assert isinstance(op, ISourceOperator)
        return op.get_num_sent()


class InputStreamTask(StreamTask):

    READ_RETRY_PERIOD_S = 0.001 # TODO config this

    def run(self):
        while self.running:
            messages = self.data_reader.read_message_batch()
            if messages is None or len(messages) == 0:
                time.sleep(self.READ_RETRY_PERIOD_S)
                continue
            for message in messages:
                record = record_from_channel_message(message)
                self.processor.process(record)


class OneInputStreamTask(InputStreamTask):
    pass


class TwoInputStreamTask(InputStreamTask):

    def __init__(
        self,
        job_master: ActorHandle,
        processor: Processor,
        execution_vertex: ExecutionVertex,
        left_stream_name: str,
        right_stream_name: str,
        worker_config: StreamingWorkerConfig
    ):
        super().__init__(
            job_master=job_master,
            processor=processor,
            execution_vertex=execution_vertex,
            worker_config=worker_config
        )

        assert isinstance(self.processor, TwoInputProcessor)
        self.processor.left_stream_name = left_stream_name
        self.processor.right_stream_name = right_stream_name

