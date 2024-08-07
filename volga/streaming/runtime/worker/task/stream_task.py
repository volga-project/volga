import logging
import time
from abc import ABC, abstractmethod
from threading import Thread
from typing import Optional

import zmq

from volga.streaming.api.job_graph.job_graph import VertexType
from volga.streaming.api.message.message import Record, record_from_channel_message
from volga.streaming.runtime.core.execution_graph.execution_graph import ExecutionVertex
from volga.streaming.runtime.network_deprecated.buffer.buffering_policy import PeriodicPartialFlushPolicy
from volga.streaming.runtime.network.io_loop import IOLoop
from volga.streaming.runtime.network.local.data_reader import DataReader
from volga.streaming.runtime.network.local.data_writer import DataWriter
from volga.streaming.runtime.worker.task.streaming_runtime_context import StreamingRuntimeContext
from volga.streaming.runtime.core.collector.output_collector import OutputCollector
from volga.streaming.runtime.core.processor.processor import Processor, TwoInputProcessor


logger = logging.getLogger("ray")


class StreamTask(ABC):

    def __init__(
        self,
        processor: Processor,
        execution_vertex: ExecutionVertex
    ):
        self.processor = processor
        self.execution_vertex = execution_vertex
        self.thread = Thread(target=self.run, daemon=True)
        self.data_writer: Optional[DataWriter] = None
        self.data_reader: Optional[DataReader] = None
        self.running = True
        self.collectors = []
        # TODO pass network config
        self.io_loop = IOLoop(f'io-loop-{execution_vertex.execution_vertex_id}')

    @abstractmethod
    def run(self):
        pass

    def start_or_recover(self):
        self._prepare_task()
        self.thread.start()

    def _prepare_task(self):
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
                    name=f'data-writer-{self.execution_vertex.execution_vertex_id}',
                    source_stream_name=str(self.execution_vertex.stream_operator.id),
                    job_name=self.execution_vertex.job_name,
                    channels=output_channels
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
                    name=f'data-reader-{self.execution_vertex.execution_vertex_id}',
                    job_name=self.execution_vertex.job_name,
                    channels=input_channels
                )
                self.io_loop.register_io_handler(self.data_reader)

        self._open_processor()

    def _open_processor(self):
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

        self.io_loop.start()

        self.processor.open(
            collectors=self.collectors,
            runtime_context=runtime_context
        )

    def close(self):
        # logger.info(f'Closing task {self.execution_vertex.execution_vertex_id}...')
        self.running = False
        self.processor.close()
        self.io_loop.close()
        if self.thread is not None:
            self.thread.join(timeout=5)
        logger.info(f'Closed task {self.execution_vertex.execution_vertex_id}')


class SourceStreamTask(StreamTask):

    def run(self):
        while self.running:
            record = Record(value=None) # empty message, this will trigger sourceFunction.fetch()
            self.processor.process(record)


class InputStreamTask(StreamTask):

    READ_RETRY_PERIOD_S = 0.001 # TODO config this

    def run(self):
        while self.running:
            messages = self.data_reader.read_message()
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
        processor: Processor,
        execution_vertex: ExecutionVertex,
        left_stream_name: str,
        right_stream_name: str,
    ):
        super().__init__(
            processor=processor,
            execution_vertex=execution_vertex
        )

        assert isinstance(self.processor, TwoInputProcessor)
        self.processor.left_stream_name = left_stream_name
        self.processor.right_stream_name = right_stream_name

