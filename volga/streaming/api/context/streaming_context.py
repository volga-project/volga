import logging
from typing import Dict, List, Optional

from ray.actor import ActorHandle

from volga.streaming.api.function.function import CollectionSourceFunction, LocalFileSourceFunction, \
    SourceFunction, DelayedCollectionSourceFunction
from volga.streaming.api.function.kafka import KafkaSourceFunction
from volga.streaming.api.function.mysql import MysqlSourceFunction
from volga.streaming.api.job_graph.job_graph_builder import JobGraphBuilder
from volga.streaming.api.job_graph.job_graph_optimizer import JobGraphOptimizer
from volga.streaming.api.stream.stream_sink import StreamSink
from volga.streaming.api.stream.stream_source import StreamSource
from volga.streaming.runtime.client.job_client import JobClient

# logger = logging.getLogger(__name__)
logger = logging.getLogger("ray")


class StreamingContext:

    def __init__(self, job_config: Optional[Dict] = None):
        self.job_config = job_config
        self._id_generator = 0
        self.stream_sinks: List[StreamSink] = []
        self.job_master: Optional[ActorHandle] = None

    def generate_id(self):
        self._id_generator += 1
        return self._id_generator

    def add_sink(self, stream_sink: StreamSink):
        self.stream_sinks.append(stream_sink)

    def source(self, source_func: SourceFunction) -> StreamSource:
        return StreamSource(self, source_func)

    def from_values(self, *values) -> StreamSource:
        return self.from_collection(values)

    def from_collection(self, values) -> StreamSource:
        assert values, "values shouldn't be None or empty"
        func = CollectionSourceFunction(values)
        return self.source(func)

    def from_delayed_collection(self, values, delay_s) -> StreamSource:
        assert values, "values shouldn't be None or empty"
        func = DelayedCollectionSourceFunction(values, delay_s)
        return self.source(func)

    def read_text_file(self, filename: str) -> StreamSource:
        # line by line
        func = LocalFileSourceFunction(filename)
        return self.source(func)

    def kafka(
        self,
        bootstrap_servers: str,
        topic: str,
        security_protocol: Optional[str] = None,
        sasl_mechanism: Optional[str] = None,
        sasl_plain_username: Optional[str] = None,
        sasl_plain_password: Optional[str] = None,
        verify_cert: Optional[bool] = False,
    ):
        func = KafkaSourceFunction(
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            security_protocol=security_protocol,
            sasl_mechanism=sasl_mechanism,
            sasl_plain_username=sasl_plain_username,
            sasl_plain_password=sasl_plain_password,
            verify_cert=verify_cert
        )
        return self.source(func)

    def mysql(
        self,
        database: str,
        table: str,
        host: str = '127.0.0.1',
        port: str = '3306',
        user: str = 'root',
        password: str = '',
    ):
        func = MysqlSourceFunction(
            database=database,
            table=table,
            host=host,
            port=port,
            user=user,
            password=password
        )
        return self.source(func)

    def submit(self):
        job_graph = JobGraphBuilder(stream_sinks=self.stream_sinks).build()
        logger.info(f'Built job graph for {job_graph.job_name}')
        logger.info(f'\n {job_graph.gen_digraph()}')
        job_client = JobClient()
        self.job_master = job_client.submit(job_graph=job_graph, job_config=self.job_config)

    # blocks until job is finished
    def execute(self):
        jg = JobGraphBuilder(stream_sinks=self.stream_sinks).build()
        logger.info(f'Built job graph for {jg.job_name}')
        logger.info(f'\n {jg.gen_digraph()}')
        optimizer = JobGraphOptimizer(jg)
        optimized_jg = optimizer.optimize()
        logger.info(f'Optimized job graph {jg.job_name}')
        logger.info(f'\n {optimized_jg.gen_digraph()}')
        job_client = JobClient()
        job_client.execute(job_graph=optimized_jg, job_config=self.job_config)