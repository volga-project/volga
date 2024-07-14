from typing import List

from volga.streaming.api.collector.collector import Collector
from volga.streaming.api.message.message import Record
from volga.streaming.api.partition.partition import Partition
from volga.streaming.runtime.network.transfer.local.data_writer import DataWriter


class OutputCollector(Collector):

    def __init__(
        self,
        collector_id: int,
        downstream_id: int,
        data_writer: DataWriter,
        output_channel_ids: List[str],
        partition: Partition
    ):
        self.collector_id = collector_id
        self.downstream_id = downstream_id
        self.data_writer = data_writer
        self.output_channel_ids = output_channel_ids
        self.partition = partition

    def collect(self, record: Record):
        partitions = self.partition.partition(record=record, num_partition=len(self.output_channel_ids))
        for partition in partitions:
            self.data_writer.write_record(self.output_channel_ids[partition], record)

    def get_id(self) -> int:
        return self.collector_id

    def get_downstream_op_id(self) -> int:
        return self.downstream_id
