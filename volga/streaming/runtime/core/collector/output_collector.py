from typing import List

from volga.streaming.api.collector.collector import Collector
from volga.streaming.api.message.message import Record
from volga.streaming.api.partition.partition import Partition
from volga.streaming.runtime.transfer.deprecated.data_writer import DataWriter_DEPR


class OutputCollector(Collector):

    def __init__(
        self,
        data_writer: DataWriter_DEPR,
        output_channel_ids: List[str],
        partition: Partition
    ):
        self.data_writer = data_writer
        self.output_channel_ids = output_channel_ids
        self.partition = partition

    def collect(self, record: Record):
        partitions = self.partition. partition(record=record, num_partition=len(self.output_channel_ids))
        for partition in partitions:
            self.data_writer.write_record(self.output_channel_ids[partition], record)
