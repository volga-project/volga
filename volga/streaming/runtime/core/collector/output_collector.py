from typing import List

from volga.streaming.api.collector.collector import Collector
from volga.streaming.api.message.message import Record
from volga.streaming.api.partition.partition import Partition
from volga.streaming.runtime.network.local.data_writer import DataWriter


class OutputCollector(Collector):

    def __init__(
        self,
        data_writer: DataWriter,
        output_channel_ids: List[str],
        partition: Partition
    ):
        self.data_writer = data_writer
        self.output_channel_ids = output_channel_ids
        self.partition = partition

    def collect(self, record: Record):
        partitions = self.partition.partition(record=record, num_partition=len(self.output_channel_ids))
        partitions_to_send = partitions.copy()

        cur_partition_index = 0

        # TODO backpressure and timeouts should go here
        while len(partitions_to_send) != 0:
            partition = partitions_to_send[cur_partition_index]
            succ = self.data_writer.try_write_record(self.output_channel_ids[partition], record)
            if succ:
                del partitions_to_send[cur_partition_index]
                if len(partitions_to_send) == 0:
                    break

            cur_partition_index = (cur_partition_index + 1)%len(partitions_to_send)
