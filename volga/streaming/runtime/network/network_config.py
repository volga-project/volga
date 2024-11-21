from typing import Optional

from pydantic import BaseModel
from volga_rust import RustDataReaderConfig, RustDataWriterConfig, RustTransferConfig, RustZmqConfig


class DataReaderConfig(BaseModel):
    output_queue_capacity_bytes: int
    response_batch_period_ms: Optional[int]

    def to_rust(self) -> RustDataReaderConfig:
        return RustDataReaderConfig(self.output_queue_capacity_bytes, self.response_batch_period_ms)


class DataWriterConfig(BaseModel):
    in_flight_timeout_s: int
    max_capacity_bytes_per_channel: int
    batch_size: int
    flush_period_s: float

    def to_rust(self) -> RustDataWriterConfig:
        return RustDataWriterConfig(self.in_flight_timeout_s, self.max_capacity_bytes_per_channel)


class TransferConfig(BaseModel):
    transfer_queue_size: int

    def to_rust(self) -> RustTransferConfig:
        return RustTransferConfig(self.transfer_queue_size)


class ZmqConfig(BaseModel):
    sndhwm: Optional[int]
    rcvhwm: Optional[int]
    sndbuf: Optional[int]
    rcvbuf: Optional[int]
    linger: Optional[int]
    connect_timeout_s: Optional[int]
    num_io_threads: Optional[int]

    def to_rust(self) -> RustZmqConfig:
        return RustZmqConfig(self.sndhwm, self.rcvhwm, self.sndbuf, self.rcvbuf, self.linger, self.connect_timeout_s, self.num_io_threads)


DEFAULT_DATA_READER_CONFIG = DataReaderConfig(output_queue_capacity_bytes=1000*1000*10, response_batch_period_ms=100)
DEFAULT_DATA_WRITER_CONFIG = DataWriterConfig(in_flight_timeout_s=1, max_capacity_bytes_per_channel=1000*1000*20, batch_size=1000, flush_period_s=1)
DEFAULT_TRANSFER_CONFIG = TransferConfig(transfer_queue_size=1000)
DEFAULT_ZMQ_CONFIG = ZmqConfig(
    sndhwm=None,
    rcvhwm=None,
    sndbuf=None,
    rcvbuf=None,
    linger=0,
    connect_timeout_s=5,
    num_io_threads=4
)