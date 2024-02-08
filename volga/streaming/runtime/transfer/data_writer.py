import enum
import ujson
from typing import List

from volga.streaming.api.message.message import Record
from volga.streaming.runtime.transfer.channel import Channel, ChannelMessage

import zmq


class TransportType(enum.Enum):
    ZMQ_PUSH_PULL = 1
    ZMQ_PUB_SUB = 2
    RAY_SHARED_MEM = 3


class DataWriter:

    def __init__(
        self,
        name: str,
        source_stream_name: str,
        output_channels: List[Channel],
        transport_type: TransportType = TransportType.ZMQ_PUSH_PULL
    ):
        if transport_type not in [
            TransportType.ZMQ_PUSH_PULL,
        ]:
            raise RuntimeError(f'Unsupported transport: {transport_type}')
        self.name = name

        self.source_stream_name = source_stream_name
        self.out_channels = output_channels
        # TODO buffering

        self.sockets_and_contexts = {}
        for channel in self.out_channels:
            if channel.channel_id in self.sockets_and_contexts:
                raise RuntimeError('duplicate channel ids')
            context = zmq.Context()

            # TODO set HWM
            socket = context.socket(zmq.PUSH)
            socket.setsockopt(zmq.LINGER, 0)
            socket.bind(f'tcp://127.0.0.1:{channel.source_port}')
            self.sockets_and_contexts[channel.channel_id] = (socket, context)

    def write_record(self, channel_id: str, record: Record):
        # add sender operator_id
        record.set_stream_name(self.source_stream_name)
        message = record.to_channel_message()
        self.write_message(channel_id, message)

    def write_message(self, channel_id: str, message: ChannelMessage):
        # TODO this should use a buffer?
        # TODO serialization perf
        json_str = ujson.dumps(message)

        socket = self.sockets_and_contexts[channel_id][0]
        # TODO depending on socket type, this can block or just throw exception, test this
        socket.send_string(json_str)

    def close(self):
        # cleanup sockets and contexts for all channels
        for channel_id in self.sockets_and_contexts:
            socket = self.sockets_and_contexts[channel_id][0]
            context = self.sockets_and_contexts[channel_id][1]
            socket.close(linger=0)
            context.destroy(linger=0)