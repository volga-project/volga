import logging
from typing import List, Optional

from volga.streaming.runtime.transfer.channel import Channel, ChannelMessage, RemoteChannel
from volga.streaming.runtime.transfer.deprecated.channel import Channel_DEP
from volga.streaming.runtime.transfer.deprecated.data_writer import TransportType

import zmq
import simplejson


logger = logging.getLogger("ray")


class DataReader_DEPR:
    def __init__(
        self,
        name: str,
        input_channels: List[Channel_DEP],
        transport_type: TransportType = TransportType.ZMQ_PUSH_PULL
    ):
        if transport_type not in [
            TransportType.ZMQ_PUSH_PULL,
        ]:
            raise RuntimeError(f'Unsupported transport {transport_type}')

        self.name = name
        self.input_channels = input_channels
        self.cur_read_id = 0
        self.running = True

        # TODO buffering
        # buffer pool impl https://github.com/Naman-Bhalla/dbms-buffer-pool-manager-python/tree/master/src
        self.sockets_and_contexts = {}
        for channel in self.input_channels:
            assert isinstance(channel, RemoteChannel)
            context = zmq.Context()
            # TODO set HWM
            socket = context.socket(zmq.PULL)
            socket.setsockopt(zmq.LINGER, 0)
            socket.connect(f'tcp://{channel.source_ip}:{channel.source_port}')
            self.sockets_and_contexts[channel.channel_id] = (socket, context)

    # TODO set timeout
    def read_message(self) -> Optional[ChannelMessage]:
        # TODO this should use a buffer?

        # round-robin read
        channel_id = None
        json_str = None
        while self.running and json_str is None:
            channel_id = self.input_channels[self.cur_read_id].channel_id
            socket = self.sockets_and_contexts[channel_id][0]
            try:
                json_str = socket.recv_string(zmq.NOBLOCK)
            except zmq.error.ContextTerminated:
                logger.info('zmq recv interrupt due to ContextTerminated')
                json_str = None
            except Exception as e:
                json_str = None
            self.cur_read_id = (self.cur_read_id + 1) % len(self.input_channels)

        # reader was stopped
        if json_str is None:
            assert self.running is False
            return None

        # TODO serialization perf
        msg = simplejson.loads(json_str)
        return msg

    def close(self):
        self.running = False
        # cleanup sockets and contexts for all channels
        for channel_id in self.sockets_and_contexts:
            socket = self.sockets_and_contexts[channel_id][0]
            context = self.sockets_and_contexts[channel_id][1]
            socket.close(linger=0)
            context.destroy(linger=0)
