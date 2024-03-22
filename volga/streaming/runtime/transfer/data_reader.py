import logging
from typing import List, Optional

from volga.streaming.runtime.transfer.channel import Channel, ChannelMessage
from volga.streaming.runtime.transfer.data_writer import TransportType

import zmq
import simplejson


logger = logging.getLogger("ray")


class DataReader:
    def __init__(
        self,
        name: str,
        input_channels: List[Channel],
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
        self.sockets_and_contexts = {}
        for channel in self.input_channels:
            context = zmq.Context()
            # TODO set HWM
            socket = context.socket(zmq.PULL)
            socket.setsockopt(zmq.LINGER, 0)
            socket.connect(f'tcp://{channel.source_ip}:{channel.source_port}')
            self.sockets_and_contexts[channel.channel_id] = (socket, context)

        self._processed_ts = []
        self._dupes = 0

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
        if msg['event_time'] not in self._processed_ts:
            self._processed_ts.append(msg['event_time'])
        else:
            self._dupes += 1
            print(f'{self.name}: {self._dupes} dupes')
        return msg

    def close(self):
        self.running = False
        # cleanup sockets and contexts for all channels
        for channel_id in self.sockets_and_contexts:
            socket = self.sockets_and_contexts[channel_id][0]
            context = self.sockets_and_contexts[channel_id][1]
            socket.close(linger=0)
            context.destroy(linger=0)
