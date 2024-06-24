import enum
from abc import ABC, abstractmethod
from threading import Thread
from typing import List, Dict, Optional, Tuple

import zmq
from zmq.utils.monitor import recv_monitor_message, parse_monitor_message

from volga.streaming.runtime.network.channel import Channel
from volga.streaming.runtime.network.config import NetworkConfig


# Indicates the role of the handler.
# SENDER means forward flow - sends payload, receives acks
# RECEIVER means backward flow - receives payload, sends acks
class Direction(enum.Enum):
    SENDER = 1
    RECEIVER = 2

# zmq on Kubernetes
# https://stackoverflow.com/questions/63430835/2-minutes-for-zmq-pub-sub-to-connect-in-kubernetes
# https://stackoverflow.com/questions/65679784/zmq-sockets-do-not-work-as-expected-on-kubernetes

# Interface describing what to do on each socket event (send or rcv)
class IOHandler(ABC):

    def __init__(
        self,
        channels: List[Channel],
        zmq_ctx: zmq.Context,
        direction: Direction,
        network_config: NetworkConfig
    ):
        self._direction = direction
        self._network_config = network_config
        self._zmq_ctx = zmq_ctx
        self._channels = channels
        self.io_loop: Optional[IOLoop] = None

    def is_running(self):
        if self.io_loop is None:
            return False
        return self.io_loop.running

    @abstractmethod
    def init_sockets(self) -> List[Tuple[str, zmq.Socket]]: # List[(socket_name, socket)]
        raise NotImplementedError()

    @abstractmethod
    def rcv(self, socket: zmq.Socket):
        raise NotImplementedError()

    @abstractmethod
    def send(self, socket: zmq.Socket):
        raise NotImplementedError()

    @abstractmethod
    def close_sockets(self):
        raise NotImplementedError()


# Polls zmq sockets and delegates send or rcv events to registered io handlers
class IOLoop:
    def __init__(self):
        self._poller: zmq.Poller = zmq.Poller()

        self.running = False
        self._thread = Thread(target=self._start_loop)
        self._registered_handlers: List[IOHandler] = []
        self._socket_to_handler: Dict[zmq.Socket, IOHandler] = {}

        # monitoring
        self._monitor_sockets: Dict[zmq.Socket, str] = {}

    def register(self, io_handler: IOHandler):
        if self.running is True:
            raise RuntimeError('Can not register new handlers when loop is running')
        self._registered_handlers.append(io_handler)
        io_handler.io_loop = self

    def _start_loop(self):
        for handler in self._registered_handlers:
            named_sockets = handler.init_sockets()
            for (socket_name, socket) in named_sockets:
                if socket in self._socket_to_handler:
                    raise RuntimeError('Duplicate socket')
                self._socket_to_handler[socket] = handler
                self._poller.register(socket, zmq.POLLIN | zmq.POLLOUT)

                # register for monitoring
                monitor_socket = socket.get_monitor_socket()
                # TODO wwe should identify it by channel_id->socket_type->side(BIND/CONNECT)
                self._monitor_sockets[monitor_socket] = socket_name
                self._poller.register(monitor_socket, zmq.POLLIN)

        self.running = True
        self._loop()
        for handler in self._registered_handlers:
            handler.close_sockets()

    def _on_monitor_event(self, monitor_socket: zmq.Socket, monitored_socket_name: str):
        evt = recv_monitor_message(monitor_socket)
        # if 'transfer_remote' in monitored_socket_name:
        #     print(f'Monitor [{monitored_socket_name}] {evt}')

    def _loop(self):
        while self.running:
            sockets_and_flags = self._poller.poll(timeout=10)
            if len(sockets_and_flags) == 0:
                # print(f'timeout polling {len(self._registered_handlers)}')
                continue
            for (socket, flag) in sockets_and_flags:
                if socket in self._monitor_sockets:
                    self._on_monitor_event(socket, self._monitor_sockets[socket])
                    continue

                handler = self._socket_to_handler[socket]
                if flag == zmq.POLLIN:
                    handler.rcv(socket)
                elif flag == zmq.POLLOUT:
                    handler.send(socket)
                elif flag == zmq.POLLOUT | zmq.POLLIN:
                    handler.send(socket)
                    handler.rcv(socket)
                else:
                    raise RuntimeError(f'Unknown flag {flag}')

    def start(self):
        self.running = True
        self._thread.start()

    def close(self):
        self.running = False
        self._thread.join(timeout=5)
