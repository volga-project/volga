import enum
import threading
import time
from abc import ABC, abstractmethod
from threading import Thread
from typing import List, Dict, Optional, Tuple, Any

import zmq
from zmq.utils.monitor import recv_monitor_message
from zmq.constants import Event

from volga.streaming.runtime.network.channel import Channel
from volga.streaming.runtime.network.config import NetworkConfig
from volga.streaming.runtime.network.metrics import MetricsRecorder
from volga.streaming.runtime.network.socket_utils import SocketMetadata, SocketOwner, SocketKind


# zmq on Kubernetes
# https://stackoverflow.com/questions/63430835/2-minutes-for-zmq-pub-sub-to-connect-in-kubernetes
# https://stackoverflow.com/questions/65679784/zmq-sockets-do-not-work-as-expected-on-kubernetes

# Indicates the role of the handler.
# SENDER means forward flow - sends payload, receives acks
# RECEIVER means backward flow - receives payload, sends acks
class Direction(enum.Enum):
    SENDER = 1
    RECEIVER = 2


class IOHandlerType(enum.Enum):
    DATA_READER = 'data_reader'
    DATA_WRITER = 'data_writer'
    TRANSFER_SENDER = 'transfer_sender'
    TRANSFER_RECEIVER = 'transfer_receiver'


# Interface describing what to do on each socket event (send or rcv)
class IOHandler(ABC):

    def __init__(
        self,
        job_name: str,
        name: str,
        channels: List[Channel],
        zmq_ctx: zmq.Context,
        direction: Direction,
        network_config: NetworkConfig
    ):
        self.job_name = job_name
        self.name = name
        self._direction = direction
        self._network_config = network_config
        self._zmq_ctx = zmq_ctx
        self._channels = channels
        self.io_loop: Optional[IOLoop] = None
        self._sockets_meta: Dict[SocketMetadata, zmq.Socket] = {}

        self.metrics_recorder = MetricsRecorder.instance(job_name)

    def is_running(self):
        if self.io_loop is None:
            return False
        return self.io_loop.running

    @abstractmethod
    def create_sockets(self) -> List[Tuple[SocketMetadata, zmq.Socket]]:
        raise NotImplementedError()

    def bind_and_connect(self):
        if len(self._sockets_meta) == 0:
            raise RuntimeError('No sockets to bind/connect')
        for meta in self._sockets_meta:
            socket = self._sockets_meta[meta]
            assert isinstance(socket, zmq.Socket)
            addr = meta.addr
            if meta.kind == SocketKind.BIND:
                socket.bind(addr)
            elif meta.kind == SocketKind.CONNECT:
                socket.connect(addr)
            else:
                raise RuntimeError('Unknown socket kind')

    @abstractmethod
    def rcv(self, socket: zmq.Socket):
        raise NotImplementedError()

    @abstractmethod
    def send(self, socket: zmq.Socket):
        raise NotImplementedError()

    @abstractmethod
    def close_sockets(self):
        raise NotImplementedError()

    @abstractmethod
    def get_handler_type(self) -> IOHandlerType:
        raise NotImplementedError()


# Polls zmq sockets and delegates send or rcv events to registered io handlers
class IOLoop:
    def __init__(self):
        self._poller = None

        self.running = False
        self._thread = None
        self._registered_handlers: List[IOHandler] = []
        self._socket_to_handler: Dict[zmq.Socket, IOHandler] = {}

        # monitoring
        self._monitor_sockets: Dict[zmq.Socket, SocketMetadata] = {}

        # keep track of connected events for sockets which should have it
        self._to_be_connected: Dict[SocketMetadata, Tuple[bool, Optional[Event]]] = {}
        # once all of the above is connected, release this event so the loop starts
        self._all_connected_event = threading.Event()

    def register(self, io_handler: IOHandler):
        if self.running is True:
            raise RuntimeError('Can not register new handlers when loop is running')
        self._registered_handlers.append(io_handler)
        io_handler.io_loop = self

    def _start_loop(self):
        self._poller = zmq.Poller()

        for handler in self._registered_handlers:
            socket_metas = handler.create_sockets()
            for (socket_meta, socket) in socket_metas:
                assert isinstance(socket_meta, SocketMetadata)
                if socket in self._socket_to_handler:
                    raise RuntimeError('Duplicate socket')
                self._socket_to_handler[socket] = handler
                self._poller.register(socket, zmq.POLLIN | zmq.POLLOUT)

                # register for monitoring
                monitor_socket = socket.get_monitor_socket()
                self._monitor_sockets[monitor_socket] = socket_meta
                self._poller.register(monitor_socket, zmq.POLLIN)

                # register if this socket should be awaited to confirm connection
                if socket_meta.kind == SocketKind.CONNECT:
                    self._to_be_connected[socket_meta] = (False, None)

            handler.bind_and_connect()

        if len(self._to_be_connected) == 0:
            self._all_connected_event.set()

        self.running = True
        self._loop()
        for handler in self._registered_handlers:
            handler.close_sockets()

    def _on_monitor_event(self, monitor_socket: zmq.Socket, socket_meta: SocketMetadata):
        evt = recv_monitor_message(monitor_socket)
        if socket_meta.kind == SocketKind.CONNECT:
            event = evt['event']
            assert isinstance(event, Event)
            if event == Event.CONNECTED:
                self._to_be_connected[socket_meta] = (True, event)
            else:
                self._to_be_connected[socket_meta] = (self._to_be_connected[socket_meta][0], event)

            # check if all connected
            all_con = True
            for s in self._to_be_connected:
                if not self._to_be_connected[s][0]:
                    all_con = False
                    break

            if all_con:
                self._all_connected_event.set()

        # TODO remove after debug
        # if socket_meta.owner == SocketOwner.CLIENT:
        #     print(f'Monitor [{socket_meta}] {evt}')

    def _loop(self):
        while self.running:
            sockets_and_flags = self._poller.poll()
            if len(sockets_and_flags) == 0:
                # print(f'timeout polling {len(self._registered_handlers)}')
                continue
            for (socket, flag) in sockets_and_flags:
                if socket in self._monitor_sockets:
                    self._on_monitor_event(socket, self._monitor_sockets[socket])
                    continue
                assert isinstance(socket, zmq.Socket)
                # do not process data until we have everything connected
                if not self._all_connected_event.is_set():
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

    def start(self, timeout: int = 5) -> Tuple[bool, Any]:

        self.running = True
        self._all_connected_event = threading.Event()
        self._thread = Thread(target=self._start_loop)
        self._thread.start()

        ready = self._all_connected_event.wait(timeout=timeout)
        if not ready:
            # get info about not connected sockets
            not_connected_info = [(str(sm), str(self._to_be_connected[sm][1])) for sm in self._to_be_connected if not self._to_be_connected[sm][0]]
            self.close()
            return False, not_connected_info
        else:
            return True, None

    def close(self):
        self.running = False
        self._thread.join(timeout=5)
