import os
import signal
from typing import Optional, Any, Tuple

import psutil
import zmq
import enum

from pydantic import BaseModel

from volga.streaming.runtime.network.config import ZMQConfig


class SocketOwner(enum.Enum):
    TRANSFER_LOCAL = 'transfer_local' # belongs to TransferActor (RemoteTransferHandler), works with client socket
    TRANSFER_REMOTE = 'transfer_remote' # belongs to TransferActor (RemoteTransferHandler), works with another TransferActor socket
    CLIENT = 'client' # belongs to DataReader/DataWriter


class SocketKind(enum.Enum):
    BIND = 'bind' # socket is used as a binded access point
    CONNECT = 'connect'# socket is used as a connecting client


class SocketMetadata(BaseModel, frozen=True):
    owner: SocketOwner
    kind: SocketKind
    channel_id: str
    addr: str


def configure_socket(socket: zmq.Socket, zmq_config: ZMQConfig):
    if zmq_config.LINGER is not None:
        socket.setsockopt(zmq.LINGER, zmq_config.LINGER)
    if zmq_config.SNDHWM is not None:
        socket.setsockopt(zmq.SNDHWM, zmq_config.SNDHWM)
    if zmq_config.RCVHWM is not None:
        socket.setsockopt(zmq.RCVHWM, zmq_config.RCVHWM)
    if zmq_config.SNDBUF is not None:
        socket.setsockopt(zmq.SNDBUF, zmq_config.SNDBUF)
    if zmq_config.RCVBUF is not None:
        socket.setsockopt(zmq.RCVBUF, zmq_config.RCVBUF)
    socket.setsockopt(zmq.CONNECT_TIMEOUT, 4)


# TODO we should have a delay mechanism for retrying in case of exceptions to avoid exhausting CPU
def rcv_no_block(socket: zmq.Socket) -> Optional[Any]:
    try:
        data = socket.recv(zmq.NOBLOCK)
        return data
    except:
        return None


# TODO we should have a delay mechanism for retrying in case of exceptions to avoid exhausting CPU
def send_no_block(socket: zmq.Socket, data) -> bool:
    try:
        socket.send(data, zmq.NOBLOCK)
        return True
    except:
        return False


def get_port_owner_pid(port: int) -> Optional[Tuple[int, str]]:
    connections = psutil.net_connections()
    for con in connections:
        if con.raddr != tuple():
            if con.raddr.port == port:
                return con.pid, con.status
        if con.laddr != tuple():
            if con.laddr.port == port:
                return con.pid, con.status
    return None


# TODO this does not work due to permission issues, figure out a way to deal with Address already in use
# should only be used to bind tcp transport sockets
def _try_tcp_bind(socket: zmq.Socket, tcp_addr: str, port: int):
    try:
        socket.bind(tcp_addr)
    except zmq.error.ZMQError as e:
        if 'Address already in use' in e.strerror:
            print(f'Unable to bind to port {port}, finding port owner and killing...')
            # get process holding onto this port and kill
            p = get_port_owner_pid(port)
            if p is None:
                raise RuntimeError(f'Unable to bind and find a port owner for port {port}')
            pid = p[0]
            os.kill(pid, signal.SIGKILL)
            print(f'Killed {pid} owning {port}, re-binding...')
            # try again
            socket.bind(tcp_addr)
            print(f'Re-binded port {port} ok')
        else:
            raise e
