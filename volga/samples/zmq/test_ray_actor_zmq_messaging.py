import time
from typing import Dict, Tuple, List, Optional

import simplejson

import ray
import zmq

# HWM and buffers
# https://stackoverflow.com/questions/53356451/pyzmq-high-water-mark-not-working-on-pub-socket
# https://stackoverflow.com/questions/22613737/how-could-i-set-hwm-in-the-push-pull-pattern-of-zmq/26424611#26424611
# https://stackoverflow.com/questions/53691760/what-are-the-differences-between-kernel-buffer-tcp-socket-buffer-and-sliding-wi

# "You might also need to set the underlying TCP buffer sizes,
# its hard to eradicate the buffer completely.
# You could try the experiment using inproc protocol with two threads instead of TCP as
# you will have more control over the buffering "

# EAGAIN
# https://github.com/zeromq/libzmq/issues/1332


class Message:
    def __init__(self, msg_id: int, payload: Dict):
        self.msg_id = msg_id
        self.payload = payload

    def ser(self) -> str:
        return simplejson.dumps({
            'msg_id': self.msg_id,
            'payload': self.payload
        })

    @staticmethod
    def de(json_str: str) -> 'Message':
        d = simplejson.loads(json_str)
        return Message(msg_id=d['msg_id'], payload=d['payload'])

    def to_bytes(self) -> bytes:
        return self.ser().encode()

    @staticmethod
    def from_bytes(bs: bytes) -> 'Message':
        return Message.de(bs.decode())

    def __repr__(self):
        return str(self.payload)

def _msg(msg_id: int) -> Message:
    # payload = {'value': f'msg_{msg_id}', 'dummy_heavy_load': 'a'*1000}
    payload = {'value': f'msg_{msg_id}'}
    return Message(msg_id=msg_id, payload=payload)


@ray.remote
class Push:

    def __init__(self):
        self.port = None
        self.socket = None
        self.total_time = None
        self.backpressure_intervals = []

    def initialize(self, port: int, snd_hwm: Optional[int], rcv_hwm: Optional[int], snd_buf: Optional[int], rcv_buf: Optional[int]):
        self.port = port
        context = zmq.Context()
        self.socket = context.socket(zmq.PUSH)
        if snd_hwm is not None:
            self.socket.setsockopt(zmq.SNDHWM, snd_hwm)
        if rcv_hwm is not None:
            self.socket.setsockopt(zmq.RCVHWM, rcv_hwm)
        if snd_buf is not None:
            self.socket.setsockopt(zmq.SNDBUF, snd_buf)
        if rcv_buf is not None:
            self.socket.setsockopt(zmq.RCVBUF, rcv_buf)

        # self.socket.bind(f'tcp://*:{port}')
        self.socket.bind('ipc:///tmp/zmqtest')
        print('Push inited')

    def start(self, to_send: List, loop_timeout_s: float):
        start = time.time()
        for msg in to_send:
            send_start = time.time()
            is_backpressured = False
            while True:
                try:
                    self.socket.send(msg.to_bytes(), zmq.NOBLOCK)
                    print(f'sent {msg.msg_id}')
                    if is_backpressured:
                        is_backpressured = False
                        self.backpressure_intervals.append((send_start, time.time()))
                        print(f'Backpressure finished after {time.time() - send_start}s')
                    break
                except zmq.error.Again:
                    if not is_backpressured:
                        print('Backpressure started')
                    is_backpressured = True
                    pass
                time.sleep(0.001)
                if loop_timeout_s > 0:
                    time.sleep(loop_timeout_s)
        self.total_time = time.time() - start
        time.sleep(1)
        self.socket.send_string('done')

    def get_backpressure_stats(self) -> Tuple[List, float]:
        return self.backpressure_intervals, self.total_time


@ray.remote
class Pull:
    def __init__(self):
        self.port = None
        self.socket = None

    def initialize(self, peer_addr: str, snd_hwm: Optional[int], rcv_hwm: Optional[int], snd_buf: Optional[int], rcv_buf: Optional[int]):
        context = zmq.Context()
        self.socket = context.socket(zmq.PULL)
        if snd_hwm is not None:
            self.socket.setsockopt(zmq.SNDHWM, snd_hwm)
        if rcv_hwm is not None:
            self.socket.setsockopt(zmq.RCVHWM, rcv_hwm)
        if snd_buf is not None:
            self.socket.setsockopt(zmq.SNDBUF, snd_buf)
        if rcv_buf is not None:
            self.socket.setsockopt(zmq.RCVBUF, rcv_buf)
        self.socket.connect(peer_addr)
        print('Pull inited')

    def start(self, start_delay_s: float, loop_timeout_s: float) -> List:
        received = []
        time.sleep(start_delay_s)
        while True:
            b = self.socket.recv()
            if b.decode() == 'done':
                break
            msg = Message.from_bytes(b)
            print(f'rcvd {msg.msg_id}')
            received.append(msg)
            if loop_timeout_s > 0:
                time.sleep(loop_timeout_s)

        time.sleep(1)
        print(f'num received: {len(received)}')
        print('Done')
        time.sleep(1)
        return received


with ray.init(address='auto'):

    sample_msg = _msg(0)
    print(f'Msg len bytes: {len(sample_msg.to_bytes())}')

    # sender config
    num_events = 10
    to_send = [_msg(i) for i in range(num_events)]

    push_loop_timeout_s = 0.01
    push_snd_hwm = 10
    push_rcv_hwm = None
    push_snd_buf = None #200*1024
    push_rcv_buf = None

    # receiver config
    pull_start_delay_s = 0
    pull_loop_timeout_s = 0.02
    pull_snd_hwm = None
    pull_rcv_hwm = 10
    pull_snd_buf = None
    pull_rcv_buf = None #200*1024

    peer_port = 5152
    # peer_addr = f'tcp://127.0.0.1:{peer_port}'
    peer_addr = 'ipc:///tmp/zmqtest'
    push = Push.remote()
    ray.get(push.initialize.remote(peer_port, push_snd_hwm, push_rcv_hwm, push_snd_buf, push_rcv_buf))
    pull = Pull.remote()
    ray.get(pull.initialize.remote(peer_addr, pull_snd_hwm, pull_rcv_hwm, pull_snd_buf, pull_rcv_buf))
    push.start.remote(to_send, push_loop_timeout_s)
    received = ray.get(pull.start.remote(pull_start_delay_s, pull_loop_timeout_s))

    # calculate backpressure stats
    backpressure_intervals, push_total_time = ray.get(push.get_backpressure_stats.remote())
    backpressure_durations = list(map(lambda e: e[1] - e[0], backpressure_intervals))
    total_backpressured_time = sum(backpressure_durations)
    backpressure = total_backpressured_time / push_total_time
    bprs_stats = f'{len(backpressure_intervals)} backpressure intervals: {backpressure_durations} \n' \
           f'Backpressure {backpressure} \n' \
           f'Total push time: {push_total_time}'
    print(bprs_stats)

    print(to_send)
    print(received)
    # assert to_send == received
    assert num_events == len(received)
