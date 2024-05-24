import time
from typing import Dict, Tuple, List

import simplejson

import ray
import zmq

# HWM
# https://stackoverflow.com/questions/53356451/pyzmq-high-water-mark-not-working-on-pub-socket
# https://stackoverflow.com/questions/22613737/how-could-i-set-hwm-in-the-push-pull-pattern-of-zmq/26424611#26424611

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


def _msg(msg_id: int) -> Message:
    payload = {'value': f'msg_{msg_id}', 'dummy_heavy_load': 'a'*10000}
    return Message(msg_id=msg_id, payload=payload)


@ray.remote
class Push:

    def __init__(self):
        self.port = None
        self.socket = None
        self.total_time = None
        self.backpressure_intervals = []

    def initialize(self, port: int, snd_hwm: int, rcv_hwm: int):
        self.port = port
        context = zmq.Context()
        self.socket = context.socket(zmq.PUSH)
        self.socket.setsockopt(zmq.SNDHWM, snd_hwm)
        self.socket.setsockopt(zmq.RCVHWM, rcv_hwm)
        # self.socket
        self.socket.bind(f'tcp://*:{port}')
        print('Push inited')

    def start(self, num_events: int, loop_timeout_s: float):
        start = time.time()
        for i in range(num_events):
            msg = _msg(i)
            send_start = time.time()
            is_backpressured = False
            while True:
                try:
                    self.socket.send_string(msg.ser(), zmq.NOBLOCK)
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
                time.sleep(0.01)
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

    def initialize(self, peer_addr: str, snd_hwm: int, rcv_hwm: int):
        context = zmq.Context()
        self.socket = context.socket(zmq.PULL)
        self.socket.setsockopt(zmq.SNDHWM, snd_hwm)
        self.socket.setsockopt(zmq.RCVHWM, rcv_hwm)
        self.socket.connect(peer_addr)
        print('Pull inited')

    def start(self, start_delay_s: float, loop_timeout_s: float) -> int:
        num_received = 0
        time.sleep(start_delay_s)
        while True:
            s = self.socket.recv_string()
            if s == 'done':
                break
            msg = Message.de(s)
            print(f'rcvd {msg.msg_id}')
            num_received += 1
            time.sleep(loop_timeout_s)

        time.sleep(1)
        print(f'num received: {num_received}')
        print('Done')
        time.sleep(1)
        return num_received


with ray.init(address='auto'):

    # sender config
    num_events = 300
    push_loop_timeout_s = 0.01
    push_snd_hwm = 100
    push_rcv_hwm = 0

    # receiver config
    pull_start_delay_s = 0
    pull_loop_timeout_s = 0.02
    pull_snd_hwm = 1
    pull_rcv_hwm = 1

    peer_port = 5152
    peer_addr = f'tcp://127.0.0.1:{peer_port}'
    push = Push.remote()
    ray.get(push.initialize.remote(peer_port, push_snd_hwm, push_rcv_hwm))
    pull = Pull.remote()
    ray.get(pull.initialize.remote(peer_addr, pull_snd_hwm, pull_rcv_hwm))
    push.start.remote(num_events, push_loop_timeout_s)
    num_received = ray.get(pull.start.remote(pull_start_delay_s, pull_loop_timeout_s))

    # calculate backpressure stats
    backpressure_intervals, push_total_time = ray.get(push.get_backpressure_stats.remote())
    backpressure_durations = list(map(lambda e: e[1] - e[0], backpressure_intervals))
    total_backpressured_time = sum(backpressure_durations)
    backpressure = total_backpressured_time / push_total_time
    bprs_stats = f'{len(backpressure_intervals)} backpressure intervals: {backpressure_durations} \n' \
           f'Backpressure {backpressure} \n' \
           f'Total push time: {push_total_time}'
    print(bprs_stats)

    assert num_events == num_received
