import time
import random
from socket import socket

import ray
import zmq

# HWM
# https://stackoverflow.com/questions/53356451/pyzmq-high-water-mark-not-working-on-pub-socket

# EAGAIN
# https://github.com/zeromq/libzmq/issues/1332


@ray.remote
class Push:

    def __init__(self):
        self.port = None
        self.socket = None

    def initialize(self, port: int):
        self.port = port
        context = zmq.Context()
        self.socket = context.socket(zmq.PUSH)
        self.socket.bind(f'tcp://127.0.0.1:{port}')
        pass

    def start(self):
        num_events = 1000000
        for i in range(num_events):
            # topic = random.randrange(10000, 10002)
            # messagedata = random.randrange(1, 215) - 80
            # self.socket.send_string("%d %d" % (topic, messagedata))
            msg = f'msg_{i}'
            self.socket.send_string(msg)
            # time.sleep(0.0000001)
            # print(f'sent {msg}')
        time.sleep(1)
        self.socket.send_string('done')

@ray.remote
class Pull:
    def __init__(self):
        self.port = None
        self.socket = None

    def initialize(self, producer_addr: str):
        context = zmq.Context()
        self.socket = context.socket(zmq.PULL)
        self.socket.connect(producer_addr)

    def start(self):
        # topicfilter = '10001'
        # self.socket.setsockopt_string(zmq.SUBSCRIBE, topicfilter)
        # self.socket.subscribe('') # subscribe to all topics
        # Process 5 updates
        received = 0
        while True:
            # string = self.socket.recv()
            # topic, messagedata = string.split()
            s = self.socket.recv_string()
            # print(s)
            if s == 'done':
                break
            received += 1

        time.sleep(1)

        print(f'received {received}')
        print('Done')
        time.sleep(1)

with ray.init(address='auto'):
    pub_port = 5151
    pub_addr = f'tcp://127.0.0.1:{pub_port}'
    push = Push.remote()
    ray.get(push.initialize.remote(pub_port))
    pull = Pull.remote()
    ray.get(pull.initialize.remote(pub_addr))
    push.start.remote()
    ray.get(pull.start.remote())
