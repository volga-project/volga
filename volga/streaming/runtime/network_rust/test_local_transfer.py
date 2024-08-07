import random
import unittest
from typing import Optional, Any

import ray
import time

from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from volga.streaming.runtime.network_rust.channel import LocalChannel
from volga.streaming.runtime.network_rust.testing_utils import TestReader, TestWriter, start_ray_io_handler_actors


class TestLocalTransfer(unittest.TestCase):

    def test_one_to_one_on_ray(self, ray_addr: Optional[str] = None, runtime_env: Optional[Any] = None):
        channel = LocalChannel(
            channel_id='1',
            ipc_addr='ipc:///tmp/zmqtest',
        )
        ray.init(address=ray_addr, runtime_env=runtime_env)
        num_msgs = 10000
        msg_size = 1024
        to_send = [{'i': str(random.randint(0, 9)) * msg_size} for _ in range(num_msgs)]
        batch_size = 1
        writer_delay_s = 0

        job_name = f'job-{int(time.time())}'
        # make sure we schedule on the same node
        all_nodes = ray.nodes()
        if len(all_nodes) >= 2:
            # skip head node
            no_head = list(filter(lambda n: 'node:__internal_head__' not in n['Resources'], all_nodes))
            node = random.sample(no_head, 1)[0]
        else:
            node = all_nodes[0]
        reader = TestReader.options(
            num_cpus=0,
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                node_id=node['NodeID'],
                soft=False
            )
        ).remote(job_name, channel, num_msgs)
        writer = TestWriter.options(
            num_cpus=0,
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                node_id=node['NodeID'],
                soft=False
            )
        ).remote(job_name, channel, batch_size, writer_delay_s)

        start_ray_io_handler_actors([reader, writer])

        # make sure Ray has enough time to start actors
        time.sleep(1)
        t = time.perf_counter()
        writer.send_items.remote(to_send)
        rcvd = ray.get(reader.receive_items.remote())
        t = time.perf_counter() - t
        throughput = num_msgs/t
        print(f'Finished in {t} s, throughput {throughput} msg/s')
        time.sleep(1)

        assert to_send == rcvd

        print('assert ok')

        ray.get(reader.close.remote())
        ray.get(writer.close.remote())

        time.sleep(1)

        ray.shutdown()


if __name__ == '__main__':
    t = TestLocalTransfer()
    t.test_one_to_one_on_ray()