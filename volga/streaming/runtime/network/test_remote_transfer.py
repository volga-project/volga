import time
import unittest
from threading import Thread

import zmq

from volga.streaming.runtime.network.channel import RemoteChannel
from volga.streaming.runtime.network.stats import StatsEvent
from volga.streaming.runtime.network.transfer.io_loop import IOLoop, Direction
from volga.streaming.runtime.network.transfer.local.data_reader import DataReader
from volga.streaming.runtime.network.transfer.local.data_writer import DataWriter
from volga.streaming.runtime.network.transfer.remote.remote_transfer_handler import RemoteTransferHandler


class TestRemoteTransfer(unittest.TestCase):

    def test_one_to_one_locally(self):
        num_items = 10000
        source_node_id = 'node_1'
        target_node_id = 'node_2'
        channel = RemoteChannel(
            channel_id='ch_0',
            source_local_ipc_addr='ipc:///tmp/source_local',
            source_node_ip='127.0.0.1',
            source_node_id=source_node_id,
            target_local_ipc_addr='ipc:///tmp/target_local',
            target_node_ip='127.0.0.1',
            target_node_id=target_node_id,
            port=1234
        )
        io_loop = IOLoop()
        zmq_ctx = zmq.Context.instance(io_threads=10)
        data_writer = DataWriter(
            name='test_writer',
            source_stream_name='0',
            channels=[channel],
            node_id=source_node_id,
            zmq_ctx=zmq_ctx
        )
        data_reader = DataReader(
            name='test_reader',
            channels=[channel],
            node_id=target_node_id,
            zmq_ctx=zmq_ctx
        )
        transfer_sender = RemoteTransferHandler(
            channels=[channel],
            zmq_ctx=zmq_ctx,
            direction=Direction.SENDER
        )
        transfer_receiver = RemoteTransferHandler(
            channels=[channel],
            zmq_ctx=zmq_ctx,
            direction=Direction.RECEIVER
        )
        io_loop.register(data_writer)
        io_loop.register(data_reader)
        io_loop.register(transfer_sender)
        io_loop.register(transfer_receiver)
        io_loop.start()

        to_send = [{'i': i} for i in range(num_items)]
        def write():
            for msg in to_send:
                data_writer._write_message(channel.channel_id, msg)


        rcvd = []
        def read():
            t = time.time()
            while True:
                if time.time() - t > 10:
                    raise RuntimeError('Timeout reading data')
                msg = data_reader.read_message()
                if msg is None:
                    time.sleep(0.001)
                    continue
                else:
                    rcvd.append(msg)
                if len(rcvd) == num_items:
                    break
        wt = Thread(target=write)

        wt.start()
        read()
        time.sleep(1)

        reader_stats = data_reader.stats
        writer_stats = data_writer.stats
        transfer_sender_stats = transfer_sender.stats
        transfer_receiver_stats = transfer_receiver.stats

        print(reader_stats)
        print(writer_stats)
        print(transfer_sender_stats)
        print(transfer_receiver_stats)

        assert to_send == sorted(rcvd, key=lambda e: e['i'])

        # TODO stats asserts
        # assert reader_stats.get_counter_for_event(StatsEvent.MSG_RCVD)[channel.channel_id] == num_items
        # assert reader_stats.get_counter_for_event(StatsEvent.ACK_SENT)[channel.channel_id] == num_items
        # assert writer_stats.get_counter_for_event(StatsEvent.ACK_RCVD)[channel.channel_id] == num_items
        # assert writer_stats.get_counter_for_event(StatsEvent.MSG_SENT)[channel.channel_id] == num_items

        print('assert ok')
        io_loop.close()
        wt.join(5)


if __name__ == '__main__':
    t = TestRemoteTransfer()
    t.test_one_to_one_locally()