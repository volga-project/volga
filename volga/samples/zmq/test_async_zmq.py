import asyncio
import functools
import unittest
import time
from threading import Thread

import zmq
import zmq.asyncio as zmq_async
from zmq.utils.monitor import recv_monitor_message


class TestAsyncZMQ(unittest.TestCase):

    def test_async_zmq(self):

        ADDR = 'ipc:///tmp/zmq_test'
        is_running = True
        socks = {}
        monitor_socks = {}
        monitor_poller = zmq_async.Poller()

        def init_r_sock():
            zmq_ctx = zmq_async.Context.instance()
            r_sock = zmq_ctx.socket(zmq.PULL)
            r_sock.setsockopt(zmq.LINGER, 0)
            r_sock.connect(ADDR)
            socks['r'] = r_sock
            ms = r_sock.get_monitor_socket()

            monitor_poller.register(ms, zmq.POLLIN)
            monitor_socks[ms] = 'r'
            print('read socket ready')

        def init_w_sock():
            zmq_ctx = zmq_async.Context.instance()
            w_sock = zmq_ctx.socket(zmq.PUSH)
            w_sock.setsockopt(zmq.LINGER, 0)
            w_sock.bind(ADDR)
            socks['w'] = w_sock
            ms = w_sock.get_monitor_socket()
            monitor_poller.register(ms, zmq.POLLIN)
            monitor_socks[ms] = 'w'
            print('write socket ready')

        async def w():
            w_sock = socks['w']
            print('write started')
            for i in range(10):
                print(f'sent {i} started')
                await w_sock.send_string(f'{i}')
                await asyncio.sleep(0.1)
                print(f'sent {i} finished')

        async def r():
            r_sock = socks['r']
            print('read started')
            while is_running:
                data = await r_sock.recv_string()
                print(f'rcv_{data}')

        async def _close_r_sock():
            print('started r close')
            sock = socks['r']
            await sock.close(linger=0)
            print('r closed')

        async def _close_w_sock():
            print('started w close')
            sock = socks['w']
            await sock.close(linger=0)
            print('w closed')

        write_loop = asyncio.new_event_loop()
        read_loop = asyncio.new_event_loop()
        monitor_loop = asyncio.new_event_loop()

        def run_read_loop():
            asyncio.set_event_loop(read_loop)
            read_loop.run_forever()

        def run_write_loop():
            asyncio.set_event_loop(write_loop)
            write_loop.run_forever()

        rt = Thread(target=functools.partial(run_read_loop))
        wt = Thread(target=functools.partial(run_write_loop))

        is_monitoring = True
        # https://github.com/zeromq/pyzmq/blob/main/examples/monitoring/zmq_monitor_class.py
        async def watch_monitor_socks():
            print('watch started')
            while is_monitoring:
                socks_masks = await monitor_poller.poll()
                for (sock, _) in socks_masks:
                    name = monitor_socks[sock]
                    assert isinstance(sock, zmq_async.Socket)
                    data = await recv_monitor_message(sock)
                    print(f'[Monitor][{name}]: {data}')

        def run_monitor_loop():
            asyncio.set_event_loop(monitor_loop)
            monitor_loop.run_forever()

        mt = Thread(target=run_monitor_loop)

        def close_r_loop():
            print('started rt close')
            asyncio.run_coroutine_threadsafe(_close_r_sock(), read_loop)
            read_loop.call_soon_threadsafe(read_loop.stop)
            rt.join(timeout=1)
            print('rt closed')

        def close_w_loop():
            print('started wt close')
            asyncio.run_coroutine_threadsafe(_close_w_sock(), write_loop)
            write_loop.call_soon_threadsafe(write_loop.stop)
            wt.join(timeout=1)
            print('wt closed')

        mt.start()

        init_r_sock()
        time.sleep(2)
        init_w_sock()
        wt.start()
        rt.start()
        asyncio.run_coroutine_threadsafe(watch_monitor_socks(), monitor_loop)

        asyncio.run_coroutine_threadsafe(w(), write_loop)
        asyncio.run_coroutine_threadsafe(r(), read_loop)
        time.sleep(5)
        is_running = False
        close_w_loop()
        close_r_loop()
        wt.join(timeout=1)
        rt.join(timeout=1)
        is_monitoring = False
        mt.join(timeout=1)


if __name__ == '__main__':
    t = TestAsyncZMQ()
    t.test_async_zmq()