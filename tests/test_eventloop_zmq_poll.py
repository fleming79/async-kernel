from __future__ import annotations

import gc
import threading
import weakref
from typing import TYPE_CHECKING

import anyio
import pytest
import zmq
from aiologic import BusyResourceError, Latch
from aiologic.lowlevel import create_async_event, create_async_waiter

import tests.utils
from async_kernel.common import SingleAsyncQueue
from async_kernel.event_loop.zmq_poll import ZMQPoll, ZMQPollSocket

if TYPE_CHECKING:
    from async_kernel import Caller
    from async_kernel.typing import Backend

# pyright: reportPrivateUsage=false


class Test_zmq_Poll:
    async def test_busy_event(self, caller: Caller) -> None:
        with ZMQPoll() as zmq_poll:
            sock = zmq_poll.socket(zmq.SocketType.PAIR)
            with (
                anyio.fail_after(tests.utils.TIMEOUT),
                zmq_poll.event_handler(sock, lambda _, __: None),
                pytest.raises(BusyResourceError),
                zmq_poll.event_handler(sock, lambda _, __: None),
            ):
                await anyio.sleep(0.1)

    def test_validate_sock(self):

        with pytest.raises(TypeError, match="is not valid"):
            ZMQPoll._validate_socket(None)

    async def test_zmq_poll(self, anyio_backend: Backend, caller: Caller):

        def handler(socket: zmq.Socket, flags: int):
            queue.append(socket.recv_multipart())

        with (
            ZMQPoll() as zmq_poll,
            zmq_poll.socket(zmq.SocketType.ROUTER) as sock_router,
            zmq_poll.socket(zmq.SocketType.DEALER) as sock_dealer,
            sock_router.bind(addr := f"inproc://test_messaging_{id(self)}"),
            sock_dealer.connect(addr),
            zmq_poll.event_handler(sock_router, handler),
        ):
            queue: SingleAsyncQueue[list[bytes]] = SingleAsyncQueue()
            sock_dealer.send(b"hello")
            async for msg in queue:
                sock_dealer.send(b"hello2")
                if b"done" in msg:
                    break
                sock_dealer.send(b"done")

    async def test_gc(self, caller: Caller):

        cleaned = create_async_event()
        with ZMQPoll() as zmq_poll:
            pass
        ref = weakref.ref(zmq_poll)
        weakref.finalize(zmq_poll, cleaned.set)
        del zmq_poll
        with anyio.move_on_after(2):
            await cleaned

        if obj := ref():
            referrers = gc.get_referrers(obj)
            assert not referrers

    async def test_poll_limit(self, caller: Caller):

        with ZMQPoll() as zmq_poll:
            sock_router = zmq_poll.socket(zmq.SocketType.ROUTER)
            sock_dealer = zmq_poll.socket(zmq.SocketType.DEALER)
            addr = "inproc://test_register_poll_callback"
            sock_router.bind(addr)
            sock_dealer.connect(addr)

            done = create_async_waiter()

            N = 3

            def in_thread(sock: zmq.Socket, event: int) -> None:
                nonlocal n
                n = n + 1
                assert threading.current_thread() is zmq_poll.thread
                sock.recv()

            n = 0
            for _ in range(N * 2):
                sock_dealer.send(b"")
            with zmq_poll.event_handler(sock_router, in_thread, flags=zmq.PollEvent.POLLOUT, countdown=(N, done.wake)):
                await done
                assert n == N
            assert n == N

    async def test_stress_socket_threadsafe(self, caller: Caller):
        """Stress test interface.iopub_send and the associated socket."""
        for n in range(2, 20, 4):
            with (
                ZMQPoll() as zmq_poll,
                zmq_poll.socket(socket_type=zmq.SocketType.XPUB) as pub,
                zmq_poll.socket(zmq.SocketType.SUB) as sub,
                pub.bind(addr := "inproc://socket_proxy_test"),
                sub.connect(addr),
            ):
                sub.subscribe(b"")
                # Wait for sub to connection
                ready = create_async_waiter()
                with zmq_poll.event_handler(pub, lambda _, __: None, countdown=(1, ready.wake)):
                    await ready

                barrier = Latch(n - 1)
                done = create_async_waiter()
                total = 0
                target = sum(range(1, n))
                assert target

                def accumulate_pub(sock: zmq.Socket, event: int, target=target, done=done):
                    nonlocal total
                    msg = sock.recv_multipart()
                    total = total + int(msg[1])
                    if total == target:
                        done.wake()

                async def f(i: int, barrier=barrier, pub=pub):
                    await barrier
                    pub.send_multipart([b"stream.stdout", str(i).encode()])

                with anyio.fail_after(tests.utils.TIMEOUT), zmq_poll.event_handler(sub, accumulate_pub):
                    for i in range(1, n):
                        caller.to_thread(f, i)
                    await done
                sub.unsubscribe(b"")

    async def test_poll_execute_states(self, caller: Caller):
        zmq_poll = ZMQPoll()
        match = "Execution is only support while in context"
        # Pre-running
        with pytest.raises(RuntimeError, match=match):
            zmq_poll.execute(lambda: 1 + 1)
        with pytest.raises(RuntimeError, match=match):
            await zmq_poll.execute_async(lambda: 1 + 1)
        # Running
        with zmq_poll:
            assert (await zmq_poll.execute_async(lambda: 1 + 1)) == 2
            assert (zmq_poll.execute(lambda: 1 + 1)) == 2
        # Stopped
        with pytest.raises(RuntimeError, match=match):
            assert zmq_poll.execute(threading.current_thread) is zmq_poll.thread
        with pytest.raises(RuntimeError, match=match):
            assert await zmq_poll.execute_async(threading.current_thread) is zmq_poll.thread

    async def test_poll_socket_states(self, caller: Caller):
        zmq_poll = ZMQPoll()
        match = "Execution is only support while in context"
        # Pre-running
        sock = zmq_poll.socket(zmq.SocketType.DEALER)
        with pytest.raises(RuntimeError, match=match):
            sock.bind("inproc://local")
        # Running
        with zmq_poll, sock.bind("inproc://local"):
            assert not sock.closed
        assert sock.closed
        # Stopped
        with pytest.raises(RuntimeError, match="is stopped!"):
            zmq_poll.socket(zmq.SocketType.DEALER)
        with pytest.raises(RuntimeError, match="is stopped!"):
            ZMQPollSocket(zmq.SocketType.DEALER, zmq_poll=zmq_poll)

    def test_set_cuve_values(self):
        with ZMQPoll() as zmq_poll:
            server = zmq_poll.socket(zmq.SocketType.SERVER)
            server.curve_secretkey, server.curve_publickey = zmq.curve_keypair()
            server.curve_server = True
            server.bind(addr := "inproc://local")

            dealer = zmq_poll.socket(zmq.SocketType.DEALER)
            dealer.connect(addr)
            dealer.send_multipart([b"test"])

            frames = server.recv_multipart()
            assert frames == [b"test"]
