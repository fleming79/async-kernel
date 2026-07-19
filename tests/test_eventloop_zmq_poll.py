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
from async_kernel.event_loop.zmq_poll import Poll

if TYPE_CHECKING:
    from async_kernel import Caller
    from async_kernel.typing import Backend

# pyright: reportPrivateUsage=false


class Test_zmq_Poll:
    async def test_busy_event(self, caller: Caller) -> None:
        with Poll() as poll:
            sock = poll._zmq_context.socket(zmq.PAIR)
            with (
                anyio.fail_after(tests.utils.TIMEOUT),
                poll.event_handler(sock, lambda _, __: None),
                pytest.raises(BusyResourceError),
                poll.event_handler(sock, lambda _, __: None),
            ):
                pass

    def test_validate_sock(self):

        with pytest.raises(TypeError, match="is not valid"):
            Poll._validate_socket(None)  # pyright: ignore[reportArgumentType]

    async def test_zmq_poll(self, anyio_backend: Backend, caller: Caller):

        def handler(socket: zmq.Socket, flags: int):
            queue.append(socket.recv_multipart())

        with Poll() as poll:
            sock_router = poll.socket(zmq.SocketType.ROUTER)
            sock_dealer = poll.socket(zmq.SocketType.DEALER)
            sock_router.bind(addr := f"inproc://test_messaging_{id(self)}")
            sock_dealer.connect(addr)

            with poll.event_handler(sock_router, handler):
                queue: SingleAsyncQueue[list[bytes]] = SingleAsyncQueue()
                sock_dealer.send(b"hello")
                async for msg in queue:
                    sock_dealer.send(b"hello2")
                    if b"done" in msg:
                        break
                    sock_dealer.send(b"done")

    async def test_gc(self, caller: Caller):

        cleaned = create_async_event()
        with Poll() as poll:
            pass
        ref = weakref.ref(poll)
        weakref.finalize(poll, cleaned.set)
        del poll
        with anyio.move_on_after(2):
            await cleaned

        if obj := ref():
            referrers = gc.get_referrers(obj)
            assert not referrers

    async def test_poll_limit(self, caller: Caller):

        with Poll() as poll:
            sock_router = poll.socket(zmq.SocketType.ROUTER)
            sock_dealer = poll.socket(zmq.SocketType.DEALER)
            addr = "inproc://test_register_poll_callback"
            sock_router.bind(addr)
            sock_dealer.connect(addr)

            done = create_async_waiter()

            N = 3

            def in_thread(sock: zmq.Socket, event: int) -> None:
                nonlocal n
                n = n + 1
                assert threading.current_thread() is poll.thread
                sock.recv()

            n = 0
            for _ in range(N * 2):
                sock_dealer.send(b"")
            with poll.event_handler(sock_router, in_thread, flags=zmq.PollEvent.POLLOUT, countdown=(N, done.wake)):
                await done
                assert n == N
            assert n == N

    async def test_stress_socket_threadsafe(self, caller: Caller):
        """Stress test interface.iopub_send and the associated socket."""
        for n in range(2, 20, 4):
            with Poll() as poll, poll.socket(zmq.SocketType.XPUB) as pub, poll.socket(zmq.SocketType.SUB) as sub:
                pub.bind(addr := "inproc://socket_proxy_test")
                sub.connect(addr)
                sub.subscribe(b"")
                # Wait for sub to connection
                ready = create_async_waiter()
                with poll.event_handler(pub, lambda _, __: None, countdown=(1, ready.wake)):
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

                with anyio.fail_after(tests.utils.TIMEOUT), poll.event_handler(sub, accumulate_pub):
                    for i in range(1, n):
                        caller.to_thread(f, i)
                    await done
