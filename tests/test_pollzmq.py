from __future__ import annotations

import gc
import weakref
from typing import TYPE_CHECKING

import anyio
import pytest
import zmq
from aiologic import BusyResourceError
from aiologic.lowlevel import create_async_event

from async_kernel.common import SingleAsyncQueue
from async_kernel.interface.poll_zmq import PollZMQ

if TYPE_CHECKING:
    from async_kernel import Caller
    from async_kernel.typing import Backend

# pyright: reportPrivateUsage=false


class Test_PollZMQ:
    def test_busy_event(self):
        pzmq = PollZMQ()
        sock = zmq.Context(0).socket(zmq.PAIR)
        with pzmq.event_handler(sock, lambda _, __: None):
            with pytest.raises(BusyResourceError), pzmq.event_handler(sock, lambda _, __: None):
                pass
            with pytest.raises(BusyResourceError):
                pzmq.poll(sock)

        pzmq.stop()
        sock.context.destroy(0)

    def test_stop_before_start(self):
        pzmq = PollZMQ()
        pzmq.stop()
        assert pzmq.stopped
        with pytest.raises(AssertionError):
            pzmq.start()

    def test_validate_sock(self):
        pzmq = PollZMQ()
        pzmq.stop()
        with pytest.raises(TypeError):
            pzmq.poll(None)  # pyright: ignore[reportArgumentType]
        with pytest.raises(TypeError), pzmq.event_handler(None, lambda _, __: None):  # pyright: ignore[reportArgumentType]
            pass

    @pytest.mark.parametrize("mode", ["simple", "delayed start", "threads"])
    async def test_poll_zmq(self, anyio_backend: Backend, caller: Caller, mode: str):

        async def func(i: int):
            addr = f"inproc://test_messaging_{id(func)}_{i}"
            sock_router: zmq.Socket = sock.context.socket(zmq.ROUTER)
            sock_dealer: zmq.Socket = sock.context.socket(zmq.DEALER)
            sock_router.bind(addr)
            sock_dealer.connect(addr)
            queue: SingleAsyncQueue[list[bytes]] = SingleAsyncQueue()

            def handler(socket: zmq.Socket, flags: int):
                queue.append(socket.recv_multipart())

            with wrt.event_handler(sock_router, handler):
                wrt.start()

                sock_dealer.send(b"hello")
                async for msg in queue:
                    sock_dealer.send(b"hello2")
                    if b"done" in msg:
                        break
                    sock_dealer.send(b"done")

        wrt = PollZMQ()
        sock = zmq.Context(0).socket(zmq.PAIR)
        if mode in ["simple", "threads"]:
            wrt.start()
        try:
            if mode in ["simple", "delayed start"]:
                await func(0)
                wrt.stop()

                with pytest.raises(RuntimeError, match="is stopped!"):  # noqa: SIM117
                    with wrt.event_handler(sock, lambda _, __: None):
                        pass
            else:
                async with caller.create_pending_group(mode=1):
                    for i in range(10):
                        caller.to_thread(func, i)

        finally:
            wrt.stop()
            sock.context.destroy(0)

    async def test_gc(self, anyio_backend: Backend):
        cleaned = create_async_event()
        pzmq = PollZMQ()

        ref = weakref.ref(pzmq)
        weakref.finalize(pzmq, cleaned.set)
        del pzmq
        with anyio.move_on_after(2):
            await cleaned

        if obj := ref():
            referrers = gc.get_referrers(obj)
            assert not referrers

    async def test_poll_callback(self, anyio_backend: Backend):
        context = zmq.Context(0)

        sock_router: zmq.Socket = context.socket(zmq.ROUTER)
        sock_dealer: zmq.Socket = context.socket(zmq.DEALER)
        pzmq = PollZMQ().start()
        try:
            sock_router.bind(addr := "inproc://test_register_poll_callback")
            sock_dealer.connect(addr)
            queue: SingleAsyncQueue[bytes] = SingleAsyncQueue()

            def router(socket: zmq.Socket, flags: int) -> None:
                queue.append(socket.recv())

            pen = pzmq.poll(sock_router, flags=zmq.PollEvent.POLLOUT)
            with pytest.raises(BusyResourceError):
                pzmq.poll(sock_router, flags=zmq.PollEvent.POLLOUT)

            with pzmq.event_handler(sock_router, router):
                sock_dealer.send(b"hello")
                async for _ in queue:
                    if pen.done():
                        return
                    sock_dealer.send(b"hello")

        finally:
            pzmq.stop()
            context.destroy(0)
