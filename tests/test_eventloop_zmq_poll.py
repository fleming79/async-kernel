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
    async def test_event_handler_busy_resource(self, caller: Caller) -> None:
        with ZMQPoll() as zmq_poll:
            sock = zmq_poll.socket(zmq.SocketType.PAIR)
            with (  # noqa: ASYNC100
                anyio.fail_after(tests.utils.TIMEOUT),
                zmq_poll.event_handler(sock, lambda _, __: None, canceller=None),
                pytest.raises(BusyResourceError),
                zmq_poll.event_handler(sock, lambda _, __: None, canceller=None),
            ):
                raise RuntimeError

    async def test_event_handler_no_pending(self, caller: Caller) -> None:
        with (
            ZMQPoll() as zmq_poll,
            pytest.raises(RuntimeError, match="is not cancellable"),
            zmq_poll.event_handler(zmq_poll.socket(zmq.SocketType.PAIR), lambda _, __: None),
        ):
            raise RuntimeError

    def test_validate_sock(self) -> None:

        with ZMQPoll() as zmq_poll:
            sock = zmq_poll.socket(zmq.SocketType.REP)
            assert zmq_poll.validate_socket(sock) is sock
        assert sock.closed
        with pytest.raises(ValueError, match="Invalid socket detected!"):
            zmq_poll.validate_socket(sock)
        sock.send_multipart([])  # Check this does nothing.

    async def test_zmq_poll(self, anyio_backend: Backend, caller: Caller) -> None:

        def handler(socket: ZMQPollSocket, flags: int):
            queue.append(socket.recv_multipart())

        with (
            ZMQPoll() as zmq_poll,
            zmq_poll.socket(zmq.SocketType.ROUTER) as sock_router,
            zmq_poll.socket(zmq.SocketType.DEALER) as sock_dealer,
            sock_router.bind(addr := f"inproc://test_messaging_{id(self)}"),
            sock_dealer.connect(addr),
            zmq_poll.event_handler(sock_router, handler, canceller=None),
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

    async def test_poll_count(self, caller: Caller):

        with ZMQPoll() as zmq_poll:
            sock_router = zmq_poll.socket(zmq.SocketType.ROUTER)
            sock_dealer = zmq_poll.socket(zmq.SocketType.DEALER)
            addr = "inproc://test_register_poll_callback"
            sock_router.bind(addr)
            sock_dealer.connect(addr)

            done = create_async_waiter()

            N = 3

            def in_thread(sock: ZMQPollSocket, event: int) -> None:
                nonlocal n
                n = n + 1
                assert threading.current_thread() is zmq_poll.thread
                sock.recv()

            n = 0
            for _ in range(N * 2):
                sock_dealer.send(b"")
            with zmq_poll.event_handler(
                sock_router, in_thread, flags=zmq.PollEvent.POLLOUT, count=(N, done.wake), canceller=None
            ):
                await done
                assert n == N
            assert n == N

    async def test_stress_socket_threadsafe(self, caller: Caller) -> None:
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
                with zmq_poll.event_handler(pub, lambda _, __: None, count=(1, ready.wake), canceller=None):
                    await ready

                barrier = Latch(n - 1)
                done = create_async_waiter()
                total = 0
                target = sum(range(1, n))
                assert target

                def accumulate_pub(sock: ZMQPollSocket, event: int, target=target, done=done):
                    nonlocal total
                    msg = sock.recv_multipart()
                    total = total + int(msg[1])
                    if total == target:
                        done.wake()

                async def f(i: int, barrier=barrier, pub=pub):
                    await barrier
                    pub.send_multipart([b"stream.stdout", str(i).encode()])

                with zmq_poll.event_handler(sub, accumulate_pub, canceller=None):
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
        with pytest.raises(RuntimeError, match="stopped"), zmq_poll:
            None  # noqa: B018  # pyright: ignore[reportUnusedExpression]
        # Stopped
        with pytest.raises(RuntimeError, match=match):
            assert zmq_poll.execute(threading.current_thread) is zmq_poll.thread
        with pytest.raises(RuntimeError, match=match):
            assert await zmq_poll.execute_async(threading.current_thread) is zmq_poll.thread

    async def test_poll_socket_states(self, caller: Caller):
        zmq_poll = ZMQPoll()
        match = "Execution is only support while in context"
        # Pre-running
        with pytest.raises(RuntimeError, match=match):
            zmq_poll.socket(zmq.SocketType.DEALER)
        # Running
        with zmq_poll:
            sock = zmq_poll.socket(zmq.SocketType.DEALER)

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

    async def test_event_handler_default_canceller(self, caller: Caller):
        """Test the handler cancels the pending."""
        with ZMQPoll() as zmq_poll:

            async def f():
                with zmq_poll.event_handler(zmq_poll.socket(zmq.SocketType.REP), lambda _, __: None):
                    await barrier
                    await create_async_waiter()

            barrier = Latch(4)
            caller.queue_call(f)
            pen = caller.queue_get(f)
            assert pen
            pending = [caller.call_soon(f), caller.to_thread(f), pen]
            await barrier
        await pen.wait(result=False)

        assert all(p.cancelled() for p in pending)

    async def test_catches_cancel(self, caller: Caller):

        def bad_canceller():
            resume.wake()
            raise TypeError

        resume, done = create_async_waiter(), create_async_waiter()
        with ZMQPoll() as zmq_poll:

            async def f():
                sock = zmq_poll.socket(zmq.SocketType.REP)
                with zmq_poll.event_handler(sock, lambda _, __: None, canceller=bad_canceller):
                    # Initial cancellation (should not normally be invoked externally)
                    zmq_poll.stopped.set_result(None)
                    await done

            pen = caller.call_soon(f)
            await resume

        done.wake()
        await pen.wait(result=False)
