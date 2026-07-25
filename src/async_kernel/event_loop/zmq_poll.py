"""Defines a background ZMQ poller thread."""

from __future__ import annotations

import logging
import threading
import weakref
from collections import deque
from collections.abc import Callable, Generator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Literal, Self

import zmq
from aiologic import BinarySemaphore, BusyResourceError
from aiologic.lowlevel import create_green_waiter
from typing_extensions import override
from zmq.backend import zmq_poll

from async_kernel import utils
from async_kernel.common import Fixed
from async_kernel.pending import Pending
from async_kernel.typing import P, T

if TYPE_CHECKING:
    from collections.abc import Callable, Generator, Sequence

    from zmq.sugar.socket import _SocketContext  # pyright: ignore[reportPrivateUsage]
    from zmq.sugar.tracker import MessageTracker


__all__ = ["ZMQPoll", "ZMQPollSocket"]


class ZMQPollSocket(zmq.sugar.Socket[bytes]):
    """A zmq socket which uses a [ZMQPoll][] thread to perform sensitive operation.

    For best reliability, sensitive operations are performed in the poll's thread.
    This socket will close automatically when Poll is stopped. It is still better to
    close the  socket when it is no longer required. The socket must be bound/connected
    in the context of the [ZMQPoll][] to which it is associated.
    """

    __slots__ = ["__poll", "_close_cb", "lock"]

    _close_cb: Callable
    _zmq_poll_ref: weakref.ref[ZMQPoll]
    lock: BinarySemaphore

    if TYPE_CHECKING:
        # magic attributes cannot be be stored in `__annotations__`.
        closed: bool
        """Set to `True` when the socket is closed."""

        curve_secretkey: bytes | None
        """The curve encryption secret key."""

        curve_publickey: bytes | None
        """The curve encryption public key."""

        curve_server: bool
        """If it is the server (must be bound)."""

        curve_serverkey: bytes | None
        """The key to use when the socket is not a serve (normally matches curve_publickey)."""

    def __init__(
        self,
        socket_type: zmq.SocketType,
        shadow: ZMQPollSocket | int = 0,
        *,
        zmq_poll: ZMQPoll,
        copy_threshold: int | None = None,
    ) -> None:
        self.lock = BinarySemaphore()
        self._zmq_poll_ref = weakref.ref(zmq_poll)

        if zmq_poll.stopped.done():
            msg = f"{zmq_poll} is stopped!"
            raise RuntimeError(msg)
        super().__init__(
            ctx_or_socket=zmq_poll._zmq_context,  # pyright: ignore[reportPrivateUsage],
            socket_type=int(socket_type),
            copy_threshold=copy_threshold,
        )
        zmq_poll.stopped.add_done_callback(cb := lambda _: self.close())
        self._close_cb = cb

    @property
    def zmq_poll(self) -> ZMQPoll:
        return self._zmq_poll_ref()  # pyright: ignore[reportReturnType]

    @override
    def set(self, option: int, value: int | bytes | str) -> None:
        return self.zmq_poll.execute(super().set, option, value)

    @override
    def send_multipart(
        self,
        msg_parts: Sequence,
        flags: int = 0,
        copy: bool = True,
        track: bool = False,
        **kwargs,
    ) -> MessageTracker | None:
        with self.lock:
            return super().send_multipart(msg_parts, flags, copy, track)

    @override
    def close(self, linger=None) -> None:
        self.zmq_poll.stopped.remove_done_callback(self._close_cb)
        self.zmq_poll.execute(super().close, linger)

    @override
    def bind(self, addr: str) -> _SocketContext[Self]:
        return self.zmq_poll.execute(super().bind, addr)

    @override
    def unbind(self, url: str) -> None:
        return self.zmq_poll.execute(super().unbind, url)

    @override
    def connect(self, addr: str) -> _SocketContext[Self]:
        return self.zmq_poll.execute(super().connect, addr)

    @override
    def disconnect(self, url: str) -> None:
        return self.zmq_poll.execute(super().disconnect, url)

    @override
    def subscribe(self, topic: str | bytes) -> None:
        topic = topic.encode("utf8") if isinstance(topic, str) else topic
        return self.zmq_poll.execute(super().subscribe, topic)

    @override
    def unsubscribe(self, topic: str | bytes) -> None:
        topic = topic.encode("utf8") if isinstance(topic, str) else topic
        return self.zmq_poll.execute(super().unsubscribe, topic)


T_key = tuple[Any | ZMQPollSocket, int]


class ZMQPoll:
    """A simple [zmq_poll](https://libzmq.readthedocs.io/en/latest/zmq_poll.html) based event loop.

    This event loop is synchronous and is intended for quick message dispatch to separate threads
    for handling.
    """

    stopped: Fixed[Any, Pending[None]] = Fixed(Pending)

    def __init__(self, *, log: logging.Logger | logging.LoggerAdapter | None = None) -> None:

        def socket_factory(
            ctx_or_socket: zmq.Context | None = None,
            socket_type: int = 0,
            *,
            copy_threshold: int | None = None,
        ) -> ZMQPollSocket:
            return ZMQPollSocket(
                socket_type=zmq.SocketType(socket_type),
                copy_threshold=copy_threshold,
                zmq_poll=ref(),  # pyright: ignore[reportArgumentType]
            )

        self._zmq_context = zmq.Context()
        ref = weakref.ref(self)
        self._zmq_context._socket_class = socket_factory  # pyright: ignore[reportPrivateUsage, reportAttributeAccessIssue]
        self._handlers: dict[T_key, Callable[[ZMQPollSocket, int], Any]] = {}
        self._countdown: dict[T_key, tuple[int, Callable[[], Any]] | None] = {}
        self._execute: deque[Pending] = deque[Pending[Any]]()
        self._not_started = False
        self.log = log or logging.LoggerAdapter(logging.getLogger())

    def __enter__(self) -> Self:
        try:
            del self._not_started
        except AttributeError:
            msg = "Context re-entry is not supported!"
            raise RuntimeError(msg) from None
        self.__start()
        return self

    def __exit__(self, type, value, traceback) -> Literal[False]:
        if not self.stopped.done():
            self.execute(self.stopped.set_result, None)
        self.thread.join()
        return False

    def _wake(self) -> None:
        """Unblock the thread."""
        with self._ctrl_sock.lock:
            self._ctrl_sock.send(b"")

    def __start(self) -> None:

        def zmq_poll_thread(
            *,
            handlers: dict[T_key, Callable[[ZMQPollSocket, int], Any]] = self._handlers,
            stopped: Pending[None] = self.stopped,
            countdown: dict[T_key, tuple[int, Callable[[], Any]] | None] = self._countdown,
            execute=self._execute,
            log=self.log,
        ) -> None:
            """Runs the 'event' loop."""
            # Thread: zmq_poll_thread
            if not utils.LAUNCHED_BY_DEBUGPY:
                utils.mark_thread_pydev_do_not_trace()

            def on_wake(sock: ZMQPollSocket, flags: int) -> None:
                """On receipt of a wake event clear the sockets."""
                nonlocal sockets
                # Called on receipt of a message (b'') on the 'wake' socket.
                sockets = None
                sock.recv()

            def do_execute() -> None:
                """Execute pending items added by the `execute` and `execute_async` methods."""
                while execute:
                    md = (pen := execute.popleft()).metadata
                    try:
                        pen.set_result(md["func"](*md["args"], **md["kwargs"]))
                    except BaseException as e:
                        pen.set_exception(e)
                    del pen

            with wake.context:
                sockets = None
                c: tuple[int, Callable] | None
                wake.bind(addr := "inproc://async_kernel_zmq_poller_wake")
                send.connect(addr)
                started.wake()
                # The main loop polls the handler keys for events in a loop.
                # It will block until an event occurs.
                try:
                    while not stopped.done():
                        if not handlers:
                            handlers[(wake, zmq.POLLIN)] = on_wake
                        if not sockets:
                            sockets = list(handlers)
                        if execute:
                            do_execute()
                            continue
                        try:
                            for k in zmq_poll(sockets, timeout=-1):
                                try:
                                    handlers[k](*k)  # pyright: ignore[reportArgumentType]
                                except KeyError:
                                    sockets = None
                                except SystemExit:
                                    stopped.set_result(None)
                                except BaseException as e:
                                    self.log.exception("Ignoring exception in handler.", exc_info=e)
                                if countdown and (c := countdown.get(k)) is not None:
                                    c = countdown[k] = (int(c[0]) - 1, c[1])
                                    # Auto eject after 'n' events
                                    if c[0] == 0:
                                        handlers.pop(k, None)
                                        countdown[k] = sockets = None
                                        c[1]()
                        except zmq.ZMQError:
                            for k, v in handlers.copy().items():
                                if k[0].closed:
                                    handlers.pop(k, None)
                                    log.debug("Closed sockets detected %s -> %s", k[0], v)
                        except Exception as e:
                            self.log.exception("Ignoring exception in zmq_poll_thread.", exc_info=e)
                finally:
                    stopped.set_result(None)
                    do_execute()
                    handlers.clear()
                    log.debug("Stopped poll event loop")

        self.log.debug("Starting ZMQPoll event loop")
        started = create_green_waiter()
        self._ctrl_sock = send = self.socket(zmq.SocketType.PAIR)
        wake = self.socket(zmq.SocketType.PAIR)
        self.thread = threading.Thread(target=zmq_poll_thread)
        self.thread.start()
        started.wait()
        self.log.debug("ZMQPoll event loop started")

    @staticmethod
    def _validate_socket(sock: ZMQPollSocket | Any) -> ZMQPollSocket:
        if not isinstance(sock, ZMQPollSocket):
            msg = f"{sock=} is not valid"
            raise TypeError(msg)
        return sock

    def socket(self, socket_type: zmq.SocketType) -> ZMQPollSocket:
        """Create a new [ZMQPollSocket][].

        Args:
            socket_type: The type of socket.
        """
        if self.stopped.done():
            msg = f"{self} is stopped!"
            raise RuntimeError(msg)
        return self._validate_socket(self._zmq_context.socket(socket_type))

    def execute(self, func: Callable[P, T], /, *args: P.args, **kwargs: P.kwargs) -> T:
        """Execute `func` in the thread waiting for the result synchronously."""
        if hasattr(self, "thread"):
            if threading.current_thread() is self.thread:
                return func(*args, **kwargs)
            self._execute.append(pen := Pending[T](func=func, args=args, kwargs=kwargs))
            if not self.stopped.done():
                self._wake()
                return pen.wait_sync()
        msg = f"Unable to execute {func=} in {self}. Execution is only support while in context."
        raise RuntimeError(msg)

    async def execute_async(self, func: Callable[P, T], /, *args: P.args, **kwargs: P.kwargs) -> T:
        """Execute `func` in the thread waiting for the result asynchronously."""
        if hasattr(self, "thread"):
            self._execute.append(pen := Pending[T](func=func, args=args, kwargs=kwargs))
            if not self.stopped.done():
                self._wake()
                return await pen
        msg = f"Unable to execute {func=} in {self}. Execution is only support while in context."
        raise RuntimeError(msg)

    @contextmanager
    def event_handler(
        self,
        sock: ZMQPollSocket,
        handler: Callable[[ZMQPollSocket, int], Any],
        /,
        *,
        flags: Literal[zmq.PollEvent.POLLIN, zmq.PollEvent.POLLOUT] = zmq.PollEvent.POLLIN,
        countdown: tuple[int, Callable[[], Any]] | None = None,
    ) -> Generator[None, Any, None]:
        """A context manager where `handler` is called with the event number when it occurs for `sock`.

        Only one `handler` is allowed per `(socket, flags)` combination.

        Args:
            sock: A zmq socket or a IO style object with a `fileno`.
            handler: A handler to handle the event. The handler is called inside the
                poll thread. Thread-safe primitives must be used by the handler such
                as [async_kernel.caller.Caller.call_soon][],[async_kernel.caller.Caller.queue_call][], etc.
            flags: The type of event to listen for.
                [zmq.PollEvent.POLLIN][]: `sock` is readable.
                [zmq.PollEvent.POLLOUT][]: `sock` was read from.
            countdown: A tuple ('n', callback) where the handler is run to completion exactly 'n' times.
                The callback could be an `event.set` to release the context.

        Tip:
            The handler is called inside a dedicated thread which may have been marked using
            [async_kernel.utils.mark_thread_pydev_do_not_trace][] which disables debug breakpoints.
        """
        sock_ = self._validate_socket(sock)
        if countdown:
            assert countdown[0] > 0
            assert callable(countdown[1])
        assert not self.stopped.done()
        if handler is not self._handlers.setdefault(k := (sock_, int(flags)), handler):
            raise BusyResourceError
        self._countdown[k] = countdown
        self._wake()
        try:
            yield None
        finally:
            self._handlers.pop(k, None)
            self._countdown.pop(k, None)
            self._wake()
