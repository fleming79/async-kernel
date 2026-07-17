"Defines a background ZMQ poller thread."

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable, Generator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Literal, Self

import zmq
from aiologic import BinarySemaphore, BusyResourceError
from aiologic.lowlevel import create_green_waiter
from zmq import Socket
from zmq.backend import zmq_poll

import async_kernel
from async_kernel import utils
from async_kernel.common import Fixed
from async_kernel.pending import Pending

if TYPE_CHECKING:
    from collections.abc import Callable, Generator


__all__ = ["Poll"]

T_key = tuple[Any | Socket[Any], int]


class Poll:
    """
    Provides a [zmq_poll](https://libzmq.readthedocs.io/en/latest/zmq_poll.html) based event loop.

    Methods:
        'socket': Create a new socket using the zmq context associated with `Poll`.
        `event_handler`: A context manager to execute a handler while the context has been held.
    """

    stopped: Fixed[Any, Pending[None]] = Fixed(Pending)

    def __init__(self, *, log: logging.Logger | logging.LoggerAdapter | None = None) -> None:

        self._zmq_context = zmq.Context()
        self._handlers: dict[T_key, Callable[[zmq.sugar.Socket, int], Any]] = {}
        self._countdown: dict[T_key, tuple[int, Callable[[], Any]] | None] = {}
        self._not_started = False
        self._lock = BinarySemaphore()
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
        with self._lock:
            if not self.stopped.done():
                self._handlers.clear()
                self._send_wake()
                self.stopped.wait_sync(shield=True)
                self.thread.join()
        return False

    def socket(self, socket_type: zmq.SocketType) -> zmq.sugar.Socket:
        "Create a zmq socket."
        return self._validate_socket(self._zmq_context.socket(socket_type))

    def _send_wake(self) -> None:
        assert self._lock.value == 0
        try:
            self._ctrl_sock.send(b"")
        except zmq.ZMQError:
            time.sleep(1)
            if not self.stopped.done():
                raise

    def __start(self) -> None:

        def zmq_poll_thread(
            *,
            started: Callable[[], None],
            handlers: dict[T_key, Callable[[zmq.sugar.Socket, int], Any]] = self._handlers,
            stopped: Pending[None] = self.stopped,
            countdown: dict[T_key, tuple[int, Callable[[], Any]] | None] = self._countdown,
            context: zmq.Context = self._zmq_context,
            log=self.log,
        ) -> None:
            # Thread: zmq_poll_thread
            if not utils.LAUNCHED_BY_DEBUGPY:
                utils.mark_thread_pydev_do_not_trace()

            def do_wake(sock: zmq.sugar.Socket, flags: int) -> None:
                nonlocal sockets
                # Called on receipt of a message (b'') on the 'wake' socket.
                sockets = None
                sock.recv()

            sockets = None
            c: tuple[int, Callable] | None
            try:
                with context, wake, send:
                    handlers[(wake, zmq.POLLIN)] = do_wake
                    started()
                    # The main loop polls the handler keys for events in a loop.
                    # It will block until an event occurs.
                    while handlers:
                        if not sockets:
                            sockets = list(handlers)
                        try:
                            for k in zmq_poll(sockets, timeout=-1):
                                if not handlers:
                                    return
                                try:
                                    handlers[k](*k)  # pyright: ignore[reportArgumentType]
                                except KeyError:
                                    if k not in handlers and async_kernel.utils.LAUNCHED_BY_PYTEST:
                                        raise
                                except SystemExit:
                                    return
                                except BaseException:
                                    pass
                                if (c := countdown.get(k)) is not None:
                                    c = countdown[k] = (int(c[0]) - 1, c[1])
                                    # Auto eject after 'n' events
                                    if c[0] == 0:
                                        handlers.pop(k, None)
                                        countdown[k] = None
                                        c[1]()
                        except zmq.ZMQError:
                            for k, v in handlers.copy().items():
                                if k[0].closed:
                                    handlers.pop(k)
                                    log.warning("Closed sockets detected %s -> %s", k[0], v)
                        except Exception:
                            continue
            finally:
                stopped.set_result(None)
                log.debug("Stopped poll event loop")

        self.log.debug("Starting poll event loop")
        started = create_green_waiter()
        self._ctrl_sock = send = self.socket(zmq.SocketType.PAIR)
        wake = self.socket(zmq.SocketType.PAIR)
        wake.bind(addr := f"inproc://async_kernel_zmq_poller_{id(self)}")
        self._ctrl_sock.connect(addr)
        self.thread = threading.Thread(target=zmq_poll_thread, kwargs={"started": started.wake})
        self.thread.start()
        started.wait()
        self.log.debug("poll event loop started")

    @staticmethod
    def _validate_socket(sock: zmq.sugar.Socket) -> zmq.sugar.Socket:
        if not callable(getattr(sock, "fileno", None)):
            msg = f"{sock=} is not valid"
            raise TypeError(msg)
        return sock

    @contextmanager
    def event_handler(
        self,
        sock: zmq.sugar.Socket,
        handler: Callable[[zmq.sugar.Socket, int], Any],
        /,
        *,
        flags: Literal[zmq.PollEvent.POLLIN, zmq.PollEvent.POLLOUT] = zmq.PollEvent.POLLIN,
        countdown: tuple[int, Callable[[], Any]] | None = None,
    ) -> Generator[None, Any, None]:
        """
        A context manager where `handler` is called with the event number when it occurs for `sock`.

        Only one `handler` is allowed per `(socket, flags)` combination.

        Args:
            sock: A zmq socket or a IO style object with a `fileno`.
            handler: A handler to handle the event. The handler is always called inside a
                dedicated thread. Thread-safe primitives must be used by the handler.
            flags: The type of event to listen for.
                [zmq.PollEvent.POLLIN][]: `sock` is readable.
                [zmq.PollEvent.POLLOUT][]: `sock` was read from.
            countdown: A tuple ('n', callback) where the handler is run to completion
                exactly 'n' times. The callback is normally an `event.set` to release
                the context.

        Tip:
            The handler is called inside a dedicated thread which may have been marked using
            [async_kernel.utils.mark_thread_pydev_do_not_trace][] which disables debug breakpoints.
        """
        sock_ = self._validate_socket(sock)
        if countdown:
            assert countdown[0] > 0
            assert callable(countdown[1])
        with self._lock:
            assert not self.stopped.done()
            if handler is not self._handlers.setdefault(k := (sock_, int(flags)), handler):
                raise BusyResourceError
            self._countdown[k] = countdown
            self._send_wake()
        try:
            yield None
        finally:
            with self._lock:
                self._handlers.pop(k, None)
                self._countdown.pop(k, None)
                self._send_wake()
