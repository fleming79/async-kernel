"Defines a background ZMQ poller thread."

from __future__ import annotations

import threading
from collections.abc import Callable, Generator
from contextlib import contextmanager
from io import IOBase
from typing import TYPE_CHECKING, Any, Literal, Self

import zmq
from aiologic import BusyResourceError
from aiologic.lowlevel import create_thread_lock
from typing_extensions import TypeVar
from zmq import Socket
from zmq.backend import zmq_poll

from async_kernel import utils
from async_kernel.common import Fixed
from async_kernel.pending import Pending, PendingTracker

if TYPE_CHECKING:
    from collections.abc import Callable, Generator


__all__ = ["PollZMQ"]


T_socket_or_file = TypeVar("T_socket_or_file", zmq.Socket, IOBase, default=zmq.Socket)


class PollZMQ:
    """
    Lightweight background ZMQ poller thread.

    This class runs a thread that performs [zmq_poll](https://libzmq.readthedocs.io/en/latest/zmq_poll.html)
    on registered sockets and dispatches events to handlers.
    """

    _handlers: Fixed[Self, dict[tuple[Any | Socket[Any], int], Callable[..., None]]] = Fixed(dict)
    _thread: None | threading.Thread = None

    def start(self) -> Self:
        "Start the thread and begin polling registered sockets."
        assert not self.stopped
        if self._thread:
            return self

        def zmq_poll_thread(callbacks: dict, sock: zmq.Socket, started: Callable[[], Any]) -> None:

            def ctrl_msg(socket: Socket, flags: int) -> None:
                # Every time a message is sent callbacks should be rebuild.
                socket.recv()
                sockets.clear()

            callbacks[(sock, zmq.POLLIN)] = ctrl_msg
            sockets = list(callbacks)
            started()

            if not utils.LAUNCHED_BY_DEBUGPY:
                utils.mark_thread_pydev_do_not_trace()
            try:
                while callbacks:
                    try:
                        if not sockets:
                            sockets = list(callbacks)
                        for k in zmq_poll(sockets, timeout=-1):
                            try:
                                callbacks[k](*k)
                            except SystemExit:
                                return
                            except BaseException:
                                continue
                    except zmq.ZMQError as e:
                        if e.errno == zmq.Errno.ENOTSOCK:
                            # This exception occurs when the interpreter is shutting down.
                            return
                    except SystemExit:
                        return
                    except BaseException:
                        continue
            finally:
                callbacks.clear()
                sock.close(0)

        self._sock_ctrl_send: zmq.Socket = zmq.Context(0).socket(zmq.PAIR)
        sock: zmq.Socket = self._sock_ctrl_send.context.socket(zmq.PAIR)
        sock.bind(addr := f"inproc://async_kernel_zmq_poller_{id(self)}")
        self._sock_ctrl_send.connect(addr)

        self._lock = create_thread_lock()
        self._lock.acquire()
        self._thread = threading.Thread(target=zmq_poll_thread, args=[self._handlers, sock, self._lock.release])
        self._thread.start()
        self._lock.acquire()
        self._lock.release()
        return self

    def __del__(self) -> None:
        self.stop()

    def _validate_sock(self, sock: T_socket_or_file):
        if not callable(getattr(sock, "fileno", None)):
            msg = f"{sock=} is not valid"
            raise TypeError(msg)
        if self.stopped:
            msg = f"{self} is stopped or shutting down!"
            raise RuntimeError(msg)

    @property
    def stopped(self) -> bool:
        alive = not self._thread or (self._thread.is_alive() and self._handlers)
        return not alive

    def _wake_thread(self):
        if self._thread and self._handlers:
            self._lock.acquire()
            try:
                self._sock_ctrl_send.send(b"", zmq.DONTWAIT)
            except zmq.ZMQError:
                pass
            finally:
                self._lock.release()

    def stop(self) -> None:
        """Stop the poll thread."""
        if not self.stopped:
            self._handlers.clear()
            if self._thread:
                if self._thread.is_alive():
                    self._wake_thread()
                    self._thread.join(timeout=1)
                    self._sock_ctrl_send.context.destroy(0)
            else:
                self._thread = threading.Thread(target=lambda: None)
                self._thread.start()

    @contextmanager
    def event_handler(
        self,
        sock: T_socket_or_file,
        handler: Callable[[T_socket_or_file, int], Any],
        /,
        flags: Literal[zmq.PollEvent.POLLIN, zmq.PollEvent.POLLOUT] = zmq.PollEvent.POLLIN,
    ) -> Generator[T_socket_or_file, Any, None]:
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

        Tip:
            The handler is called inside a dedicated thread which may have been marked using
            [async_kernel.utils.mark_thread_pydev_do_not_trace][] which disables debug breakpoints.
        """
        self._validate_sock(sock)
        k = (sock, int(flags))
        if k in self._handlers:
            raise BusyResourceError

        assert handler is self._handlers.setdefault(k, handler)
        self._wake_thread()

        try:
            yield sock
        finally:
            self._handlers.pop(k, None)
            self._wake_thread()

    def poll(
        self,
        sock: T_socket_or_file,
        flags: Literal[zmq.PollEvent.POLLIN, zmq.PollEvent.POLLOUT] = zmq.PollEvent.POLLIN,
        *,
        trackers: type[PendingTracker] | tuple[type[PendingTracker], ...] = PendingTracker,
    ) -> Pending[int]:
        """
        Register a single use poll event for `sock`.

        Args:
            sock: The socket or file-like object to monitor.
            flags: The event flags to wait for.
            trackers: The pending tracker types to use.

        Returns:
            Pending: A pending that resolves with the event value.
        """
        self._validate_sock(sock)

        def callback(sock: zmq.Socket, flags: int) -> None:
            remove()
            pen.set_result(flags)

        def remove(msg: str | None = None):
            if self._handlers.pop(k, None):
                self._wake_thread()

        k = (sock, int(flags))
        if callback is not self._handlers.setdefault(k, callback):
            msg = f"{sock=} is in already being monitored!"
            raise BusyResourceError(msg)

        pen = Pending(None, trackers, sock=sock, flags=flags)
        pen.set_canceller(remove)
        self._wake_thread()
        return pen
