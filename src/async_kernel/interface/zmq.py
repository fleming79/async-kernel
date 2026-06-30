"Defines a base kernel interface using zmq sockets."

from __future__ import annotations

import contextlib
import os
import pathlib
import sys
import threading
import time
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, Generic, Literal, Never, Self

import zmq
from aiologic import BinarySemaphore, CountdownEvent
from aiologic.lowlevel import enable_signal_safety
from jupyter_client.connect import ConnectionFileMixin
from jupyter_client.session import Session
from traitlets import traitlets
from typing_extensions import override
from zmq import Flag, PollEvent, Socket, SocketOption

from async_kernel import utils
from async_kernel.caller import Caller
from async_kernel.common import Fixed, KernelInterrupt, MethodNotSupported
from async_kernel.interface.base import BaseInterface, HasInterface
from async_kernel.typing import Channel, Content, Job, Message, MsgHeader, NoValue, T_shell_co

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable, Generator

    from jupyter_client import KernelConnectionInfo


__all__ = ["ZMQInterface"]


class AsyncSession(HasInterface, Session):
    check_pid = traitlets.Bool(False).tag(config=True)


class ZMQInterface(BaseInterface[T_shell_co], ConnectionFileMixin, Generic[T_shell_co]):  # pyright: ignore[reportUnsafeMultipleInheritance]
    "The base kernel interface using ZMQ sockets."

    aliases = BaseInterface.aliases | {
        ("f", "connection_file"): "ZMQInterface.connection_file",
        "host": "ZMQInterface.host",
        "host_options": "ZMQInterface.host_options",
        "backend_options": "ZMQInterface.backend_options",
        "backend": "ZMQInterface.backend",
        "ip": "ZMQInterface.ip",
        "hb": "ZMQInterface.hb_port",
        "shell": "ZMQInterface.shell_port",
        "iopub": "ZMQInterface.iopub_port",
        "stdin": "ZMQInterface.stdin_port",
        "control": "ZMQInterface.control_port",
        "transport": "ZMQInterface.transport",
    }
    ""

    session = traitlets.Instance(AsyncSession, ())
    ""

    transport: traitlets.CaselessStrEnum[str] = traitlets.CaselessStrEnum(
        ["tcp", "ipc"] if sys.platform == "linux" else ["tcp"], default_value="tcp"
    ).tag(config=True)
    "Transport for sockets."

    _initialized = False
    _zmq_context = Fixed(zmq.Context)
    _iopub_url = "inproc://iopub-capture"
    _sockets: Fixed[Self, dict[Channel, zmq.Socket]] = Fixed(dict)

    @traitlets.validate("connection_file")
    def _validate_connection_file(self, proposal: dict) -> str:

        if self._sockets and self.connection_file and proposal["value"] != self.connection_file:
            msg = "It is too late to set the connection file!"
            raise RuntimeError(msg)
        return proposal["value"]

    @property
    @override
    def summary(self) -> str:
        return f"{super().summary} connection_file={str(self.connection_file)!r}"

    @override
    def load_connection_info(self, info: KernelConnectionInfo) -> None:
        if self._sockets:
            msg = "It is too late to configure!"
            raise RuntimeError(msg)
        super().load_connection_info(info)

    @override
    def blocking_client(self) -> Never:
        raise MethodNotSupported  # pragma: no cover

    @override
    def connect_control(self, identity: bytes | None = None) -> Never:
        raise MethodNotSupported  # pragma: no cover

    @override
    def connect_hb(self, identity: bytes | None = None) -> Never:
        raise MethodNotSupported  # pragma: no cover

    @override
    def connect_iopub(self, identity: bytes | None = None) -> Never:
        raise MethodNotSupported  # pragma: no cover

    @override
    def connect_shell(self, identity: bytes | None = None) -> Never:
        raise MethodNotSupported  # pragma: no cover

    @override
    def connect_stdin(self, identity: bytes | None = None) -> Never:
        raise MethodNotSupported  # pragma: no cover

    @override
    @asynccontextmanager
    async def __asynccontextmanager__(self, *, set_started=True) -> AsyncGenerator[Self]:

        if os.path.exists(self.connection_file):  # noqa: PTH110
            self.load_connection_file()
        try:
            async with super().__asynccontextmanager__(set_started=False):
                self._start_hb_iopub_shell_control_threads()
                with self._bind_socket(Channel.stdin):
                    assert len(self._sockets) == len(Channel)
                    self.write_connection_file()
                    if set_started:
                        self._started()
                    yield self
        finally:
            self._zmq_context.term()

    def _start_hb_iopub_shell_control_threads(self) -> None:
        def heartbeat(ready: Callable[[], None]) -> None:
            # ref: https://jupyter-client.readthedocs.io/en/stable/messaging.html#heartbeat-for-kernels
            utils.mark_thread_pydev_do_not_trace()
            with self._bind_socket(Channel.heartbeat) as socket:
                ready()
                self.started.wait_sync()
                try:
                    zmq.proxy(socket, socket)
                except zmq.ContextTerminated:
                    return

        def pub_proxy(ready: Callable[[], None]) -> None:
            utils.mark_thread_pydev_do_not_trace()

            # We use an internal proxy to collect pub messages for distribution.
            # Each thread needs to open its own socket to publish to the internal proxy.
            # Ref: https://zguide.zeromq.org/docs/chapter2/#Working-with-Messages (fig 14)

            frontend: zmq.Socket = self._zmq_context.socket(zmq.XSUB)
            frontend.bind(Caller.iopub_url)

            # Capture broadcast messages received on both frontend and backend
            capture = self._zmq_context.socket(zmq.PUB)
            capture.bind(self._iopub_url)

            with self._bind_socket(Channel.iopub) as iopub_socket:
                ready()
                try:
                    zmq.proxy(frontend, iopub_socket, capture)
                except (zmq.ContextTerminated, Exception):
                    pass
            frontend.close(linger=50)
            capture.close(linger=50)

        ready = CountdownEvent(5)

        threading.Thread(target=heartbeat, name="heartbeat", args=[ready.down]).start()
        threading.Thread(target=pub_proxy, name="iopub proxy", args=[ready.down]).start()
        threading.Thread(target=self._pub_capture, args=[ready.down]).start()
        # message loops
        for channel in [Channel.shell, Channel.control]:
            name = f"{channel}-receive_msg_loop"
            threading.Thread(target=self.receive_msg_loop, name=name, args=(channel, ready.down)).start()
        ready.wait()

    def _pub_capture(self, ready: Callable[[], None]) -> None:
        """
        Capture connection messages on iopub.

        Will send an 'iopub_welcome' whenever a socket subscribes to the iopub socket [ref](https://jupyter-client.readthedocs.io/en/stable/messaging.html#welcome-message).
        """

        utils.mark_thread_pydev_do_not_trace()

        socket: zmq.Socket = self._zmq_context.socket(zmq.SUB)
        socket.linger = 0
        socket.connect(self._iopub_url)
        # welcome_message:  https://jupyter.org/enhancement-proposals/65-jupyter-xpub/jupyter-xpub.html#replace-pub-socket-with-xpub-socket
        # Only subscribe to the 'pub subscribe' topic byte `1` (byte `0` is 'pub unsubscribe').
        socket.subscribe(b"\x01")
        with socket:
            ready()
            self.started.wait_sync()
            while True:
                try:
                    if frames := socket.recv_multipart():
                        frame = next(iter(frames))
                        if frame[0] == 1:
                            msg = self.msg("iopub_welcome", content={"subscription": frame[1:].decode()})
                            self.iopub_send(msg, parent=None)
                except zmq.ContextTerminated:
                    break
                except Exception:
                    continue

    @contextlib.contextmanager
    def _bind_socket(self, channel: Channel) -> Generator[Any | Socket[Any], Any, None]:
        """
        Bind a zmq.Socket storing a reference to the socket and the port
        details and closing the socket on leaving the context.
        """
        assert channel not in self._sockets

        match channel:
            case Channel.shell | Channel.control | Channel.heartbeat | Channel.stdin:
                socket_type = zmq.ROUTER
            case Channel.iopub:
                socket_type = zmq.XPUB
        socket: zmq.Socket = self._zmq_context.socket(socket_type)
        socket.linger = 1000
        if self.curve_secretkey is not None and self.curve_publickey is not None:
            socket.curve_secretkey = self.curve_secretkey
            socket.curve_publickey = self.curve_publickey
            socket.curve_server = True
        # Bind the socket
        if port := int(getattr(self, f"{channel}_port")):
            addr = f"tcp://{self.ip}:{port}" if self.transport == "tcp" else f"ipc://{self.ip}-{port}"
            socket.bind(addr)
        else:
            # Bind to a new port
            while True:
                if self.transport == "tcp":
                    port = socket.bind_to_random_port(f"tcp://{self.ip}")
                    break
                else:
                    n, offset = 0, list(Channel).index(channel)
                    n = n + 10
                    port = n + offset
                    if not pathlib.Path(f"{self.ip}-{port}").exists():
                        try:
                            socket.bind(f"ipc://{self.ip}-{port}")
                            break
                        except zmq.ZMQError:
                            pass
            assert port
            setattr(self, f"{channel}_port", port)

        self.log.debug("%s socket on port: %i", channel, port)
        self._sockets[channel] = socket
        try:
            yield socket
        finally:
            socket.close(linger=50)
            self._sockets.pop(channel)

    @override
    def input_request(self, prompt: str, *, password=False) -> Any:
        job = utils.get_job()
        if not job["msg"].get("content", {}).get("allow_stdin", False):
            msg = "Stdin is not allowed in this context!"
            raise RuntimeError(msg)
        socket = self._sockets[Channel.stdin]
        # Clear messages on the stdin socket
        while socket.get(SocketOption.EVENTS) & PollEvent.POLLIN:  # pyright: ignore[reportOperatorIssue]
            socket.recv_multipart(flags=Flag.DONTWAIT, copy=False)
        # Send the input request.
        assert self is not None
        self.session.send(
            stream=socket,
            msg_or_type="input_request",
            content={"prompt": prompt, "password": password},
            parent=job["msg"],  # pyright: ignore[reportArgumentType]
            ident=job["ident"],
        )
        # Poll for a reply.
        while not (socket.poll(100) & PollEvent.POLLIN):
            if pen := self.kernel._interrupt_requested:  # pyright: ignore[reportPrivateUsage]
                with enable_signal_safety():
                    pen.set_result(None)
                raise KernelInterrupt
        return self.session.recv(socket)[1]["content"]["value"]  # pyright: ignore[reportOptionalSubscript]

    @override
    def iopub_send(
        self,
        msg_or_type: Message[dict[str, Any]] | dict[str, Any] | str,
        *,
        content: Content | None = None,
        metadata: dict[str, Any] | None = None,
        parent: dict[str, Any] | MsgHeader | None | NoValue = NoValue,  # pyright: ignore[reportInvalidTypeForm]
        ident: bytes | list[bytes] | None = None,
        buffers: list[bytes] | None = None,
    ) -> None:
        """
        Send a message on the zmq iopub socket.
        """
        if socket := Caller.iopub_sockets.get(t_ident := Caller.id_current()):
            msg = self.session.send(
                stream=socket,
                msg_or_type=msg_or_type,  # pyright: ignore[reportArgumentType]
                content=content,
                metadata=metadata,
                parent=parent if parent is not NoValue else utils.get_parent_message(),  # pyright: ignore[reportArgumentType]
                ident=ident,
                buffers=buffers,  # pyright: ignore[reportArgumentType]
            )
            if msg:
                self.log.debug("iopub_send: msg_type:'%s', content: %s", msg["header"]["msg_type"], msg["content"])
        elif (caller := self.callers.get(Channel.control)) and caller.id != t_ident:
            caller.call_direct(
                self.iopub_send,
                msg_or_type=msg_or_type,
                content=content,
                metadata=metadata,
                parent=parent if parent is not NoValue else None,
                ident=ident,
                buffers=buffers,
            )

    def receive_msg_loop(self, channel: Literal[Channel.control, Channel.shell], ready: Callable[[], None]) -> None:
        """
        Opens a zmq socket for the channel, receives messages and calls the message handler.
        """
        if not utils.LAUNCHED_BY_DEBUGPY:
            utils.mark_thread_pydev_do_not_trace()

        session, log, message_handler = self.session, self.log, self.kernel.message_handler
        lock = BinarySemaphore()

        async def send_reply(job: Job, content: dict, /) -> None:
            if "status" not in content:
                content["status"] = "ok"
            async with lock:
                msg = session.send(
                    stream=socket,
                    msg_or_type=job["msg"]["header"]["msg_type"].replace("request", "reply"),
                    content=content,
                    parent=job["msg"],  # pyright: ignore[reportArgumentType]
                    ident=job["ident"],
                    buffers=content.pop("buffers", None),
                )
                if msg:
                    log.debug("***send_reply %s*** %s", channel, msg)

        with self._bind_socket(channel) as socket:
            ready()
            self.started.wait_sync()

            while True:
                try:
                    ident, msg = session.recv(socket, mode=zmq.BLOCKY, copy=False)
                    msg["channel"] = channel  # pyright: ignore[reportOptionalSubscript]
                    job = Job(received_time=time.monotonic(), msg=msg, ident=ident)  # pyright: ignore[reportArgumentType]
                    message_handler(job, send_reply, self.iopub_send)
                except zmq.ContextTerminated:
                    break
                except Exception as e:
                    log.debug("Bad message on %s: %s", channel, e)
                    continue
