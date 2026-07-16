"Defines a base kernel interface using zmq sockets."

from __future__ import annotations

import contextlib
import os
import sys
import threading
import time
from collections.abc import Callable
from contextlib import asynccontextmanager, contextmanager
from typing import TYPE_CHECKING, Any, Generic, Literal, Never, Self

import zmq
from aiologic import BinarySemaphore
from aiologic.lowlevel import AsyncEvent, create_async_event, enable_signal_safety
from jupyter_client.connect import ConnectionFileMixin
from jupyter_client.session import Session
from traitlets import traitlets
from typing_extensions import override
from zmq import Flag, PollEvent, Socket, SocketOption

from async_kernel import utils
from async_kernel.caller import Caller
from async_kernel.common import Fixed, KernelInterrupt, MethodNotSupported
from async_kernel.event_loop.zmq_poll import Poll
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
    _zmq_context: Fixed[Self, zmq.sugar.Context] = Fixed(lambda c: c["owner"]._poll._zmq_context)  # pyright: ignore[reportPrivateUsage]
    _iopub_url = "inproc://iopub-capture"
    _sockets: Fixed[Self, dict[Channel, zmq.Socket]] = Fixed(dict)

    _poll: Fixed[Self, Poll] = Fixed(lambda _: Poll(False))

    @traitlets.validate("connection_file")
    def _validate_connection_file(self, proposal: dict) -> str:

        if self._sockets and self.trait_has_value("connection_file") and proposal["value"] != self.connection_file:
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
        self.write_connection_file()
        with self._open_sockets() as ready:
            await ready
            assert len(self._sockets) == len(Channel)
            async with super().__asynccontextmanager__(set_started=False):
                self.started.add_done_callback(lambda _: self._poll.start())
                if set_started:
                    self._started()
                yield self

    @contextmanager
    def _open_sockets(self) -> Generator[AsyncEvent, Any, None]:
        ready = create_async_event()
        with (
            self._poll,
            self._heartbeat(),
            self._message_handler(Channel.shell),
            self._message_handler(Channel.control),
            self._bind_socket(Channel.stdin),
        ):
            threading.Thread(target=self._pub_proxy, name="iopub proxy", args=[ready.set]).start()
            yield ready

    @contextmanager
    def _heartbeat(self) -> Generator[None, Any, None]:
        # ref: https://jupyter-client.readthedocs.io/en/stable/messaging.html#heartbeat-for-kernels
        def heartbeat(socket: zmq.Socket, flags: int) -> None:
            msg = socket.recv_multipart()
            socket.send_multipart(msg)

        with self._bind_socket(Channel.heartbeat) as socket, self._poll.event_handler(socket, heartbeat):
            yield

    def _pub_proxy(self, ready: Callable[[], None]) -> None:
        utils.mark_thread_pydev_do_not_trace()

        def on_reg_msg(socket: Socket, flags: int) -> None:
            if frames := socket.recv_multipart():
                frame = next(iter(frames))
                if frame[0] == 1:
                    msg = self.msg("iopub_welcome", content={"subscription": frame[1:].decode()})
                    self.iopub_send(msg, parent=None)

        # We use an internal proxy to collect pub messages for distribution.
        # Each thread needs to open its own socket to publish to the internal proxy.
        # Ref: https://zguide.zeromq.org/docs/chapter2/#Working-with-Messages (fig 14)

        frontend: zmq.Socket = self._zmq_context.socket(zmq.XSUB)
        frontend.bind(Caller.iopub_url)

        # Capture broadcast messages received on both frontend and backend
        # welcome_message:  https://jupyter.org/enhancement-proposals/65-jupyter-xpub/jupyter-xpub.html#replace-pub-socket-with-xpub-socket
        capture: zmq.Socket = zmq.Context(0).socket(zmq.PUB)
        capture_: zmq.Socket = capture.context.socket(zmq.SUB)

        capture.bind(self._iopub_url)
        capture_.connect(self._iopub_url)
        # Only subscribe to the 'pub subscribe' topic byte `1` (byte `0` is 'pub unsubscribe').
        capture_.subscribe(b"\x01")

        with (
            self._bind_socket(Channel.iopub) as iopub_socket,
            capture,
            capture_,
            self._poll.event_handler(capture_, on_reg_msg),
            contextlib.suppress(zmq.ZMQError, Exception),
        ):
            ready()
            zmq.proxy(frontend, iopub_socket, capture)

    @contextlib.contextmanager
    def _bind_socket(self, channel: Channel) -> Generator[Any | Socket[Any], Any, None]:
        """
        Bind a zmq.Socket storing a reference to the socket and the port
        details and closing the socket on leaving the context.
        """
        port = int(getattr(self, f"{channel}_port"))
        assert port
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
        # bind the socket
        addr = f"tcp://{self.ip}:{port}" if self.transport == "tcp" else f"ipc://{self.ip}-{port}"
        socket.bind(addr)
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
            socket.recv_multipart(flags=Flag.DONTWAIT)
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

    @contextmanager
    def _message_handler(self, channel: Literal[Channel.control, Channel.shell]):
        """
        Opens a zmq socket for the channel, receives messages and calls the message handler.
        """
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

        def recv_msg(socket: Socket, flags: int) -> None:
            try:
                ident, msg = session.recv(socket, mode=zmq.DONTWAIT)
                msg["channel"] = channel  # pyright: ignore[reportOptionalSubscript]
                job = Job(received_time=time.monotonic(), msg=msg, ident=ident)  # pyright: ignore[reportArgumentType]
                message_handler(job, send_reply, self.iopub_send)
            except Exception as e:
                log.debug("Bad message on %s: %s", channel, e)

        with self._bind_socket(channel) as socket, self._poll.event_handler(socket, recv_msg):
            yield
