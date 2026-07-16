"Defines a base kernel interface using zmq sockets."

from __future__ import annotations

import os
import sys
import time
from collections.abc import Generator
from contextlib import AbstractContextManager, asynccontextmanager, contextmanager
from typing import TYPE_CHECKING, Any, Generic, Literal, Never, Self

import jupyter_client.session
import wrapt
import zmq
from aiologic import BinarySemaphore
from aiologic.lowlevel import enable_signal_safety
from jupyter_client.connect import ConnectionFileMixin
from traitlets import traitlets
from typing_extensions import override
from zmq import Flag, PollEvent, SocketOption

from async_kernel import utils
from async_kernel.common import Fixed, KernelInterrupt, MethodNotSupported
from async_kernel.event_loop.zmq_poll import Poll
from async_kernel.interface.base import BaseInterface, HasInterface
from async_kernel.typing import Channel, Content, Job, Message, MsgHeader, NoValue, T_shell_co

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable, Generator

    from jupyter_client import KernelConnectionInfo
    from zmq.sugar.socket import Socket


__all__ = ["ZMQInterface"]


class Session(HasInterface, jupyter_client.session.Session):
    check_pid = traitlets.Bool(False).tag(config=True)


class SocketProxy(wrapt.BaseObjectProxy[zmq.Socket]):
    "A proxy for a socket that provides a lock for send_multipart."

    _lock = BinarySemaphore()

    def send_multipart(
        self,
        msg_parts,
        flags: int = 0,
        copy: bool = True,
        track: bool = False,
        **kwargs,
    ):
        with self._lock:
            return self.__wrapped__.send_multipart(msg_parts, flags, copy, track, **kwargs)


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

    session = traitlets.Instance(Session, ())
    "Provides messaging utilities."

    transport: traitlets.CaselessStrEnum[str] = traitlets.CaselessStrEnum(
        ["tcp", "ipc"] if sys.platform == "linux" else ["tcp"], default_value="tcp"
    ).tag(config=True)
    "Transport for sockets."

    _sockets: Fixed[Self, dict[Channel, zmq.sugar.Socket]] = Fixed(dict)
    _iopub_socket: SocketProxy | None = None

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
    async def _open_channels(self, ready: Callable[[], Any], stop: Awaitable, /) -> None:
        # Thread: control

        def heartbeat(hb: zmq.Socket, event: int) -> None:
            hb.send(hb.recv())

        if os.path.exists(self.connection_file):  # noqa: PTH110
            self.load_connection_file()
        self.write_connection_file()

        with Poll(log=self.log) as poll:
            self._poll = poll
            async with self._iopub():
                with (
                    self._open_socket(Channel.heartbeat) as hb,
                    poll.event_handler(hb, heartbeat),
                    self._open_socket(Channel.stdin),
                    self._message_handler(Channel.control, self.started) as ctrl,
                    self._message_handler(Channel.shell, self.started) as shell,
                ):
                    assert len(self._sockets) == 5
                    ready()
                    await self.started
                    with ctrl, shell:
                        await stop

    @contextmanager
    def _open_socket(self, channel: Channel, /) -> Generator[zmq.sugar.Socket]:
        """Create, bind and configure a socket."""
        port = int(getattr(self, f"{channel}_port"))
        assert port
        if channel is not Channel.stdin:
            assert channel not in self._sockets

        match channel:
            case Channel.shell | Channel.control | Channel.heartbeat | Channel.stdin:
                socket = self._poll.socket(zmq.SocketType.ROUTER)
            case Channel.iopub:
                socket = self._poll.socket(zmq.SocketType.XPUB)
        socket.setsockopt(zmq.SocketOption.LINGER, 500)

        if self.curve_secretkey is not None and self.curve_publickey is not None:
            socket.curve_secretkey = self.curve_secretkey
            socket.curve_publickey = self.curve_publickey
            socket.curve_server = True

        # Bind the socket.
        addr = f"tcp://{self.ip}:{port}" if self.transport == "tcp" else f"ipc://{self.ip}-{port}"
        socket.bind(addr)
        self.log.debug("%s socket on port: %i", channel, port)
        self._sockets[channel] = socket
        try:
            with socket:
                yield socket
        finally:
            self.log.debug("%s socket closed", channel)

    @asynccontextmanager
    async def _iopub(self) -> AsyncGenerator[None]:
        """
        Managages the iopub socket, handles connection welcome messages, and provides internal sockets so that `iopub_send` works everywhere.
        """

        def on_reg_msg(socket: Socket, flags: int) -> None:
            "https://jupyter-client.readthedocs.io/en/stable/messaging.html#welcome-message"
            # Thread: zmq_poll_thread
            # handle PUB subscribe/unsubscribe messages.
            # welcome_message:  https://jupyter.org/enhancement-proposals/65-jupyter-xpub/jupyter-xpub.html#replace-pub-socket-with-xpub-socket
            msg = socket.recv()
            if msg[0] == 1:
                ident = msg[1:]
                msg = self.msg("iopub_welcome", content={"subscription": ident.decode()})
                self.iopub_send(msg, ident=ident)

        with self._open_socket(Channel.iopub) as iopub_sock, self._poll.event_handler(iopub_sock, on_reg_msg):
            self._iopub_socket = SocketProxy(iopub_sock)
            yield
            del self._iopub_socket

    @contextmanager
    def _message_handler(
        self, channel: Literal[Channel.control, Channel.shell], start: Awaitable, /
    ) -> Generator[AbstractContextManager[None]]:
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
                    log.debug("send_reply %s %s", channel, msg)

        def recv_msg(socket: Socket, flags: int) -> None:
            # Thread: zmq_poll_thread
            try:
                ident, msg = session.recv(socket, mode=zmq.DONTWAIT)
                msg["channel"] = channel  # pyright: ignore[reportOptionalSubscript]
                job = Job(received_time=time.monotonic(), msg=msg, ident=ident)  # pyright: ignore[reportArgumentType]
                message_handler(job, send_reply, self.iopub_send)
            except Exception as e:
                log.debug("Bad message on %s: %s", channel, e)

        with self._open_socket(channel) as socket:
            yield self._poll.event_handler(socket, recv_msg)

    @override
    def input_request(self, prompt: str, *, password=False) -> Any:
        job = utils.get_job()
        if not job["msg"].get("content", {}).get("allow_stdin", False):
            msg = "Stdin is not allowed in this context!"
            raise RuntimeError(msg)
        socket = self._sockets[Channel.stdin]
        # Clear messages on the stdin socket.
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
        if (sock := self._iopub_socket) and (
            msg := self.session.send(
                stream=sock,  # pyright: ignore[reportArgumentType]
                msg_or_type=msg_or_type,  # pyright: ignore[reportArgumentType]
                content=content,
                metadata=metadata,
                parent=parent if parent is not NoValue else utils.get_parent_message(),  # pyright: ignore[reportArgumentType]
                ident=ident,
                buffers=buffers,  # pyright: ignore[reportArgumentType]
            )
        ):
            self.log.debug("iopub_send: msg_type:%r %s", msg_or_type, msg)
