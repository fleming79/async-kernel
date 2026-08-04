"""Defines a base kernel interface using zmq sockets."""

from __future__ import annotations

import functools
import os
import sys
from typing import TYPE_CHECKING, Any, Generic, Never, Self

import jupyter_client.session
import zmq
from jupyter_client.connect import ConnectionFileMixin
from traitlets import traitlets
from typing_extensions import override

from async_kernel.common import Fixed, MethodNotSupported
from async_kernel.event_loop.zmq_poll import ZMQPoll, ZMQPollSocket
from async_kernel.interface.base import BaseInterface, HasInterface
from async_kernel.typing import Channel, Message, MsgType, T_shell_co

if TYPE_CHECKING:
    from collections.abc import Callable

    from jupyter_client import KernelConnectionInfo

    from async_kernel.client.zmq import ZMQKernelClient
    from async_kernel.pending import ProtectedPending


__all__ = ["ZMQInterface"]


class Session(HasInterface, jupyter_client.session.Session):
    check_pid = traitlets.Bool(False).tag(config=True)


class ZMQInterface(BaseInterface[T_shell_co], ConnectionFileMixin, Generic[T_shell_co]):  # pyright: ignore[reportUnsafeMultipleInheritance]
    """The base kernel interface using ZMQ sockets."""

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

    client_class: traitlets.Type[type[ZMQKernelClient[Self]], type[ZMQKernelClient[Self]] | str] = traitlets.Type(  # pyright: ignore[reportAssignmentType, reportIncompatibleVariableOverride]
        "async_kernel.client.zmq.ZMQKernelClient"
    ).tag(config=True)

    _sockets: Fixed[Self, dict[Channel, ZMQPollSocket]] = Fixed(dict)
    _iopub_socket: ZMQPollSocket | None = None

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
    async def _open_channels(self, ready: Callable[[], Any], stop: ProtectedPending, /) -> None:
        # Thread: control

        def heartbeat(hb: ZMQPollSocket, event: int) -> None:
            hb.send_multipart(hb.recv_multipart())

        def on_reg_msg(socket: ZMQPollSocket, flags: int) -> None:
            """https://jupyter-client.readthedocs.io/en/stable/messaging.html#welcome-message."""
            # Thread: zmq_poll_thread
            # handle PUB subscribe/unsubscribe messages.
            # welcome_message:  https://jupyter.org/enhancement-proposals/65-jupyter-xpub/jupyter-xpub.html#replace-pub-socket-with-xpub-socket
            msg = socket.recv()
            if msg[0] == 1:
                ident = msg[1:]
                msg = self.msg(MsgType.iopub_welcome, channel=Channel.iopub, content={"subscription": ident.decode()})
                self.iopub_send(msg, ident=ident)

        def msg_handler(
            sock, event, channel: Channel, recv=self.session.recv, handle_msg=self.handle_incoming_msg
        ) -> None:
            ident, msg = recv(sock)
            msg["channel"] = channel
            handle_msg(msg, ident)

        if os.path.exists(self.connection_file):  # noqa: PTH110
            self.load_connection_file()
        self.write_connection_file()

        with ZMQPoll() as zmq_poll:
            self._zmq_poll = zmq_poll
            with (
                self._zmq_poll.event_handler(await self._open_socket(Channel.iopub), on_reg_msg),
                zmq_poll.event_handler(await self._open_socket(Channel.heartbeat), heartbeat),
            ):
                ctrl = await self._open_socket(Channel.control)
                shell = await self._open_socket(Channel.shell)
                stdin = await self._open_socket(Channel.stdin)
                assert len(self._sockets) == 5
                ready()
                await self.started
                with (
                    zmq_poll.event_handler(ctrl, functools.partial(msg_handler, channel=Channel.control)),
                    zmq_poll.event_handler(shell, functools.partial(msg_handler, channel=Channel.shell)),
                    zmq_poll.event_handler(stdin, functools.partial(msg_handler, channel=Channel.stdin)),
                ):
                    await stop
            del ctrl, shell, self._zmq_poll

    async def _open_socket(self, channel: Channel, /):
        """Create, bind and configure a socket."""

        def open_socket():
            port = int(getattr(self, f"{channel}_port"))
            assert port
            if channel is not Channel.stdin:
                assert channel not in self._sockets

            match channel:
                case Channel.shell | Channel.control | Channel.heartbeat | Channel.stdin:
                    socket = self._zmq_poll.socket(zmq.SocketType.ROUTER)
                case Channel.iopub:
                    socket = self._zmq_poll.socket(zmq.SocketType.XPUB)
            socket.setsockopt(zmq.SocketOption.LINGER, 500)

            if self.curve_secretkey is not None:
                socket.curve_secretkey = self.curve_secretkey
                socket.curve_publickey = self.curve_publickey
                socket.curve_server = True
            # Bind the socket.
            addr = f"tcp://{self.ip}:{port}" if self.transport == "tcp" else f"ipc://{self.ip}-{port}"
            self.log.debug("%s socket on port: %i", channel, port)
            self._sockets[channel] = socket
            socket.bind(addr)
            return socket

        return await self._zmq_poll.execute_async(open_socket)

    @override
    def _send_msg(self, msg: Message, ident: bytes | list[bytes] | None = None) -> Message:
        return self.session.send(self._sockets[msg["channel"]], msg, buffers=msg.pop("buffers", None), ident=ident)  # pyright: ignore[reportReturnType, reportArgumentType]
