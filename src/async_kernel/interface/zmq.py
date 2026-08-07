"""Defines a base kernel interface using zmq sockets."""

from __future__ import annotations

import functools
import json
import os
import pathlib
import sys
from typing import TYPE_CHECKING, Any, Generic, Never, Self

import jupyter_client.session
import zmq
from jupyter_client.connect import ConnectionFileMixin
from traitlets import traitlets
from typing_extensions import override

from async_kernel.common import Fixed, MethodNotSupported
from async_kernel.event_loop.zmq_poll import ZMQPoll, ZMQPollSocket
from async_kernel.interface.base import Connection, HasInterface
from async_kernel.typing import BuffersType, Channel, Message, MsgHeader, MsgType, T, T_interface_co

if TYPE_CHECKING:
    from jupyter_client import KernelConnectionInfo


__all__ = ["ZMQConnection"]


class Session(HasInterface, jupyter_client.session.Session):
    check_pid = traitlets.Bool(False).tag(config=True)


class ZMQConnection(Connection[T_interface_co], ConnectionFileMixin, Generic[T_interface_co]):  # pyright: ignore[reportUnsafeMultipleInheritance]
    """Provides the ZMQ sockets for clients to connect and communicate with the interface."""

    session = traitlets.Instance(Session, ())
    "Provides messaging utilities."

    transport: traitlets.CaselessStrEnum[str] = traitlets.CaselessStrEnum(
        ["tcp", "ipc"] if sys.platform == "linux" else ["tcp"], default_value="tcp"
    ).tag(config=True)
    "Transport for sockets."

    session_id: Fixed[Self, str] = Fixed(lambda c: c["owner"].session.session)
    ""

    _sockets: Fixed[Self, dict[Channel, ZMQPollSocket]] = Fixed(dict)
    _iopub_socket: ZMQPollSocket | None = None

    @property
    @override
    def kernel_name(self) -> str:  # pyright: ignore[reportIncompatibleVariableOverride]
        return self.parent.kernel_name

    @traitlets.validate("connection_file")
    def _validate_connection_file(self, proposal: dict) -> str:

        if self._sockets and self.trait_has_value("connection_file") and proposal["value"] != self.connection_file:
            msg = "It is too late to set the connection file!"
            raise RuntimeError(msg)
        return proposal["value"]

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
    async def open_channels(self) -> None:
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
                self.session.send(socket, msg, ident=ident)  # pyright: ignore[reportArgumentType]

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
                if not self.parent.started.done():
                    self.log.debug("Waiting until the interface is ready.")
                    await self.parent.started
                with (
                    zmq_poll.event_handler(ctrl, functools.partial(msg_handler, channel=Channel.control)),
                    zmq_poll.event_handler(shell, functools.partial(msg_handler, channel=Channel.shell)),
                    zmq_poll.event_handler(stdin, functools.partial(msg_handler, channel=Channel.stdin)),
                ):
                    await super().open_channels()
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
    def msg(
        self,
        msg_type: str | MsgType,
        content: T | None = None,
        *,
        channel: Channel,
        parent: Message | dict[str, Any] | None = None,
        header: MsgHeader | dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        buffers: BuffersType = None,
    ) -> Message[T]:
        """Create a message suitable for sending."""
        msg: Message = self.session.msg(msg_type, content, parent, header, metadata)  # pyright: ignore[reportAssignmentType, reportArgumentType]
        msg["channel"] = channel
        msg["buffers"] = buffers
        return msg

    @override
    def transmit_msg(self, msg: Message, ident: list[bytes]) -> None:
        return self.session.send(self._sockets[msg["channel"]], msg, buffers=msg.pop("buffers", None), ident=ident)  # pyright: ignore[reportReturnType, reportArgumentType]

    @override
    def connection_info(self) -> str:
        if (f := pathlib.Path(self.connection_file)).exists():
            return f"connection_file: {f}\nInfo: {json.dumps(json.loads(f.read_bytes()), indent=2)}"
        return ""
