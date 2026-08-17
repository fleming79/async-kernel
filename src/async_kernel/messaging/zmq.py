"""ZMQ messaging objects using zmq sockets."""

from __future__ import annotations

import functools
import json
import os
import pathlib
import subprocess
import sys
from contextlib import asynccontextmanager
from functools import partial
from typing import TYPE_CHECKING, Any, Generic, Never, Self

import anyio
import jupyter_client.session
import zmq
from aiologic.lowlevel import create_async_event, create_async_waiter
from jupyter_client.connect import ConnectionFileMixin
from traitlets import traitlets
from traitlets.config import Config
from typing_extensions import override

from async_kernel.common import Fixed, MethodNotSupported, SingleAsyncQueue
from async_kernel.event_loop.zmq_poll import ZMQPoll, ZMQPollSocket
from async_kernel.interface import Interface
from async_kernel.kernelspec import make_argv
from async_kernel.messaging.base import BaseClient, BaseMessage, Connection
from async_kernel.typing import BuffersType, Channel, Message, MsgHeader, MsgType, T, T_interface_co

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable

    from jupyter_client import KernelConnectionInfo

    from async_kernel.pending import ProtectedPending


__all__ = ["ZMQClient", "ZMQConnection"]


class Session(jupyter_client.session.Session):
    check_pid = traitlets.Bool(False).tag(config=True)

    @property
    def config(self):  # pyright: ignore[reportImplicitOverride]
        try:
            return Interface.instance().config
        except RuntimeError:
            return Config()


Interface.classes.append(Session)


class ZMQMessage(BaseMessage, ConnectionFileMixin):  # pyright: ignore[reportUnsafeMultipleInheritance]
    """ZMQ socket based messaging.

    refs:
        - https://zeromq.org/socket-api/
        - https://zguide.zeromq.org/
    """

    session: Fixed[Self, Session] = Fixed(lambda c: Session(session=c["owner"].session_id))
    "Provides messaging utilities."

    transport: traitlets.CaselessStrEnum[str] = traitlets.CaselessStrEnum(
        ["tcp", "ipc"] if sys.platform == "linux" else ["tcp"], default_value="tcp"
    ).tag(config=True)
    "Transport for sockets."

    _sockets: Fixed[Self, dict[Channel, ZMQPollSocket]] = Fixed(dict)

    zmq_poll = Fixed(ZMQPoll)

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
    def msg(
        self,
        msg_type: str | MsgType,
        content: T | None,
        channel: Channel,
        *,
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


class ZMQConnection(ZMQMessage, Connection[T_interface_co], Generic[T_interface_co]):
    """Provides the ZMQ sockets for clients to connect and communicate with the interface."""

    @property
    @override
    def kernel_name(self) -> str:  # pyright: ignore[reportIncompatibleVariableOverride]
        return self.parent.kernel_name

    @override
    async def connection_task(self, started: Callable[[], Any], stop: ProtectedPending) -> None:

        def heartbeat_handler(hb: ZMQPollSocket, event: int) -> None:
            # Thread: zmq_poll_thread
            hb.send_multipart(hb.recv_multipart())

        def iopub_reg_handler(socket: ZMQPollSocket, flags: int) -> None:
            """https://jupyter-client.readthedocs.io/en/stable/messaging.html#welcome-message."""
            # Thread: zmq_poll_thread
            # handle PUB subscribe/unsubscribe messages.
            # welcome_message:  https://jupyter.org/enhancement-proposals/65-jupyter-xpub/jupyter-xpub.html#replace-pub-socket-with-xpub-socket
            msg = socket.recv()
            if msg[0] == 1:
                ident = msg[1:]
                # Note: The welcome message is cached until parent._started is called.
                self.parent.iopub_send(MsgType.iopub_welcome, content={"subscription": ident.decode()}, ident=ident)

        def handler(sock, event, channel: Channel, recv=self.session.recv, handle_msg=self.handle_incoming_msg) -> None:
            # Thread: zmq_poll_thread
            ident, msg = recv(sock)
            msg["channel"] = channel
            handle_msg(msg, ident)

        with self.zmq_poll as zpoll:
            await self._bind_sockets()
            with (
                zpoll.event_handler(self._sockets[Channel.control], partial(handler, channel=Channel.control)),
                zpoll.event_handler(self._sockets[Channel.shell], functools.partial(handler, channel=Channel.shell)),
                zpoll.event_handler(self._sockets[Channel.stdin], functools.partial(handler, channel=Channel.stdin)),
                zpoll.event_handler(self._sockets[Channel.heartbeat], heartbeat_handler),
                zpoll.event_handler(self._sockets[Channel.iopub], iopub_reg_handler),
            ):
                await super().connection_task(started, stop)

    async def _bind_sockets(self):
        """Create, configure and bind all sockets."""

        def bind_sockets() -> None:
            if os.path.exists(self.connection_file):  # noqa: PTH110
                self.load_connection_file()
            self.write_connection_file()

            for channel in Channel:
                port = int(getattr(self, f"{channel}_port"))
                assert port
                if channel is not Channel.stdin:
                    assert channel not in self._sockets

                match channel:
                    case Channel.shell | Channel.control | Channel.heartbeat | Channel.stdin:
                        socket = self.zmq_poll.socket(zmq.SocketType.ROUTER)
                        # ref: https://github.com/ipython/ipykernel/issues/270
                        socket.router_handover = 1
                    case Channel.iopub:
                        socket = self.zmq_poll.socket(zmq.SocketType.XPUB)
                socket.setsockopt(zmq.SocketOption.LINGER, 500)
                socket.identity = self.session.bsession
                if self.curve_secretkey is not None:
                    socket.curve_secretkey = self.curve_secretkey
                    socket.curve_publickey = self.curve_publickey
                    socket.curve_server = True
                # Bind the socket.
                addr = f"tcp://{self.ip}:{port}" if self.transport == "tcp" else f"ipc://{self.ip}-{port}"
                self.log.debug("%s socket on port: %i", channel, port)
                self._sockets[channel] = socket
                socket.bind(addr)

        await self.zmq_poll.execute_async(bind_sockets)

    @override
    def connection_info(self) -> str:
        if self.connection_file and (f := pathlib.Path(self.connection_file)).exists():
            return f"connection_file: {f}\nInfo: {json.dumps(json.loads(f.read_bytes()), indent=2)}"
        return ""


class ZMQClient(BaseClient[T_interface_co], ZMQMessage, Generic[T_interface_co]):
    """A client for an interface that provides a [ZMQConnection][].

    The client can be connected to an existing interface's using either:
    - `ZMQClient.load_connection_info` or,
    - `ZMQClient.load_connection_file`

    A new interface/kernel can be started with [ZMQClient.subprocess_kernel][].
    """

    encryption = traitlets.Enum(["curve"], default_value=None, allow_none=True)
    "The type of encryption to use."

    @override
    def write_connection_file(self, **kwargs: Any) -> None:
        if self.encryption == "curve" and not self.curve_publickey:
            self.curve_publickey, self.curve_secretkey = zmq.curve_keypair()
        if self.curve_publickey:
            self.encryption = "curve"
        return super().write_connection_file(**kwargs)

    async def _connect_socket(self, channel: Channel, /) -> ZMQPollSocket:
        """Create, configure and connect a socket."""
        port = int(getattr(self, f"{channel}_port"))
        assert port
        if channel not in [Channel.iopub, Channel.heartbeat]:
            assert channel not in self._sockets

        def open_socket() -> ZMQPollSocket:
            # Thread: zmq_poll
            port = int(getattr(self, f"{channel}_port"))
            assert port
            if channel is not Channel.iopub:
                assert channel not in self._sockets
            # Open the socket.
            match channel:
                case Channel.heartbeat:
                    socket = self.zmq_poll.socket(zmq.SocketType.REQ)
                case Channel.shell | Channel.control | Channel.stdin:
                    socket = self.zmq_poll.socket(zmq.SocketType.DEALER)
                case Channel.iopub:
                    socket = self.zmq_poll.socket(zmq.SocketType.SUB)
            socket.identity = self.session.bsession
            socket.setsockopt(zmq.SocketOption.LINGER, 500)
            # Encryption.
            if self.curve_secretkey is not None and self.curve_publickey is not None:
                socket.curve_secretkey = self.curve_secretkey
                socket.curve_publickey = self.curve_publickey
                socket.curve_serverkey = self.curve_publickey
            # Bind the socket.
            addr = f"tcp://{self.ip}:{port}" if self.transport == "tcp" else f"ipc://{self.ip}-{port}"
            socket.connect(addr)
            self.log.debug("%s socket connected to %s", channel, addr)
            if channel not in [Channel.iopub, Channel.heartbeat]:
                self._sockets[channel] = socket
            return socket

        return await self.zmq_poll.execute_async(open_socket)

    async def _establish_connection(self, timeout: float | None) -> None:
        # Wait for welcome
        async with self.iopub_subscribe(timeout=timeout):
            pass
        self.log.debug("Getting kernel info to configure session")
        # The stdin socket is used to send the request to ensure the connection is established.
        # This should help prevent input request messages from being silently discarded before
        # the socket is connected. This primarily occurs when running tests that execute code
        # requesting input. This can make the tests appear flaky for no apparent reason.
        msg = await self.send_message(self.msg(MsgType.kernel_info_request, None, Channel.stdin))
        adapt_version = int(msg["content"]["protocol_version"].split(".")[0])
        if adapt_version != jupyter_client.protocol_version_info[0]:  # pyright: ignore[reportPrivateImportUsage]
            self.session.adapt_version = adapt_version  # pragma: no cover
        self.log.debug("Session config complete")

    @override
    def start(self, *, connect_timeout: float | None = None) -> Self:
        """Connect this client to the interface.

        Args:
            connect_timeout: The maximum time to wait for the connection to reply with a welcome message and to configure the session.
                passing `connect_timeout=0` will skip the `_establish_connection` step.
        """
        if not self.shell_port:
            msg = "Connection info has not been set. Tip: consider using the method `subprocess_kernel` or `load_connection_info`."
            raise RuntimeError(msg)
        return super().start(connect_timeout=connect_timeout)

    @override
    async def connection_task(
        self, started: Callable[[], Any], stop: ProtectedPending, *, connect_timeout: float | None = None
    ) -> None:
        def handler(sock, event, channel: Channel, recv=self.session.recv, handle_msg=self.handle_incoming_msg) -> None:
            ident, msg = recv(sock)
            msg["channel"] = channel
            handle_msg(msg, ident)

        connect = self._connect_socket
        async with self.caller:
            with (
                self.zmq_poll as zpoll,
                zpoll.event_handler(await connect(Channel.control), partial(handler, channel=Channel.control)),
                zpoll.event_handler(await connect(Channel.shell), partial(handler, channel=Channel.shell)),
                zpoll.event_handler(await connect(Channel.stdin), partial(handler, channel=Channel.stdin)),
            ):
                if connect_timeout != 0:
                    await self._establish_connection(connect_timeout)
                await super().connection_task(started, stop)
                self._sockets.clear()

    @asynccontextmanager
    async def subprocess_kernel(
        self,
        *,
        connect_timeout: float | None = None,
        heartbeat_interval: float | None = 10.0,
        shutdown_timeout: float | None = 10.0,
        **kwargs,
    ) -> AsyncGenerator[subprocess.Popen]:
        """Start a kernel interface as a subprocess."""
        self.write_connection_file()
        command = make_argv(connection_file=self.connection_file, **kwargs)
        process: subprocess.Popen | None = None
        try:
            # We deliberately use subprocess directly because it is safer in pytest and debugpy.
            async with self.start(connect_timeout=0):
                process = subprocess.Popen(command)
                await self._establish_connection(connect_timeout)
                if heartbeat_interval is not None:
                    hb = self.caller.create_start_stop_task(self._monitor_heartbeat)
                    async with hb.start(interval=heartbeat_interval):
                        yield process
                else:
                    yield process
                await self.shutdown(False).wait(timeout=shutdown_timeout)
                process.wait(timeout=shutdown_timeout)
        finally:
            self.cleanup_connection_file()
            self.cleanup_ipc_files()
            if process and process.returncode is None:
                # Terminate will prevent coverage from writing the necessary files.
                process.terminate()

    async def _monitor_heartbeat(
        self, started: Callable[[], None], stop: ProtectedPending, interval: float = 10.0
    ) -> None:
        reply = "starting"

        def recv(sock: ZMQPollSocket, event: int):
            # Thread: zmq_poll
            nonlocal reply, started
            reply = sock.recv() == b"ping"

        with await self._connect_socket(Channel.heartbeat) as hb:
            ready = create_async_waiter()
            with self.zmq_poll.event_handler(hb, recv, count=(1, ready.wake)):
                hb.send(b"ping")
                await ready
                started()
            with self.zmq_poll.event_handler(hb, recv):
                while not stop.done():
                    reply = ""
                    hb.send(b"ping")
                    try:
                        await stop.wait(timeout=interval)
                    except TimeoutError:
                        if not reply:
                            msg = f"Heartbeat not detected after {interval}s!"
                            raise RuntimeError(msg) from None

    @asynccontextmanager
    @override
    async def iopub_subscribe(
        self, topic=b"", *, timeout: float | None = 1.0
    ) -> AsyncGenerator[SingleAsyncQueue[Message]]:

        def forward_messages(sock: ZMQPollSocket, event: int) -> None:
            msg: Message = self.session.recv(sock)[1]  # pyright: ignore[reportAssignmentType]
            if not ready:
                if msg["header"]["msg_type"] == MsgType.iopub_welcome:
                    ready.set()
            else:
                queue.append(msg)

        queue, ready, scope = SingleAsyncQueue(), create_async_event(), anyio.CancelScope()

        def canceller():
            scope.cancel("ZMQ poll eventloop is stopped!")  # pragma: no cover

        iopub = await self._connect_socket(Channel.iopub)
        with iopub, self.zmq_poll.event_handler(iopub, forward_messages, canceller=canceller), scope:
            iopub.subscribe(topic)
            self.log.debug("Waiting for welcome message.")
            if await ready.with_(timeout=timeout):
                self.log.debug("Welcome message received.")
            else:
                msg = f"Welcome message not received after {timeout:0.1f}s!"
                raise TimeoutError(msg)
            yield queue
