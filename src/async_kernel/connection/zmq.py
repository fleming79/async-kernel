"""Defines a base kernel interface using zmq sockets."""

from __future__ import annotations

import contextlib
import functools
import json
import os
import pathlib
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

from async_kernel import Caller
from async_kernel.common import Fixed, MethodNotSupported, SingleAsyncQueue
from async_kernel.connection.base import BaseKernelClient, BaseMessage, Connection
from async_kernel.event_loop.zmq_poll import ZMQPoll, ZMQPollSocket
from async_kernel.interface import Interface
from async_kernel.kernelspec import make_argv
from async_kernel.typing import BuffersType, Channel, Message, MsgHeader, MsgType, T, T_interface_co

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable

    from anyio.abc._subprocesses import Process
    from jupyter_client import KernelConnectionInfo

    from async_kernel.pending import ProtectedPending


__all__ = ["ZMQConnection"]


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
    session = traitlets.Instance(Session, ())
    "Provides messaging utilities."

    transport: traitlets.CaselessStrEnum[str] = traitlets.CaselessStrEnum(
        ["tcp", "ipc"] if sys.platform == "linux" else ["tcp"], default_value="tcp"
    ).tag(config=True)
    "Transport for sockets."

    session_id: Fixed[Self, str] = Fixed(lambda c: c["owner"].session.session)
    ""

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
        if (f := pathlib.Path(self.connection_file)).exists():
            return f"connection_file: {f}\nInfo: {json.dumps(json.loads(f.read_bytes()), indent=2)}"
        return ""


class ZMQKernelClient(BaseKernelClient[T_interface_co], ZMQMessage, Generic[T_interface_co]):
    """Communicates with a single kernel on any host via zmq channels."""

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

    async def _wait_for_welcome(self) -> None:
        """Wait for non-local interface to publish a welcome message."""
        while True:
            resume = create_async_event()
            iopub = await self._connect_socket(Channel.iopub)
            self.log.debug("Waiting for welcome message")
            with (
                iopub,
                self.zmq_poll.event_handler(iopub, lambda _, __: None, count=(1, resume.set), canceller=None),
            ):
                # Wait for iopub welcome message
                iopub.subscribe(b"")
                if await resume.with_(timeout=2):
                    self.log.debug("Welcome message received")
                    return
                self.log.warning("Welcome message not received after 2s!")  # pragma: no cover

    async def _configure_session(self) -> None:

        self.log.debug("Getting kernel info to configure session")
        while True:
            attempt = 1
            try:
                msg = await self.kernel_info()
                adapt_version = int(msg["content"]["protocol_version"].split(".")[0])
                if adapt_version != jupyter_client.protocol_version_info[0]:  # pyright: ignore[reportPrivateImportUsage]
                    self.session.adapt_version = adapt_version  # pragma: no cover
                self.log.debug("Session config complete")
                break
            except TimeoutError:
                self.log.warning("Kernel did not respond to kernel info request. attempt %d Retrying ...", attempt)

    @override
    async def connection_task(self, started: Callable[[], Any], stop: ProtectedPending) -> None:
        def handler(sock, event, channel: Channel, recv=self.session.recv, handle_msg=self.handle_incoming_msg) -> None:
            ident, msg = recv(sock)
            msg["channel"] = channel
            handle_msg(msg, ident)

        if not self.shell_port:
            msg = "Connection info has not been set. Tip: consider using the method `subprocess_kernel` or `load_connection_info`."
            raise RuntimeError(msg)
        connect = self._connect_socket
        with (
            self.zmq_poll as zpoll,
            zpoll.event_handler(await connect(Channel.control), partial(handler, channel=Channel.control)),
            zpoll.event_handler(await connect(Channel.shell), partial(handler, channel=Channel.shell)),
            zpoll.event_handler(await connect(Channel.stdin), partial(handler, channel=Channel.stdin)),
        ):
            await self._wait_for_welcome()
            await self._configure_session()
            await super().connection_task(started, stop)
            self._sockets.clear()

    @asynccontextmanager
    async def subprocess_kernel(
        self,
        *,
        startup_delay: float = 0.5,
        start_timeout: float | None = None,
        heartbeat_interval=10.0,
        **kwargs,
    ) -> AsyncGenerator[Process]:
        """Start a kernel interface as a subprocess."""
        self.write_connection_file()
        process = None
        try:
            command = make_argv(connection_file=self.connection_file, **kwargs)
            hb_started = create_async_waiter()
            async with Caller() as caller, await anyio.open_process(command) as process, self.start():
                pen = caller.call_soon(self.monitor_heartbeat, heartbeat_interval, hb_started.wake)
                await hb_started
                try:
                    with anyio.CancelScope() as scope:
                        pen.add_done_callback(lambda _: not self.stopped.done() and scope.cancel("Lost heartbeat"))
                        yield process
                    if pen.done() and (e := pen.exception()):
                        raise e
                finally:
                    if process.returncode is None:
                        await self.shutdown(False).wait(timeout=5)
        finally:
            self.cleanup_connection_file()
            self.cleanup_ipc_files()

    async def monitor_heartbeat(self, interval: float = 10.0, started: Callable[[], Any] = lambda: None) -> None:
        """Monitor the heartbeat of the interface returning when the heartbeat is lost.

        Args:
            interval: The duration to sleep between sending requests.
            started: A callable that is called on the first successful heartbeat reply.
                It is called inside the zmq poll thread.
        """
        reply = "starting"
        noop = lambda: None  # noqa: E731

        def recv(sock: ZMQPollSocket, event: int):
            # Thread: zmq_poll
            nonlocal reply, started
            reply = sock.recv() == b"ping"
            if started is not noop:
                self.log.debug("Heartbeat monitor started (interval=%0.1fs)", interval)
                started()
                started = noop

        with await self._connect_socket(Channel.heartbeat) as hb, self.zmq_poll.event_handler(hb, recv):
            while not self.stopped.done() and reply:
                reply = ""
                hb.send(b"ping")
                with contextlib.suppress(TimeoutError):
                    await self.stopped.wait(timeout=interval)
            if not self.stopped.done():
                started()
                msg = f"Heartbeat not detected for {interval=}s!"
                raise RuntimeError(msg)

    @override
    def transmit_msg(self, msg: Message, ident: list[bytes]) -> None:
        return self.session.send(self._sockets[msg["channel"]], msg, buffers=msg.pop("buffers", None), ident=ident)  # pyright: ignore[reportReturnType, reportArgumentType]

    @asynccontextmanager
    async def iopub_subscribe(self, topic=b"") -> AsyncGenerator[SingleAsyncQueue[Message]]:
        """Open a new iopub socket and subscribe to a particular topic.

        Usaage:
        ```python
        async with client.iopub_subscribe() as queue:
            async for msg in queue:
                pass
        ```

        Tip:
            - A sync version of this async context can be achieved by using zmq_poll directly.
        """

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
            self.log.debug("waiting for welcome")
            if not await ready.with_(timeout=1):
                self.log.warning("Welcome message not received in time!")
            yield queue
