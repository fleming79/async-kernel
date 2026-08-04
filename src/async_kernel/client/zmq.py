"""Base class to manage the interaction with a running kernel."""

# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import annotations

import functools
import time
from contextlib import asynccontextmanager, nullcontext
from typing import TYPE_CHECKING, Any, Generic, Self

import anyio
import jupyter_client
import jupyter_client.session
import traitlets
import zmq
from aiologic.lowlevel import create_async_event
from jupyter_client.connect import ConnectionFileMixin
from typing_extensions import override

from async_kernel import utils
from async_kernel.client.base import BaseKernelClient
from async_kernel.common import Fixed, SingleAsyncQueue
from async_kernel.event_loop.zmq_poll import ZMQPoll, ZMQPollSocket
from async_kernel.kernelspec import make_argv
from async_kernel.typing import BuffersType, Channel, Job, Message, MsgHeader, MsgType, T, T_zmq_interface_co

if TYPE_CHECKING:
    import subprocess
    from collections.abc import AsyncGenerator, Callable

    from async_kernel.pending import ProtectedPending


class ClientSession(jupyter_client.session.Session):
    check_pid = traitlets.Bool(False).tag(config=True)


class ZMQKernelClient(BaseKernelClient[T_zmq_interface_co], ConnectionFileMixin, Generic[T_zmq_interface_co]):  # pyright: ignore[reportUnsafeMultipleInheritance]
    """Communicates with a single kernel on any host via zmq channels."""

    _sockets: Fixed[Any, dict[Channel, ZMQPollSocket]] = Fixed(dict)

    session: traitlets.Instance[jupyter_client.session.Session] = traitlets.Instance(jupyter_client.session.Session, ())
    ""
    session_id: Fixed[Self, str] = Fixed(lambda c: c["owner"].session.session)

    encryption = traitlets.Enum(["curve"], default_value=None, allow_none=True)
    "The type of encryption to use."

    @override
    def write_connection_file(self, **kwargs: Any) -> None:
        if self.encryption == "curve" and not self.curve_publickey:
            self.curve_publickey, self.curve_secretkey = zmq.curve_keypair()
        if self.curve_publickey:
            self.encryption = "curve"
        return super().write_connection_file(**kwargs)

    @override
    def set_interface(self, interface: T_zmq_interface_co) -> None:  # pyright: ignore[reportGeneralTypeIssues]
        super().set_interface(interface)
        self.load_connection_info(interface.get_connection_info())
        self.connection_file = interface.connection_file
        # We  don't 'own' the connection  file so mark it written to avoid it being overwritten.
        self._connection_file_written = True

    @override
    async def _open_channels(self, ready: Callable[[], Any], stop: ProtectedPending, /) -> None:
        # Thread: control
        if self.interface:
            zmq_poll = self.interface._zmq_poll  # pyright: ignore[reportPrivateUsage]
            assert zmq_poll.thread.is_alive()
            ctx = nullcontext()
        else:
            if not self.shell_port:
                msg = "Connection info has not been set. Tip: consider using the method `subprocess_kernel`."
                raise RuntimeError(msg)
            ctx = zmq_poll = ZMQPoll()
        with ctx:
            self._zmq_poll = zmq_poll
            ctrl = await self.open_socket(Channel.control)
            shell = await self.open_socket(Channel.shell)
            stdin = await self.open_socket(Channel.stdin)
            assert len(self._sockets) == 3
            with (
                zmq_poll.event_handler(ctrl, functools.partial(self._msg_handler, channel=Channel.control)),
                zmq_poll.event_handler(shell, functools.partial(self._msg_handler, channel=Channel.shell)),
                zmq_poll.event_handler(stdin, functools.partial(self._msg_handler, channel=Channel.stdin)),
            ):
                ready()
                await stop
                self._sockets.clear()
                del self._zmq_poll

    def _msg_handler(self, sock: ZMQPollSocket, event: int, channel: Channel) -> None:
        msg: Message
        ident: list[bytes]

        ident, msg = self.session.recv(sock)  # pyright: ignore[reportAssignmentType]
        msg["channel"] = channel
        match channel:
            case Channel.control | Channel.shell:
                self._handle_reply(msg)
            # case Channel.iopub:
            #     self._handle_iopub(job)
            case Channel.stdin:
                self._handle_request(Job(msg=msg, ident=ident, received_time=time.monotonic()))
            case _:
                self.log.debug("Unhandled message")

    async def open_socket(self, channel: Channel, /) -> ZMQPollSocket:
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
                    socket = self._zmq_poll.socket(zmq.SocketType.REQ)
                    socket.identity = self.session.bsession
                case Channel.shell | Channel.control | Channel.stdin:
                    socket = self._zmq_poll.socket(zmq.SocketType.DEALER)
                    socket.identity = self.session.bsession
                case Channel.iopub:
                    socket = self._zmq_poll.socket(zmq.SocketType.SUB)
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

        return await self._zmq_poll.execute_async(open_socket)

    @asynccontextmanager
    async def subprocess_kernel(
        self, startup_delay=0.5, start_timeout=None, **kwargs
    ) -> AsyncGenerator[subprocess.Popen[bytes]]:
        import subprocess  # noqa: PLC0415

        self.write_connection_file()
        process = None
        try:
            command = make_argv(connection_file=self.connection_file, name=self.kernel_name, **kwargs)
            # We use subprocess instead of the async version for better coverage support and debugging reliability.
            process = subprocess.Popen(command)
            # Adding  a delay (especially on windows) before opening the connection gives better startup reliability.
            await anyio.sleep(startup_delay)
            async with self:
                with anyio.fail_after(start_timeout):
                    await self._wait_for_welcome()
                    await self._configure_session()
                try:
                    yield process
                finally:
                    pen = self.shutdown(False)
                    await pen.wait(shield=True, timeout=1 if not utils.LAUNCHED_BY_DEBUGPY else 1e6)
                    process.wait(timeout=1 if not utils.LAUNCHED_BY_DEBUGPY else 1e6)
        finally:
            if process:
                process.terminate()  # pragma: no cover
            self.cleanup_connection_file()
            self.cleanup_ipc_files()

    async def monitor_heartbeat(self, interval=10.0, started=lambda: None) -> None:
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
            started()
            started = noop

        with await self.open_socket(Channel.heartbeat) as hb, self._zmq_poll.event_handler(hb, recv):
            while reply:
                reply = ""
                hb.send(b"ping")
                await anyio.sleep(interval)

    async def _wait_for_welcome(self) -> None:
        """Wait for non-local interface to publish a welcome message."""
        if not self.interface:
            self.log.debug("Waiting interface to be ready")
            while True:
                resume = create_async_event()
                iopub = await self.open_socket(Channel.iopub)
                self.log.debug("Waiting for welcome message")
                with (
                    iopub,
                    self._zmq_poll.event_handler(iopub, lambda _, __: None, count=(1, resume.set), canceller=None),
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
                msg = await self.kernel_info().wait(timeout=1)
                adapt_version = int(msg["content"]["protocol_version"].split(".")[0])
                if adapt_version != jupyter_client.protocol_version_info[0]:  # pyright: ignore[reportPrivateImportUsage]
                    self.session.adapt_version = adapt_version  # pragma: no cover
                self.log.debug("Session config complete")
                break
            except TimeoutError:
                self.log.warning("Kernel did not respond to kernel info request. attempt %d Retrying ...", attempt)

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
    def _send_msg(self, msg: Message, ident: bytes | list[bytes] | None = None) -> Message:
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

        iopub = await self.open_socket(Channel.iopub)
        with iopub, self._zmq_poll.event_handler(iopub, forward_messages, canceller=canceller), scope:
            iopub.subscribe(topic)
            self.log.debug("waiting for welcome")
            if not await ready.with_(timeout=1):
                self.log.warning("Welcome message not received in time!")
            yield queue
