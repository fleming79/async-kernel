"""Base class to manage the interaction with a running kernel."""

# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import annotations

import time
from collections.abc import Generator
from contextlib import asynccontextmanager, contextmanager, nullcontext
from typing import TYPE_CHECKING, Any, Generic

import anyio
import jupyter_client
import jupyter_client.session
import traitlets
import zmq
from aiologic.lowlevel import async_sleep, create_async_event, create_async_waiter
from jupyter_client.connect import ConnectionFileMixin
from typing_extensions import override

from async_kernel.client.base import BaseKernelClient
from async_kernel.common import Fixed, SingleAsyncQueue
from async_kernel.event_loop.zmq_poll import ZMQPoll, ZMQPollSocket
from async_kernel.kernelspec import make_argv
from async_kernel.typing import Channel, Job, Message, MsgHeader, MsgType, T, T_zmq_interface_co

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable, Generator

    from anyio.abc._subprocesses import Process


class ClientSession(jupyter_client.session.Session):
    check_pid = traitlets.Bool(False).tag(config=True)


class ZMQKernelClient(BaseKernelClient[T_zmq_interface_co], ConnectionFileMixin, Generic[T_zmq_interface_co]):  # pyright: ignore[reportUnsafeMultipleInheritance]
    """Communicates with a single kernel on any host via zmq channels."""

    _sockets: Fixed[Any, dict[Channel, ZMQPollSocket]] = Fixed(dict)

    session: traitlets.Instance[jupyter_client.session.Session] = traitlets.Instance(jupyter_client.session.Session, ())
    ""

    @override
    def set_interface(self, interface: T_zmq_interface_co) -> None:  # pyright: ignore[reportGeneralTypeIssues]
        super().set_interface(interface)
        self.session = interface.session
        self.load_connection_info(interface.get_connection_info())
        self.connection_file = interface.connection_file
        # We  don't 'own' the connection  file so mark it written to avoid it being overwritten.
        self._connection_file_written = True

    @override
    async def _open_channels(self, ready: Callable[[], Any], stop: Awaitable, /) -> None:
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
            with (
                self.open_socket(Channel.control) as ctrl,
                self.open_socket(Channel.shell) as shell,
                self.open_socket(Channel.stdin) as stdin,
            ):
                assert len(self._sockets) == 3
                channels = {ctrl: Channel.control, shell: Channel.shell, stdin: Channel.stdin}

                def handle_msg(sock: ZMQPollSocket, event: int) -> None:
                    msg: Message
                    ident: list[bytes]

                    ident, msg = self.session.recv(sock, zmq.BLOCKY)  # pyright: ignore[reportAssignmentType]
                    msg["channel"] = channels[sock]
                    if sock is shell or sock is ctrl:
                        self._handle_shell_control_msg(msg)
                    else:
                        self._handle_msg(Job(msg=msg, ident=ident, received_time=time.monotonic()))

                with (
                    zmq_poll.event_handler(ctrl, handle_msg),
                    zmq_poll.event_handler(shell, handle_msg),
                    zmq_poll.event_handler(stdin, handle_msg),
                ):
                    # Only check for heartbeat for a non-local interface.
                    # pen = None if interface else self.callers[Channel.control].call_soon(self._heartbeat)
                    ready()
                    await stop
                    # if pen:
                    #     await pen.cancel_wait()

    @contextmanager
    def open_socket(self, channel: Channel, /) -> Generator[ZMQPollSocket]:
        """Create, bind and configure a socket."""
        port = int(getattr(self, f"{channel}_port"))
        assert port
        if channel is not Channel.iopub:
            assert channel not in self._sockets

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

        if self.curve_secretkey is not None and self.curve_publickey is not None:
            socket.curve_secretkey = self.curve_secretkey
            socket.curve_publickey = self.curve_publickey
            socket.curve_serverkey = self.curve_publickey

        # Bind the socket.
        addr = f"tcp://{self.ip}:{port}" if self.transport == "tcp" else f"ipc://{self.ip}-{port}"
        socket.connect(addr)
        self.log.debug("%s socket on port: %i", channel, port)
        if channel is not Channel.iopub:
            self._sockets[channel] = socket
        try:
            yield socket
        finally:
            # Temp fix for socket not closing properly which prevents interpreter shutdown.
            self.callers[Channel.control].call_direct(socket.close)
            self._sockets.pop(channel, None)
            self.log.debug("%s socket closed", channel)

    async def _heartbeat(self) -> None:
        """Ping the kernel every 1s."""
        count = 0

        def recv(sock: ZMQPollSocket, event: int):
            nonlocal count
            assert sock.recv() == b"ping"
            count = 0

        with self.open_socket(Channel.heartbeat) as sock, self._zmq_poll.event_handler(sock, recv):
            while True:
                count = count + 1
                sock.send(b"ping")
                await async_sleep(1)
                self._has_heartbeat = count < 5

    @asynccontextmanager
    async def subprocess_kernel(self, **kwargs) -> AsyncGenerator[Process]:
        self.write_connection_file()
        try:
            command = make_argv(connection_file=self.connection_file, name=self.kernel_name, **kwargs)
            async with await anyio.open_process(command) as process, self:
                await self._wait_ready()
                await self._configure_session()
                try:
                    yield process
                finally:
                    await self.shutdown(False)
        finally:
            self.cleanup_connection_file()
            self.cleanup_ipc_files()

    async def _wait_ready(self) -> None:
        """Wait for non-local interface to publish a welcome message."""
        if not self.interface:
            resume = create_async_waiter()
            with (
                self.open_socket(Channel.iopub) as iopub,
                self._zmq_poll.event_handler(iopub, lambda _, __: None, count=(1, resume.wake), canceller=None),
            ):
                # Wait for iopub welcome message
                iopub.subscribe(b"")
                await resume

    async def _configure_session(self) -> None:
        msg = await self.kernel_info()
        adapt_version = int(msg["content"]["protocol_version"].split(".")[0])
        if adapt_version != jupyter_client.protocol_version_info[0]:  # pyright: ignore[reportPrivateImportUsage]
            self.session.adapt_version = adapt_version

    @override
    def msg(
        self,
        msg_type: str | MsgType,
        content: T | None = None,
        *,
        parent: Message | dict[str, Any] | None = None,
        header: MsgHeader | dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        channel: Channel = Channel.shell,
    ) -> Message[T]:
        """Create a message suitable for sending."""
        msg: Message = self.session.msg(msg_type, content, parent, header, metadata)  # pyright: ignore[reportAssignmentType, reportArgumentType]
        msg["channel"] = channel
        return msg

    @override
    def _send_msg(self, msg: Message) -> Message:
        return self.session.send(self._sockets[msg["channel"]], msg)  # pyright: ignore[reportReturnType, reportArgumentType]

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
        assert self._has_heartbeat

        def forward_messages(sock: ZMQPollSocket, event: int) -> None:
            msg: Message = self.session.recv(sock)[1]  # pyright: ignore[reportAssignmentType]
            if not ready:
                if msg["header"]["msg_type"] == MsgType.iopub_welcome:
                    ready.set()
            else:
                queue.append(msg)

        queue, ready, scope = SingleAsyncQueue(), create_async_event(), anyio.CancelScope()

        def canceller():
            scope.cancel("ZMQ poll eventloop is stopped!")

        with (
            self.open_socket(Channel.iopub) as iopub,
            self._zmq_poll.event_handler(iopub, forward_messages, canceller=canceller),
        ):
            try:
                with scope:
                    iopub.subscribe(topic)
                    await ready
                    yield queue
            finally:
                iopub.unsubscribe(b"")
