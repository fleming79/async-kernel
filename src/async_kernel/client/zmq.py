"""Base class to manage the interaction with a running kernel."""

# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.

import time
from collections.abc import AsyncGenerator, Awaitable, Callable, Generator
from contextlib import asynccontextmanager, contextmanager
from typing import Any, Generic, override

import anyio
import jupyter_client
import jupyter_client.session
import traitlets
import zmq
from aiologic.lowlevel import async_sleep, create_async_event, create_async_waiter
from jupyter_client.connect import ConnectionFileMixin
from traitlets.traitlets import Instance

from async_kernel.client.base import BaseKernelClient
from async_kernel.common import Fixed, SingleAsyncQueue
from async_kernel.event_loop.zmq_poll import Poll
from async_kernel.pending import Pending
from async_kernel.typing import Channel, Job, Message, MsgHeader, MsgType, T


class PendingMessage(Pending[T], Generic[T]):
    @property
    def msg_id(self) -> str:
        return self.metadata["parent"]["header"]["msg_id"]


class ClientSession(jupyter_client.session.Session):
    check_pid = traitlets.Bool(False).tag(config=True)


class ZMQKernelClient(BaseKernelClient, ConnectionFileMixin):  # pyright: ignore[reportUnsafeMultipleInheritance]
    """Communicates with a single kernel on any host via zmq channels."""

    _sockets: Fixed[Any, dict[Channel, zmq.Socket]] = Fixed(dict)

    session: Instance[ClientSession] = traitlets.Instance(ClientSession, ())
    ""

    @override
    def __del__(self) -> None:
        """Handle garbage collection."""
        super().__del__()
        if stop := getattr(self, "_stop", None):
            stop.wake()

    @override
    async def _open_channels(self, ready: Callable[[], Any], stop: Awaitable, /) -> None:
        if not self.shell_port:
            msg = "Connection info has not been set. Tip: Use `load_connection_file` or `load_connection_info`."
            raise RuntimeError(msg)
        with Poll(log=self.log) as poll:
            self.poll = poll
            with (
                self.open_socket(Channel.control) as ctrl,
                self.open_socket(Channel.shell) as shell,
                self.open_socket(Channel.stdin) as stdin,
            ):
                assert len(self._sockets) == 3
                channels = {ctrl: Channel.control, shell: Channel.shell, stdin: Channel.stdin}

                def handle_msg(sock: zmq.Socket, event: int) -> None:
                    msg: Message
                    ident: list[bytes]

                    ident, msg = self.session.recv(sock, zmq.BLOCKY)  # pyright: ignore[reportAssignmentType]
                    msg["channel"] = channels[sock]
                    if sock is shell or sock is ctrl:
                        self._handle_shell_control_msg(msg)
                    else:
                        self._handle_msg(Job(msg=msg, ident=ident, received_time=time.monotonic()))

                self._has_heartbeat = "STARTING"
                await self._wait_for_ready()
                with (
                    poll.event_handler(ctrl, handle_msg),
                    poll.event_handler(shell, handle_msg),
                    poll.event_handler(stdin, handle_msg),
                ):
                    await self._configure_session_protocol()
                    async with anyio.create_task_group() as tg:
                        tg.start_soon(self._heartbeat)
                        await stop
                        tg.cancel_scope.cancel("Shutdown")

    @contextmanager
    def open_socket(self, channel: Channel, /) -> Generator[zmq.Socket]:
        """Create, bind and configure a socket."""
        port = int(getattr(self, f"{channel}_port"))
        assert port
        if channel is not Channel.iopub:
            assert channel not in self._sockets

        match channel:
            case Channel.heartbeat:
                socket = self.poll.socket(zmq.SocketType.REQ)
                socket.identity = self.session.bsession
            case Channel.shell | Channel.control | Channel.stdin:
                socket = self.poll.socket(zmq.SocketType.DEALER)
                socket.identity = self.session.bsession
            case Channel.iopub:
                socket = self.poll.socket(zmq.SocketType.SUB)
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
            with socket:
                yield socket
        finally:
            self._sockets.pop(channel, None)
            self.log.debug("%s socket closed", channel)

    async def _heartbeat(self) -> None:
        """Ping the kernel every 1s."""
        count = 0

        def recv(sock: zmq.Socket, event: int):
            nonlocal count
            assert sock.recv() == b"ping"
            count = 0

        with self.open_socket(Channel.heartbeat) as sock, self.poll.event_handler(sock, recv):
            while True:
                count = count + 1
                try:
                    sock.send(b"ping")
                except zmq.ZMQError:
                    if self.stopping.done():
                        break
                try:
                    await async_sleep(1)
                except anyio.get_cancelled_exc_class():
                    return
                self._has_heartbeat = count < 5

    async def _configure_session_protocol(self) -> None:
        msg = await self.kernel_info()
        adapt_version = int(msg["content"]["protocol_version"].split(".")[0])
        if adapt_version != jupyter_client.protocol_version_info[0]:  # pyright: ignore[reportPrivateImportUsage]
            self.session.adapt_version = adapt_version
        self.session.adapt_version = adapt_version

    async def _wait_for_ready(self) -> None:
        with self.open_socket(Channel.iopub) as iopub:
            # Wait for iopub welcome message
            iopub.subscribe(b"")
            resume = create_async_waiter()
            with self.poll.event_handler(iopub, lambda _, __: None, countdown=(1, resume.wake)):
                await resume
            iopub.unsubscribe(b"")

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
        """Return the nested message dict.

        This format is different from what is sent over the wire. The
        serialize/deserialize methods converts this nested message dict to the wire
        format, which is a list of message parts.
        """
        msg: Message = self.session.msg(msg_type, content, parent, header, metadata)  # pyright: ignore[reportAssignmentType, reportArgumentType]
        msg["channel"] = channel
        return msg

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
            - A sync version of this function can be achieved by using poll directly.
        """

        def forward_messages(sock: zmq.Socket, event: int) -> None:
            msg: Message = self.session.recv(sock)[1]  # pyright: ignore[reportAssignmentType]
            if not ready:
                if msg["header"]["msg_type"] == MsgType.iopub_welcome:
                    ready.set()
            else:
                queue.append(msg)

        queue = SingleAsyncQueue()
        ready = create_async_event()

        with self.open_socket(Channel.iopub) as iopub, self.poll.event_handler(iopub, forward_messages):
            iopub.subscribe(topic)
            await ready
            yield queue
