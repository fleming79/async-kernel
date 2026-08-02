"""A collection of objects to provide a kernel interface based on callbacks."""

from __future__ import annotations

import time
from collections import deque
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, Generic, Self
from uuid import uuid4

import traitlets
from traitlets.traitlets import TraitType
from typing_extensions import override

import async_kernel
from async_kernel.client.base import BaseKernelClient
from async_kernel.common import Fixed, SingleAsyncQueue
from async_kernel.compat.json import pack_json_str, unpack_json
from async_kernel.interface.base import BaseInterface
from async_kernel.typing import BuffersType, Channel, Hosts, Job, Message, T_interface_co, T_shell_co

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable


__all__ = ["CallableInterface"]


class CallableInterface(BaseInterface[T_shell_co], Generic[T_shell_co]):
    """A callback based interface to interact with the kernel using serialized messages.

    Usage:

        ```python
        from async_kernel.interface.callable import CallableInterface

        # Start the kernel providing the necessary callbacks.
        kernel_interface = await CallableInterface(options).start(send=..., stopped=...)

        # Pass messages to the kernel.
        kernel_interface["handle_msg"](msg, buffer)

        # Stop the kernel.
        kernel_interface["stop"](msg, buffer)
        ```
    See also:
        - [async_kernel.typing.CallableInterfaceReturnArgs]
    """

    host: TraitType[Hosts | None, Hosts | None] = TraitType(None)
    "Not yet supported"
    client_class = (  # pyright: ignore[reportAssignmentType]
        traitlets.Type("async_kernel.interface.callable.CallableKernelClient").tag(config=True)
    )
    client: Fixed[Self, CallableKernelClient[Self]]  # pyright: ignore[reportIncompatibleVariableOverride]
    clients: Fixed[Self, deque[CallableKernelClient]] = Fixed(deque)

    @asynccontextmanager
    async def start_async_context(self, *, send: Callable, stopped: Callable[[], None] | None = None):
        """Start this interface as an async context."""
        self._transmit = send
        async with self:
            try:
                yield self.receive
            finally:
                if stopped:
                    stopped()

    async def start_async(self, *, send: Callable, stopped: Callable[[], None]):
        """Start the kernel.

        Args:
            send: The function to send kernel messages to the client. It must accept

                1. A json string of the message.
                2. A list of buffers, or None if there are no buffers.
                3. A boolean value that indicates a response is required for the stdio channel.

            stopped: A callback that is called once the kernel has stopped.

        Returns: The function for sending messages to the interface/kernel.
        """
        assert not self.started.done()
        assert not self.stopping.done()
        self._transmit = send
        async_kernel.Caller().call_soon(self.run, stopped=stopped)
        await self.started
        return self.receive

    @override
    def _send_msg(
        self,
        msg: Message,
        buffers: BuffersType = None,
        ident: bytes | list[bytes] | None = None,
    ) -> Message:
        self._transmit(msg_str := pack_json_str(msg), buffers, ident)
        for client in self.clients:
            # We serialize data as a safe way to copy the message.
            client.handle_msg(unpack_json(msg_str), buffers, ident)
        return msg

    async def _send_reply(self, job: Job, content: dict, /) -> None:
        if "status" not in content:
            content["status"] = "ok"
        msg_type = job["msg"]["header"]["msg_type"].replace("request", "reply")
        msg = self.msg(msg_type, content=content, parent=job["msg"], channel=job["msg"]["channel"])
        self.send_message_no_reply(msg, content.pop("buffers", None), msg["header"]["session"].encode())

    def receive(self, msg_json: str, buffers: BuffersType = None, /):
        """This is where the fronted passes messages to the interface."""
        msg: Message[dict[str, Any]] = unpack_json(msg_json)
        # Copy the buffer
        msg["buffers"] = [b[:] for b in buffers] if buffers else []
        msg["channel"] = Channel(msg["channel"])
        match msg["channel"]:
            case Channel.shell | Channel.control:
                job = Job(received_time=time.monotonic(), msg=msg, ident=b"")
                self.kernel.message_handler(job, self._send_reply, self.iopub_send)
            case Channel.stdin:
                self._handle_reply(msg)
            case _:
                raise NotImplementedError


class CallableKernelClient(BaseKernelClient[T_interface_co], Generic[T_interface_co]):
    bsession = Fixed(lambda _: uuid4().bytes)
    interface: Fixed[Self, CallableInterface]  # pyright: ignore[reportIncompatibleMethodOverride]
    _iopub_queues: Fixed[Self, deque[tuple[bytes, SingleAsyncQueue]]] = Fixed(deque)

    @override
    async def _open_channels(self, ready: Callable[[], Any], stop: Awaitable, /) -> None:
        self.interface.clients.append(self)
        ready()
        await stop
        self.interface.clients.remove(self)

    def handle_msg(
        self,
        msg: Message,
        buffers: BuffersType = None,
        ident: bytes | list[bytes] | None = None,
    ):
        if buffers:
            msg["buffers"] = buffers
        match Channel(msg["channel"]):
            case Channel.iopub:
                ident = ident or []
                ident = [ident] if not isinstance(ident, list) else ident
                for topic, queue in self._iopub_queues:
                    if not topic or topic in ident:
                        queue.append(msg)
            case _:
                pass

    @override
    def _send_msg(
        self,
        msg: Message,
        buffers: BuffersType = None,
        ident: bytes | list[bytes] | None = None,
    ) -> Message:
        kernel = (interface := self.interface).kernel
        job = Job(received_time=time.monotonic(), msg=msg, ident=self.bsession)
        kernel.message_handler(job, self._send_reply, interface.iopub_send)
        return msg

    async def _send_reply(self, job: Job, content: dict, /) -> None:
        if "status" not in content:
            content["status"] = "ok"
        msg_type = job["msg"]["header"]["msg_type"].replace("request", "reply")
        msg = self.msg(msg_type, channel=job["msg"]["channel"], content=content, parent=job["msg"])
        self._handle_reply(msg)

    @asynccontextmanager
    async def iopub_subscribe(self, topic=b"") -> AsyncGenerator[SingleAsyncQueue[Message]]:
        """Open a new iopub socket and subscribe to a particular topic.

        Usaage:
        ```python
        async with client.iopub_subscribe() as queue:
            async for msg in queue:
                pass
        ```
        """
        queue = SingleAsyncQueue()
        self._iopub_queues.append((topic, queue))
        try:
            yield queue
        finally:
            self._iopub_queues.remove((topic, queue))
            queue.stop()
