"""A collection of objects to provide a kernel interface based on callbacks."""

from __future__ import annotations

from collections import deque
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, Generic, Self

import traitlets
from traitlets.traitlets import TraitType
from typing_extensions import override

import async_kernel
from async_kernel.client.base import BaseKernelClient
from async_kernel.common import Fixed
from async_kernel.compat.json import pack_json_str
from async_kernel.interface.base import BaseInterface
from async_kernel.typing import BuffersType, Channel, Hosts, Message, T_interface_co, T_shell_co

if TYPE_CHECKING:
    from collections.abc import Callable

    from async_kernel.pending import ProtectedPending


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
    bsession: Fixed[Self, bytes] = Fixed(lambda c: c["owner"].session_id.encode())
    "Used to identfiy this object as the origin of a message."

    @asynccontextmanager
    async def start_async_context(self, *, send: Callable, stopped: Callable[[], None] | None = None):
        """Start this interface as an async context."""
        self._transmit = send
        async with self:
            try:
                yield self.handle_incoming_msg_str
            finally:
                if stopped:
                    stopped()

    async def start_async(self, *, send: Callable, stopped: Callable[[], None]):
        """Start the kernel.

        Args:
            send: The function to send messages to the client. It must accept:

                1. The message dict.
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
        return self.handle_incoming_msg_str

    @override
    def _send_msg(self, msg: Message, ident: bytes | list[bytes] | None = None) -> Message:

        buffers: BuffersType = msg.pop("buffers", None)  # pyright: ignore[reportAssignmentType]
        msg_str = pack_json_str(msg)
        ident = [self.bsession] if ident is None else ident if isinstance(ident, list) else [ident]
        channel = msg["channel"]
        self._transmit(msg_str, buffers, ident)
        for client in self.clients:
            if channel is Channel.iopub or not ident or client.bsession in ident:
                client.handle_incoming_msg_str(msg_str, ident, buffers)
        return msg


class CallableKernelClient(BaseKernelClient[T_interface_co], Generic[T_interface_co]):
    interface: Fixed[Self, CallableInterface]  # pyright: ignore[reportIncompatibleMethodOverride]
    bsession: Fixed[Self, bytes] = Fixed(lambda c: c["owner"].session_id.encode())
    "Used to identfiy this object as the origin of a message."

    @override
    async def _open_channels(self, ready: Callable[[], Any], stop: ProtectedPending, /) -> None:
        self.interface.clients.append(self)
        ready()
        await stop
        self.interface.clients.remove(self)

    @override
    def _send_msg(self, msg: Message, ident: bytes | list[bytes] | None = None) -> Message:
        ident = [self.bsession] if ident is None else ident if isinstance(ident, list) else [ident]
        self.interface.handle_incoming_msg(msg, ident)
        return msg
