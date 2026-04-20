"""A collection of objects to provide a kernel interface based on callbacks."""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING, Any, TypedDict

from IPython.core.error import StdinNotImplementedError
from typing_extensions import override

import async_kernel
from async_kernel.compat.json import pack_json_str, unpack_json
from async_kernel.interface.base import BaseKernelInterface
from async_kernel.typing import Channel, Content, Job, Message, MsgHeader, MsgType, NoValue

if TYPE_CHECKING:
    from collections.abc import Callable


__all__ = ["CallableKernelInterface", "Handlers"]


class Handlers(TypedDict):
    "Handlers returned by [async_kernel.interface.callable.CallableKernelInterface][] when it is started."

    handle_msg: Callable[[str, list[bytes] | list[bytearray] | None]]
    """
    Handle messages from the client.
    
    The handler requires two positional arguments
        
    1. The message serialized as a JSON string. The channel ("shell" or "control" ) 
        should also be included in the Message under the key "channel". 
    2. A list of buffers if there are any, or None if there are no buffers.
    """

    stop: Callable[[], None]
    "Stop the kernel."


class CallableKernelInterface(BaseKernelInterface):
    """
    A callback based interface to interact with the kernel using serialized messages.

    Usage:

        ```python
        from async_kernel.interface.callable import CallableKernelInterface

        # Start the kernel providing the necessary callbacks.
        kernel_interface = await CallableKernelInterface(options).start(send=..., stopped=...)

        # Pass messages to the kernel.
        kernel_interface["handle_msg"](msg, buffer)

        # Stop the kernel.
        kernel_interface["stop"](msg, buffer)
        ```
    See also:
        - [async_kernel.typing.CallableKernelInterfaceReturnArgs]
    """

    _send: Callable[[str, list | None, bool], None | str]

    async def start(
        self,
        *,
        send: Callable[[str, list | None, bool], None | str],
        stopped: Callable[[], None],
    ) -> Handlers:
        """
        Start the kernel.

        Args:
            send: The function to send kernel messages to the client. It must accept

                1. A json string of the message.
                2. A list of buffers, or None if there are no buffers.
                3. A boolean value that indicates a response is required for the stdio channel.

            stopped: A callback that is called once the kernel has stopped.

        Returns: A pending that when resolved returns the message handler callback.
        """
        self._send = send
        self._task = asyncio.create_task(coro=self.run(stopped=stopped))
        await self.kernel.event_started
        return Handlers(handle_msg=self._handle_msg, stop=self.kernel.interface.stop)

    def _send_to_frontend(
        self,
        msg: Message[dict],
        *,
        channel: Channel = Channel.shell,
        buffers: list[bytearray | bytes] | None = None,
        requires_reply=False,
    ) -> Message | None:
        msg["channel"] = channel
        reply = self._send(pack_json_str(msg), buffers, requires_reply)
        if requires_reply:
            assert reply
            return unpack_json(reply)
        return None

    async def _send_reply(self, job: Job, content: dict, /) -> None:
        if "status" not in content:
            content["status"] = "ok"
        msg_type = job["msg"]["header"]["msg_type"].replace("request", "reply")
        msg = self.msg(msg_type, content=content, parent=job["msg"])
        self._send_to_frontend(msg, channel=job["msg"]["channel"], buffers=content.pop("buffers", None))

    def _handle_msg(self, msg_json: str, buffers: list[bytearray] | list[bytes] | None = None, /):
        "The main message handler that gets returned by the `start` method."
        msg: Message[dict[str, Any]] = unpack_json(msg_json)
        # Copy the buffer
        msg["buffers"] = [b[:] for b in buffers] if buffers else []
        msg["channel"] = Channel(msg["channel"])
        job = Job(received_time=time.monotonic(), msg=msg, ident=b"")
        self.message_handler(msg["channel"], MsgType(job["msg"]["header"]["msg_type"]), job, self._send_reply)  # pyright: ignore[reportArgumentType]

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
        if parent is NoValue:
            parent = async_kernel.utils.get_parent()
        if not isinstance(msg_or_type, dict):
            msg_or_type = self.msg(msg_type=msg_or_type, content=content, parent=parent, metadata=metadata)  # pyright: ignore[reportArgumentType]
        self._send_to_frontend(msg_or_type, channel="iopub", buffers=buffers)  # pyright: ignore[reportArgumentType]

    @override
    def input_request(self, prompt: str, *, password=False) -> Any:
        job = async_kernel.utils.get_job()
        if not job["msg"].get("content", {}).get("allow_stdin", False):
            msg = "Stdin is not allowed in this context!"
            raise StdinNotImplementedError(msg)
        msg = self.msg("input_request", content={"prompt": prompt, "password": password})
        reply = self._send_to_frontend(msg, channel=Channel.stdin, requires_reply=True)
        assert reply
        return reply["content"]["value"]
