"""Defines a kernel interface that uses functions for kernel messaging messages."""

from __future__ import annotations

import asyncio
import json
import signal
import time
from typing import TYPE_CHECKING, Any, TypedDict

import anyio
import orjson
from aiologic.lowlevel import enable_signal_safety
from IPython.core.error import StdinNotImplementedError
from typing_extensions import override

import async_kernel
from async_kernel.interface.base import BaseKernelInterface
from async_kernel.kernel import KernelInterruptError
from async_kernel.typing import Channel, Content, Job, Message, MsgHeader, MsgType, NoValue

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import FrameType

    from async_kernel.pending import Pending


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

    ORJSON_OPTION = orjson.OPT_SERIALIZE_NUMPY | orjson.OPT_NAIVE_UTC | orjson.OPT_UTC_Z
    _send: Callable[[str, list | None, bool], None | str]

    def pack(self, msg: Message, /) -> str:
        """
        Pack a message to a string.
        """
        return orjson.dumps(msg, default=repr, option=self.ORJSON_OPTION).decode()

    def unpack(self, msg_string, /) -> Message[dict[str, Any]]:
        """
        Unpack a message from a json string.
        """
        try:
            return orjson.loads(msg_string)
        except Exception:
            return json.loads(msg_string)

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
        ready: Pending[Handlers] = async_kernel.Pending()
        sig = signal.signal(signal.SIGINT, self._signal_handler)

        async def run_kernel():
            try:
                async with self.kernel:
                    ready.set_result(Handlers(handle_msg=self._handle_msg, stop=self.kernel.stop))
                    await anyio.sleep_forever()
            except Exception as e:
                del self._send
                if not ready.done():
                    ready.set_exception(e)
            finally:
                signal.signal(signal.SIGINT, sig)
                stopped()

        self._task = asyncio.create_task(run_kernel())
        return await ready

    @enable_signal_safety
    def _signal_handler(self, signum, frame: FrameType | None) -> None:
        self.last_interrupt_frame = frame
        self.interrupt()
        self.last_interrupt_frame = None
        raise KernelInterruptError

    def _send_to_frontend(
        self,
        msg: Message[dict],
        *,
        channel: Channel = Channel.shell,
        buffers: list[bytearray | bytes] | None = None,
        requires_reply=False,
    ) -> Message | None:
        msg["channel"] = channel
        reply = self._send(self.pack(msg), buffers, requires_reply)
        if requires_reply:
            assert reply
            return self.unpack(reply)
        return None

    async def _send_reply(self, job: Job, content: dict, /) -> None:
        if "status" not in content:
            content["status"] = "ok"
        msg_type = job["msg"]["header"]["msg_type"].replace("request", "reply")
        msg = self.msg(msg_type, content=content, parent=job["msg"])
        self._send_to_frontend(msg, channel=job["msg"]["channel"], buffers=content.pop("buffers", None))

    def _handle_msg(self, msg_json: str, buffers: list[bytearray] | list[bytes] | None = None, /):
        "The main message handler that gets returned by the `start` method."
        msg: Message[dict[str, Any]] = self.unpack(msg_json)
        # Copy the buffer
        msg["buffers"] = [b[:] for b in buffers] if buffers else []
        msg["channel"] = Channel(msg["channel"])
        job = Job(received_time=time.monotonic(), msg=msg, ident=b"")
        self.kernel.msg_handler(msg["channel"], MsgType(job["msg"]["header"]["msg_type"]), job, self._send_reply)  # pyright: ignore[reportArgumentType]

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
