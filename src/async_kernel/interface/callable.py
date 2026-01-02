from __future__ import annotations

import asyncio
import json
import signal
import time
from typing import TYPE_CHECKING, Any, Literal, TypedDict

import anyio
import orjson
from aiologic.lowlevel import enable_signal_safety
from IPython.core.error import StdinNotImplementedError
from typing_extensions import override

import async_kernel
from async_kernel.interface.base import BaseKernelInterface
from async_kernel.kernel import KernelInterruptError
from async_kernel.typing import Content, Job, Message, MsgHeader, MsgType, NoValue, SocketID

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import FrameType

    from async_kernel.pending import Pending


__all__ = ["CallableKernelInterface", "Handlers"]


class Handlers(TypedDict):
    "Handlers returned by [async_kernel.interface.callable.CallableKernelInterface][] when it is started."

    handle_msg: Callable[[str, list[bytes] | list[bytearray] | None]]
    "The handler for all incoming messages."

    stop: Callable[[], None]
    "The handler to stop the kernel."


class CallableKernelInterface(BaseKernelInterface):
    """
    A callback based interface to interact with the kernel using serialized messages.

    Usage:

        ```python
        from async_kernel.interface.callable import CallableKernelInterface

        # The kernel is provided with callbacks to send messages
        client_callbacks = {"send": ..., "stopped": ...}

        callbacks = CallableKernelInterface(options).start(client_callbacks)

        # Pass messages to the kernel
        callbacks["handle_msg"](msg, buffer)

        ...

        # Stop the kernel
        callbacks["stop"](msg, buffer)
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
            send: The callback to send messages from the kernel to the client.

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
        self, msg: Message[dict], channel: Literal["shell", "control", "iopub", "stdin"], requires_reply=False
    ) -> Message | None:
        msg["channel"] = channel  # pyright: ignore[reportGeneralTypeIssues]
        buffers = msg.pop("buffers", None)
        reply = self._send(self.pack(msg), buffers or None, requires_reply)
        if requires_reply:
            assert reply
            return self.unpack(reply)
        return None

    async def _send_reply(self, job: Job, content: dict, /) -> None:
        if "status" not in content:
            content["status"] = "ok"
        msg_type = job["msg"]["header"]["msg_type"].replace("request", "reply")
        msg = self.msg(msg_type, content=content, parent=job["msg"])
        self._send_to_frontend(msg, job["socket_id"].name)

    def _handle_msg(self, msg_json: str, buffers: list[bytearray] | list[bytes] | None = None, /):
        "The main message handler that gets returned by the `start` method."
        msg = self.unpack(msg_json)
        if buffers:
            # Copy the buffer
            msg["buffers"] = [b[:] for b in buffers]
        socket_id: Literal[SocketID.shell, SocketID.control] = SocketID(msg.get("channel", SocketID.shell))  # pyright: ignore[reportAssignmentType]
        job = Job(received_time=time.monotonic(), socket_id=socket_id, msg=msg, ident=b"")
        self.kernel.msg_handler(socket_id, MsgType(job["msg"]["header"]["msg_type"]), job, self._send_reply)

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
            msg_or_type["buffers"] = buffers  # pyright: ignore[reportGeneralTypeIssues]
        self._send_to_frontend(msg_or_type, "iopub")  # pyright: ignore[reportArgumentType]

    @override
    def input_request(self, prompt: str, *, password=False) -> Any:
        job = async_kernel.utils.get_job()
        if not job["msg"].get("content", {}).get("allow_stdin", False):
            msg = "Stdin is not allowed in this context!"
            raise StdinNotImplementedError(msg)
        msg = self.msg("input_request", content={"prompt": prompt, "password": password})
        reply = self._send_to_frontend(msg, "stdin", requires_reply=True)
        assert reply
        return reply["content"]["value"]
