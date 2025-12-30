from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Literal, override

import anyio
import orjson
from IPython.core.error import StdinNotImplementedError

import async_kernel
from async_kernel.interface.interface import InterfaceBase
from async_kernel.typing import Content, Job, Message, MsgHeader, MsgType, NoValue, SocketID

if TYPE_CHECKING:
    from collections.abc import Callable


class CallableInterface(InterfaceBase):
    """An interface for relaying messages to the kernel.

    Designed for usage inside pyodide with jupyterlite.
    """

    _sender: Callable[[str, list[bytearray | bytes] | None, bool], None | str]  # Will return a result if blocking.

    async def start(self, sender: Callable[[str, list | None, bool], None | str]) -> Callable[..., None]:
        """Start the kernel asynchronously.

        Args:
            sender: A callable for sending messages to the frontend.
                It should accept one argument: A string serialised message.

        Returns:
            The message handler where incoming messages should be put. The 'channel' ('shell' or 'control') should be included inside the message.
        """
        assert not hasattr(self, "_sender")

        async def run_kernel():
            try:
                async with self.kernel:
                    ready.set_result(None)
                    await anyio.sleep_forever()
            except Exception as e:
                del self._sender
                if not ready.done():
                    ready.set_exception(e)

        self._sender = sender
        ready = async_kernel.Pending()
        self._task = asyncio.create_task(run_kernel())
        await ready
        return self._handle_msg

    def _send_to_frontend(self, msg: Message[dict], *, requires_reply=False):
        if "channel" not in msg:
            msg["channel"] = "shell"  # pyright: ignore[reportGeneralTypeIssues]
        buffers = msg.pop("buffers", None)
        msg_string = orjson.dumps(
            msg, option=orjson.OPT_SERIALIZE_NUMPY | orjson.OPT_NAIVE_UTC | orjson.OPT_UTC_Z, default=repr
        ).decode()
        return self._sender(msg_string, buffers or None, requires_reply)

    async def _send_reply(self, job: Job, content: dict) -> None:
        if "status" not in content:
            content["status"] = "ok"
        msg = job["msg"]
        if "channel" not in msg:
            msg["channel"] = job["socket_id"].name  # pyright: ignore[reportGeneralTypeIssues]
        self._send_to_frontend(
            self.msg(
                msg_type=job["msg"]["header"]["msg_type"].replace("request", "reply"),
                content=content,
                parent=job["msg"],
            )
        )

    def _handle_msg(self, msg_string: str, buffers: list[bytearray | bytes] | None = None):
        msg: Message = orjson.loads(msg_string)
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
        """Send an iopub message."""
        if parent is NoValue:
            parent = async_kernel.utils.get_parent()
        if not isinstance(msg_or_type, dict):
            msg_or_type = self.msg(msg_type=msg_or_type, content=content, parent=parent, metadata=metadata)  # pyright: ignore[reportArgumentType]
            msg_or_type["buffers"] = buffers  # pyright: ignore[reportGeneralTypeIssues]
        msg_or_type["channel"] = "iopub"  # pyright: ignore[reportGeneralTypeIssues]
        self._send_to_frontend(msg_or_type)  # pyright: ignore[reportArgumentType]

    @override
    def input_request(self, prompt: str, *, password=False) -> Any:
        job = async_kernel.utils.get_job()
        if not job["msg"].get("content", {}).get("allow_stdin", False):
            msg = "Stdin is not allowed in this context!"
            raise StdinNotImplementedError(msg)
        msg = self.msg(
            "input_request",
            content={"prompt": prompt, "password": password},
        )
        msg["channel"] = "stdin"  # pyright: ignore[reportGeneralTypeIssues]
        return self._send_to_frontend(msg, requires_reply=True)
