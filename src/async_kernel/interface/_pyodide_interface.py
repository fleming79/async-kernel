from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING, Any, Literal, override

import anyio
import orjson

import async_kernel
from async_kernel.interface.interface import InterfaceBase
from async_kernel.typing import Content, Job, Message, MsgHeader, MsgType, NoValue, SocketID

if TYPE_CHECKING:
    from collections.abc import Callable


class PyodideInterface(InterfaceBase):
    _sender: Callable[[str], None]

    def _send_to_frontend(self, msg: Message):
        if "channel" not in msg:
            msg["channel"] = "shell"  # pyright: ignore[reportGeneralTypeIssues]
        msg_string = orjson.dumps(
            msg, option=orjson.OPT_SERIALIZE_NUMPY | orjson.OPT_NAIVE_UTC | orjson.OPT_UTC_Z, default=repr
        ).decode()
        self._sender(msg_string)

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

    def _handle_msg(self, msg_string: str):
        msg: Message = orjson.loads(msg_string)
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
            msg_or_type["buffers"] = buffers or []  # pyright: ignore[reportGeneralTypeIssues]
        msg_or_type["channel"] = "iopub"  # pyright: ignore[reportGeneralTypeIssues]
        self._send_to_frontend(msg_or_type)  # pyright: ignore[reportArgumentType]

    async def start(self, sender: Callable[[str], None]):
        """Start the kernel.

        This method is designed to be called by the webworker with pyodide.

        Args:
            sender: A callable for sending messages to the frontend.
                It should accept one argument: A string serialised message.

        Returns:
            The message handler.
        """
        self._sender = sender

        async def run_kernel():
            async with self.kernel:
                await anyio.sleep_forever()

        self._task = asyncio.create_task(run_kernel())
        await self.kernel.event_started
        return self._handle_msg
