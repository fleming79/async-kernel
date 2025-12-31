from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Literal, override

import anyio
import orjson

import async_kernel
from async_kernel.interface.base import Interface
from async_kernel.typing import Content, Job, Message, MsgHeader, MsgType, NoValue, SocketID

if TYPE_CHECKING:
    from collections.abc import Callable


class CallableKernelInterface(Interface):
    """An interface that uses functions to pass messages to and from the kernel.

    Designed for usage when the platform is 'emscripten'.
    """

    ORJSON_OPTION = orjson.OPT_SERIALIZE_NUMPY | orjson.OPT_NAIVE_UTC | orjson.OPT_UTC_Z
    _sender: Callable[[str, list[bytearray | bytes] | None], None]
    _input: Callable[[str, bool], None]

    def start(  # pyright: ignore[reportImplicitOverride]
        self,
        sender: Callable[[str, list | None], None],
        input: Callable[[str, bool], None],
        kernel_settings: dict,
        /,
    ) -> async_kernel.Pending[Callable[..., None]]:
        """Start the kernel.

        Args:
            sender: A callable to send messages to the frontend. Must accept two positional arguments:
                1. A string with the message serialized in json.
                2. `None` or a list of buffers.
            input: A callable to handle input requests. Must accept two positional arguments.
                1. A string to use as the prompt.
                2. A boolean value for whether the prompt should be handled as a password.

        Returns:
            The message handler where incoming messages should be put. The 'channel' ('shell' or 'control') should be included inside the message.
        """
        assert not hasattr(self, "_sender")
        self.kernel.load_settings(kernel_settings)
        self._sender = sender
        self._input = input
        ready = async_kernel.Pending()

        async def run_kernel():
            try:
                async with self.kernel:
                    ready.set_result(self._handle_msg)
                    await anyio.sleep_forever()
            except Exception as e:
                del self._sender
                if not ready.done():
                    ready.set_exception(e)

        self._task = asyncio.create_task(run_kernel())
        return ready

    def _send_to_frontend(self, msg: Message[dict], channel: Literal["shell", "control", "iopub"]):
        msg["channel"] = channel  # pyright: ignore[reportGeneralTypeIssues]
        buffers = msg.pop("buffers", None)
        msg_string = orjson.dumps(msg, option=self.ORJSON_OPTION, default=repr).decode()
        return self._sender(msg_string, buffers or None)

    async def _send_reply(self, job: Job, content: dict, /) -> None:
        if "status" not in content:
            content["status"] = "ok"
        msg_type = job["msg"]["header"]["msg_type"].replace("request", "reply")
        msg = self.msg(msg_type, content=content, parent=job["msg"])
        self._send_to_frontend(msg, job["socket_id"].name)

    def _handle_msg(self, msg_string: str, buffers: list[bytearray] | list[bytes] | None = None, /):
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
        if parent is NoValue:
            parent = async_kernel.utils.get_parent()
        if not isinstance(msg_or_type, dict):
            msg_or_type = self.msg(msg_type=msg_or_type, content=content, parent=parent, metadata=metadata)  # pyright: ignore[reportArgumentType]
            msg_or_type["buffers"] = buffers  # pyright: ignore[reportGeneralTypeIssues]
        self._send_to_frontend(msg_or_type, "iopub")  # pyright: ignore[reportArgumentType]

    @override
    def input_request(self, prompt: str, *, password=False) -> Any:
        return self._input(prompt, password)
