from __future__ import annotations

from typing import TYPE_CHECKING

import anyio
from aiologic.lowlevel import create_async_event

from async_kernel.common import SingleAsyncQueue
from async_kernel.compat.json import pack_json_str, unpack_json
from async_kernel.interface import Interface, start_kernel_callable_interface
from async_kernel.messaging.base import LocalClient
from async_kernel.typing import Backend, BuffersType, Channel, ExecuteContent, MsgType

if TYPE_CHECKING:
    from async_kernel.typing import Message


async def test_start_kernel_callable_interface(anyio_backend: Backend):

    messages: SingleAsyncQueue[Message] = SingleAsyncQueue()
    reader = aiter(messages)
    stopped = create_async_event()

    def from_interface(packed_msg: str, buffers: BuffersType = None, ident=None) -> str | None:
        msg: Message = unpack_json(packed_msg)
        if msg["header"]["msg_type"] == MsgType.input_request:
            msg = client.msg(
                msg["header"]["msg_type"].replace("request", "reply"),
                {"value": "The value"},
                Channel.stdin,
                parent=msg,
            )
            return pack_json_str(msg)

        messages.append(msg)
        return None

    callable_interface = await start_kernel_callable_interface(
        send=from_interface, stopped=stopped.set, settings={"Interface.iopub_send_first_connection_only": True}
    )
    interface = Interface.instance()
    async with LocalClient().start() as client:
        await client.kernel_info()
        callable_interface["handle_msg"](
            pack_json_str(client.msg(MsgType.kernel_info_request, None, Channel.shell)), None
        )

        while True:
            msg = await anext(reader)
            assert msg["header"]["session"]
            if msg["header"]["msg_type"] == MsgType.kernel_info_reply:
                assert msg["content"]["status"] == "ok"
                break
        # Test input_request
        content: ExecuteContent = {
            "code": 'reply = input("input:")',
            "silent": False,
            "store_history": False,
            "user_expressions": {"reply": "reply"},
            "allow_stdin": True,
            "stop_on_error": True,
            "subshell_id": None,
        }
        msg = client.msg(MsgType.execute_request, content, Channel.shell)
        callable_interface["handle_msg"](pack_json_str(msg), None)
        while True:
            msg = await anext(reader)
            assert msg["header"]["session"]
            if msg["header"]["msg_type"] == MsgType.execute_reply:
                val = eval(msg["content"]["user_expressions"]["reply"]["data"]["text/plain"])
                assert val == "The value"
                break

        await anyio.sleep(0.02)
        interface.stop()

    await stopped
