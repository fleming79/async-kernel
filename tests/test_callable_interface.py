from __future__ import annotations

from typing import TYPE_CHECKING

from async_kernel.compat.json import pack_json_str, unpack_json
from async_kernel.interface.callable import CallableInterface
from async_kernel.typing import Channel, MsgType

if TYPE_CHECKING:
    from async_kernel.typing import Backend, Message


class TestCallableInterface:
    async def test_start_async_context(self, anyio_backend: Backend):

        messages: list[Message] = []

        def from_interface(msg_string, buffers, ident, /):
            messages.append(unpack_json(msg_string))

        async with (interface := CallableInterface()).start_async_context(send=from_interface) as send_to_interface:
            msg_json = pack_json_str(interface.client.msg(MsgType.kernel_info_request, channel=Channel.shell))
            async with interface.client.iopub_subscribe() as queue:
                send_to_interface(msg_json)
                async for msg in queue:
                    if msg["content"]["execution_state"] == "idle":
                        break
            assert messages[-2]["header"]["msg_type"] == MsgType.kernel_info_reply
