from __future__ import annotations

from typing import TYPE_CHECKING

from aiologic.lowlevel import create_async_event

from async_kernel.common import SingleAsyncQueue
from async_kernel.compat.json import pack_json_str, unpack_json
from async_kernel.connection.base import LocalClient
from async_kernel.interface import Interface, start_kernel_callable_interface
from async_kernel.typing import Backend, Channel, MsgType

if TYPE_CHECKING:
    from async_kernel.typing import Message


async def test_start_kernel_callable_interface(anyio_backend: Backend):

    messages: SingleAsyncQueue[Message] = SingleAsyncQueue()
    stopped = create_async_event()

    def from_interface(packed_msg, ident, buffers, /) -> None:
        messages.append(unpack_json(packed_msg))

    send_to_inteface = await start_kernel_callable_interface(transmit=from_interface, stopped=stopped.set)
    interface = Interface.instance()
    async with LocalClient().start() as client:
        await client.kernel_info()
        send_to_inteface(pack_json_str(client.msg(MsgType.kernel_info_request, channel=Channel.shell)), [], None)
        async for msg in messages:
            if msg["header"]["msg_type"] == MsgType.kernel_info_reply:
                assert msg["content"]["status"] == "ok"
                break
        interface.stop()
        await stopped
        await interface.stopped
