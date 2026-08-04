from __future__ import annotations

from typing import TYPE_CHECKING

from aiologic.lowlevel import create_async_event

from async_kernel.common import SingleAsyncQueue
from async_kernel.compat.json import pack_json_str, unpack_json
from async_kernel.interface import BaseInterface, start_kernel_callable_interface
from async_kernel.typing import Backend, Channel, MsgType
from tests.utils import validate_message

if TYPE_CHECKING:
    from async_kernel.typing import Message


async def test_start_kernel_callable_interface(anyio_backend: Backend):

    messages: SingleAsyncQueue[Message] = SingleAsyncQueue()
    stopped = create_async_event()

    def from_interface(packed_msg, ident, buffers, /) -> None:
        messages.append(unpack_json(packed_msg))

    send_to_inteface = await start_kernel_callable_interface(transmit=from_interface, stopped=stopped.set)
    interface = BaseInterface.instance()
    msg = interface.msg(MsgType.kernel_info_request, channel=Channel.shell)
    send_to_inteface(pack_json_str(msg), [b"123"], None)
    async for msg in messages:
        if msg["header"]["msg_type"] == MsgType.kernel_info_reply:
            validate_message(msg, msg_type=MsgType.kernel_info_reply)
            break
    interface.stop()
    await stopped
    await interface.stopped
