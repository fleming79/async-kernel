from __future__ import annotations

import zmq

from async_kernel.client.zmq import ZMQKernelClient
from async_kernel.typing import Backend, MsgType
from tests import utils


async def test_curve_encryption(anyio_backend: Backend):

    curve_publickey, curve_secretkey = zmq.curve_keypair()
    client = ZMQKernelClient(curve_publickey=curve_publickey, curve_secretkey=curve_secretkey)
    assert client.curve_publickey
    assert client.curve_secretkey
    async with client.subprocess_kernel(backend=anyio_backend), client.iopub_subscribe() as queue:
        reader = aiter(queue)
        await client.execute("1+1")
        utils.check_pub_message(await anext(reader), msg_type=MsgType.iopub_status, execution_state="busy")
        utils.check_pub_message(await anext(reader), msg_type=MsgType.iopub_execute_input)
        utils.check_pub_message(await anext(reader), msg_type=MsgType.iopub_execute_result)
        utils.check_pub_message(await anext(reader), msg_type=MsgType.iopub_status, execution_state="idle")
