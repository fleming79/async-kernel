from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from async_kernel.typing import MsgType
from tests import utils

if TYPE_CHECKING:
    from async_kernel.client.zmq import ZMQKernelClient


@pytest.fixture(scope="module", params=["no encryption", "curve"])
def encryption(request):
    return request.param


async def test_curve_encryption(subprocess_kernels_client: ZMQKernelClient, encryption: str):
    client = subprocess_kernels_client

    if encryption.startswith("curve"):
        assert client.curve_publickey
        assert client.curve_secretkey
    async with client.iopub_subscribe() as queue:
        reader = aiter(queue)
        await client.execute("1+1")
        utils.check_pub_message(await anext(reader), msg_type=MsgType.iopub_status, execution_state="busy")
        utils.check_pub_message(await anext(reader), msg_type=MsgType.iopub_execute_input)
        utils.check_pub_message(await anext(reader), msg_type=MsgType.iopub_execute_result)
        utils.check_pub_message(await anext(reader), msg_type=MsgType.iopub_status, execution_state="idle")
