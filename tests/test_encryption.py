from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from tests import utils

if TYPE_CHECKING:
    from jupyter_client.asynchronous.client import AsyncKernelClient


@pytest.fixture(scope="module", params=["", "curve"])
def encryption(request):
    return request.param


async def test_curve_encryption(subprocess_kernels_client: AsyncKernelClient, encryption: str):
    client = subprocess_kernels_client
    await utils.clear_iopub(client)
    if encryption == "curve":
        assert client.curve_publickey
        assert client.curve_secretkey
    msg_id, reply = await utils.execute(client, "1+1", clear_pub=False)
    assert reply["status"] == "ok"
    await utils.check_pub_message(client, msg_id, execution_state="busy")
    await utils.check_pub_message(client, msg_id, msg_type="execute_input")
    await utils.check_pub_message(client, msg_id, msg_type="execute_result")
    await utils.check_pub_message(client, msg_id, execution_state="idle")
