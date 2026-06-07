from __future__ import annotations

from typing import TYPE_CHECKING

import anyio
import pytest
import zmq
from jupyter_client import connect
from jupyter_client.asynchronous.client import AsyncKernelClient

from async_kernel.interface.zmq import ZMQInterface
from async_kernel.typing import Channel
from tests import utils

if TYPE_CHECKING:
    import pathlib

    from async_kernel import Kernel


# pyright: reportPrivateUsage=false


@pytest.fixture(scope="module")
async def curve_encrypted_kernel(anyio_backend, tmp_path_factory):
    connection_file: pathlib.Path = tmp_path_factory.mktemp("async_kernel") / "temp_connection.json"
    curve_publickey, curve_secretkey = zmq.curve_keypair()
    connect.write_connection_file(
        str(connection_file),
        curve_publickey=curve_publickey,
        curve_secretkey=curve_secretkey,
    )
    interface = ZMQInterface(connection_file=connection_file)
    async with interface:
        yield interface.kernel


@pytest.fixture(scope="module")
async def curve_encrypted_client(curve_encrypted_kernel: Kernel):

    assert isinstance(curve_encrypted_kernel.parent, ZMQInterface)
    client = AsyncKernelClient()
    client.load_connection_info(curve_encrypted_kernel.parent.get_connection_info())
    client.start_channels()
    try:
        yield client
    finally:
        await utils.clear_iopub(client, timeout=0.1)
        client.stop_channels()
        await anyio.sleep(0)


async def test_curve_encryption(
    curve_encrypted_kernel: Kernel[ZMQInterface], curve_encrypted_client: AsyncKernelClient
):
    assert curve_encrypted_kernel.parent._sockets[Channel.shell].curve_server == 1

    msg_id, reply = await utils.execute(curve_encrypted_client, "1+1", clear_pub=False)
    assert reply["status"] == "ok"
    await utils.check_pub_message(curve_encrypted_client, msg_id, execution_state="busy")
    await utils.check_pub_message(curve_encrypted_client, msg_id, msg_type="execute_input")
    await utils.check_pub_message(curve_encrypted_client, msg_id, msg_type="execute_result")
    await utils.check_pub_message(curve_encrypted_client, msg_id, execution_state="idle")
