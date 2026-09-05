from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from aiologic.lowlevel import create_async_waiter

from async_kernel import Caller
from async_kernel.messaging import LocalClient
from async_kernel.messaging.base import BaseClient, Connection
from async_kernel.messaging.zmq import ZMQClient, ZMQConnection
from async_kernel.typing import Channel, MsgType

if TYPE_CHECKING:
    from async_kernel import Kernel


@pytest.mark.parametrize("connection_name", ["local"], scope="module")
class TestConnection:
    async def test_lifecycle(self, kernel: Kernel, mocker) -> None:

        connection = Connection()
        assert connection.parent is kernel.parent

        connection.start()
        await connection.started
        assert connection.parent.connections
        assert connection.connection_info() == ""
        await connection.stop()

    async def test_base_client(self, kernel: Kernel, mocker):
        async with BaseClient().start() as client:
            msg = client.msg(MsgType.comm_open, None, Channel.shell)
            with pytest.raises(TypeError, match="does not send a reply"):
                client.send_message(msg)


@pytest.mark.parametrize("connection_name", ["local"], scope="module")
class TestZMQConnection:
    async def test_info(self, kernel: Kernel):

        connection = ZMQConnection()
        assert connection.connection_info() == ""

        client = ZMQClient()
        with pytest.raises(RuntimeError, match="Connection info has not been set"):
            client.start()

        async with connection.start():
            client.load_connection_info(connection.get_connection_info())
            async with client.start():
                reply = await client.kernel_info()
                assert reply

    async def test_iopub_subscribe(self, kernel: Kernel):

        async def f(ready):
            async with client.iopub_subscribe():
                ready()
                await create_async_waiter()

        async with LocalClient().start() as client:
            ready = create_async_waiter()
            pen = Caller().to_thread(f, ready.wake)
            await ready
        with pytest.raises(RuntimeError, match="Scope cancelled"):
            await pen

    async def test_too_late(self, kernel):
        async with ZMQConnection().start() as connection:
            with pytest.raises(RuntimeError, match="too late"):
                connection.connection_file = "never set.json"
            with pytest.raises(RuntimeError, match="too late"):
                connection.load_connection_info({})
