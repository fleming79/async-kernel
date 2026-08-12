from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from async_kernel.connection.base import BaseKernelClient, Connection
from async_kernel.connection.zmq import ZMQConnection, ZMQKernelClient
from async_kernel.typing import Channel, MsgType

if TYPE_CHECKING:
    from async_kernel import Kernel


@pytest.mark.parametrize("connection_name", ["local"], scope="module")
class TestConnection:
    async def test_lifecycle(self, kernel: Kernel, mocker) -> None:

        connection = Connection()
        assert connection.parent is kernel.parent

        # Check unhandled messages
        debug = mocker.patch.object(connection.log, "debug")
        connection.handle_incoming_msg({"channel": Channel.heartbeat}, [])  # pyright: ignore[reportArgumentType]
        assert debug.call_count == 1
        assert debug.call_args.args[0].startswith("Unhandled message")

        connection.start()
        await connection.started
        assert connection.parent.connections
        assert connection.connection_info() == ""
        await connection.stop()

    async def test_base_client(self, kernel: Kernel, mocker):
        async with BaseKernelClient().start() as client:
            # Check unhandled messages
            debug = mocker.patch.object(client.log, "debug")
            client.handle_incoming_msg({"channel": Channel.heartbeat}, [])  # pyright: ignore[reportArgumentType]
            assert debug.call_count == 1
            assert debug.call_args.args[0].startswith("Unhandled message")

            msg = client.msg(MsgType.comm_open, None, Channel.shell)
            with pytest.raises(TypeError, match="does not send a reply"):
                client.send_message(msg)


@pytest.mark.parametrize("connection_name", ["local"], scope="module")
class TestZMQConnection:
    async def test_info(self, kernel: Kernel):

        connection = ZMQConnection()
        assert connection.connection_info() == ""

        client = ZMQKernelClient()
        with pytest.raises(RuntimeError, match="Connection info has not been set"):
            client.start()

        async with connection.start():
            client.load_connection_info(connection.get_connection_info())
            async with client.start():
                reply = await client.kernel_info()
                assert reply

    async def test_iopub(self, kernel):
        async with ZMQConnection().start() as connection:
            client = ZMQKernelClient()
            client.load_connection_info(connection.get_connection_info())
            async with client.start():
                async with client.iopub_subscribe():
                    pass
                with pytest.raises(TimeoutError, match="Welcome message not received"):
                    async with client.iopub_subscribe(timeout=0):
                        pass

    async def test_too_late(self, kernel):
        async with ZMQConnection().start() as connection:
            with pytest.raises(RuntimeError, match="too late"):
                connection.connection_file = "never set.json"
            with pytest.raises(RuntimeError, match="too late"):
                connection.load_connection_info({})
