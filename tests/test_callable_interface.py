from __future__ import annotations

import anyio
import orjson
import pytest

from async_kernel.interface.callable import CallableKernelInterface


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def interface(anyio_backend):
    # These are the functions that should be provided externally
    def sender(msg_string, buffers, /):
        assert isinstance(msg_string, str)

    def input(prompt, password, /):
        return prompt

    callable_interface = CallableKernelInterface()
    await callable_interface.start(sender, input, {})
    try:
        yield callable_interface
    finally:
        callable_interface.kernel.stop()


class TestCallableInterface:
    async def test_start(self, interface: CallableKernelInterface):
        assert interface.kernel.event_started

    async def test_msg(self, interface: CallableKernelInterface, mocker):
        sender = mocker.patch.object(interface, "_sender")
        code = "import async_kernel\nassert async_kernel.utils.get_job()['msg']['buffers'] == [b'123']"
        msg = interface.msg("execute_request", content={"code": code})
        msg["header"]["session"] = "test session"
        buffers = [b"123"]
        async with interface.kernel.caller.create_pending_group():
            interface._handle_msg(orjson.dumps(msg).decode(), buffers)  # pyright: ignore[reportPrivateUsage]

        assert sender.call_count == 4
        reply = orjson.loads(sender.call_args_list[2][0][0])
        assert reply["header"]["msg_type"] == "execute_reply"
        assert reply["content"]["status"] == "ok"

    async def test_kernel_info(self, interface: CallableKernelInterface, mocker):
        sender = mocker.patch.object(interface, "_sender")
        msg = interface.msg("kernel_info_request")
        msg["header"]["session"] = "test session"
        interface._handle_msg(orjson.dumps(msg).decode())  # pyright: ignore[reportPrivateUsage]
        while sender.call_count != 3:
            await anyio.sleep(0.1)
        reply = orjson.loads(sender.call_args_list[1][0][0])
        assert reply["header"]["msg_type"] == "kernel_info_reply"
        assert reply["content"]["status"] == "ok"

    async def test_input(self, interface: CallableKernelInterface):
        assert interface.input_request("test") == "test"

    async def test_prevent_multiple_instances(self, interface):
        with pytest.raises(RuntimeError):
            CallableKernelInterface()
